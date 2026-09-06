"""Round-2 PostgreSQL fault injection for the durable stream journal.

Runs against a scratch PostgreSQL (FREQTRADE_TEST_PG_URL). Verifies the
persistence semantics the unattended gate depends on:

- transaction boundaries (record/rollback keeps the gate closed),
- restart persistence (a committed incident survives a new engine),
- resolution atomicity (resolve + rollback keeps the gate closed),
- bounded retention purge,
- concurrent writers (unique constraint: exactly one incident per key).
"""

import os
import threading
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from freqtrade.persistence import Order, Trade, init_db
from freqtrade.persistence.pm_stream_journal import PMStreamJournal
from freqtrade.util.datetime_helpers import dt_now


pytest.importorskip("psycopg2")

PG_URL = os.environ.get("FREQTRADE_TEST_PG_URL")
if not PG_URL:
    pytest.skip(
        "Set FREQTRADE_TEST_PG_URL to a scratch PostgreSQL URL to run the PM "
        "stream-journal fault-injection tests.",
        allow_module_level=True,
    )

PAIR = "DASH/USDT:USDT"
ORDER_ID = "10660398840"
CLIENT_ID = "ft13ff22a52bad4a7fb82c42020657"


def event(order_id=ORDER_ID, status="FILLED", filled="11"):
    try:
        oid = int(order_id)
    except (TypeError, ValueError):
        oid = order_id
    return {"i": oid, "c": CLIENT_ID, "X": status, "z": filled}


def local_order(order_id=ORDER_ID):
    """A real local Trade/Order row so journal links satisfy the PG FK/NOT NULL."""
    trade = Trade(
        pair=PAIR,
        stake_currency="USDT",
        open_rate=69.0,
        amount=0.36,
        fee_open=0.001,
        fee_close=0.001,
        stake_amount=25.0,
        open_date=dt_now(),
        exchange="binance",
        is_open=True,
        is_short=False,
        leverage=1.0,
    )
    order = Order(
        ft_pair=PAIR,
        order_id=order_id,
        ft_order_side="buy",
        ft_is_open=False,
        ft_amount=0.36,
        ft_price=69.0,
        status="closed",
    )
    trade.orders.append(order)
    PMStreamJournal.session.add(trade)
    PMStreamJournal.session.flush()
    return order


@pytest.fixture()
def pm_pg():
    init_db(PG_URL)
    PMStreamJournal.session.query(PMStreamJournal).delete()
    PMStreamJournal.session.query(Order).delete()
    PMStreamJournal.session.query(Trade).delete()
    PMStreamJournal.session.commit()
    yield
    PMStreamJournal.session.remove()


def test_journal_table_exists_on_pg_with_instrument_unique(pm_pg):
    inspector = inspect(PMStreamJournal.session.get_bind())
    assert "pm_stream_journal" in inspector.get_table_names()
    uniques = inspector.get_unique_constraints("pm_stream_journal")
    assert any(u["column_names"] == ["pair", "exchange_order_id"] for u in uniques)


def test_record_rollback_keeps_gate_closed(pm_pg):
    PMStreamJournal.record_unresolved(PAIR, ORDER_ID, CLIENT_ID, event(), "unknown")
    PMStreamJournal.session.flush()
    PMStreamJournal.session.rollback()
    assert PMStreamJournal.get(PAIR, ORDER_ID) is None
    assert PMStreamJournal.get_unresolved() == []


def test_recorded_incident_survives_process_restart(pm_pg):
    PMStreamJournal.record_unresolved(PAIR, ORDER_ID, CLIENT_ID, event(), "unknown")
    PMStreamJournal.session.commit()
    PMStreamJournal.session.remove()

    # "Restart": a completely fresh engine/session sees the durable incident.
    engine = create_engine(PG_URL)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        rows = (
            session.query(PMStreamJournal)
            .filter(PMStreamJournal.pair == PAIR, PMStreamJournal.exchange_order_id == ORDER_ID)
            .all()
        )
        assert len(rows) == 1
        assert rows[0].unresolved
        assert rows[0].max_cumulative_filled == 11
        assert rows[0].saw_filled


def test_resolve_rollback_keeps_gate_closed_then_commit_releases(pm_pg):
    row, _ = PMStreamJournal.record_unresolved(PAIR, ORDER_ID, CLIENT_ID, event(), "unknown")
    PMStreamJournal.session.commit()
    linked_pk = local_order().id
    PMStreamJournal.session.commit()

    assert PMStreamJournal.resolve(PAIR, ORDER_ID, linked_pk)
    PMStreamJournal.session.flush()
    PMStreamJournal.session.rollback()
    row = PMStreamJournal.get(PAIR, ORDER_ID)
    assert row.unresolved and row.linked_order_pk is None

    assert PMStreamJournal.resolve(PAIR, ORDER_ID, linked_pk)
    PMStreamJournal.session.commit()
    PMStreamJournal.session.remove()
    row = PMStreamJournal.get(PAIR, ORDER_ID)
    assert not row.unresolved and row.linked_order_pk == linked_pk


def test_purge_resolved_keeps_recent_and_unresolved_on_pg(pm_pg):
    # Plain-evidence rows: ids match local Order rows -> purgeable after retention.
    for key in ("stale", "fresh"):
        local_order(order_id=key)
    order = local_order()
    for key, days_ago in (("stale", 60), ("fresh", 1)):
        row, _ = PMStreamJournal.record_unresolved(PAIR, key, "", event(order_id=key), "x")
        PMStreamJournal.resolve(PAIR, key, order.id)
        row.updated_at = datetime.now(UTC) - timedelta(days=days_ago)
    PMStreamJournal.record_unresolved(PAIR, "pending", "", event(order_id="pending"), "x")
    # Durable child alias: its exchange id is NOT a local Order id -> never purged.
    alias, _ = PMStreamJournal.record_unresolved(
        PAIR, "child999", "", {"i": "child999"}, "conditional child ownership alias"
    )
    PMStreamJournal.resolve(PAIR, "child999", order.id)
    alias.updated_at = datetime.now(UTC) - timedelta(days=60)
    PMStreamJournal.session.commit()

    purged = PMStreamJournal.purge_resolved(datetime.now(UTC) - timedelta(days=30))
    PMStreamJournal.session.commit()

    assert purged == 1
    assert PMStreamJournal.get(PAIR, "stale") is None
    assert PMStreamJournal.get(PAIR, "fresh") is not None
    assert PMStreamJournal.get(PAIR, "pending").unresolved
    kept_alias = PMStreamJournal.get(PAIR, "child999")
    assert kept_alias is not None and not kept_alias.unresolved
    assert kept_alias.linked_order_pk == order.id


def test_concurrent_writers_produce_exactly_one_incident(pm_pg):
    """Two sessions inserting the same (pair, order_id): the unique constraint
    guarantees exactly one durable incident - the gate can never double-count."""
    import json

    from freqtrade.util.datetime_helpers import dt_now

    engine = create_engine(PG_URL)
    Session = sessionmaker(bind=engine)
    results: dict[str, str] = {}
    barrier = threading.Barrier(2)

    def writer(name: str):
        with Session() as session:
            barrier.wait(timeout=10)
            now = dt_now()
            session.add(
                PMStreamJournal(
                    pair=PAIR,
                    exchange_order_id=ORDER_ID,
                    client_id=CLIENT_ID,
                    order_data=json.dumps(event()),
                    max_cumulative_filled=11,
                    saw_filled=True,
                    reason=name,
                    unresolved=True,
                    created_at=now,
                    updated_at=now,
                )
            )
            try:
                session.commit()
                results[name] = "committed"
            except IntegrityError:
                session.rollback()
                results[name] = "conflict"

    threads = [threading.Thread(target=writer, args=(n,)) for n in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
        assert not t.is_alive()

    assert sorted(results.values()) == ["committed", "conflict"]
    assert PMStreamJournal.session.query(PMStreamJournal).count() == 1
