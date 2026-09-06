"""Durable PM event ownership incidents, without hidden transaction commits."""

import json

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateTable

from freqtrade.persistence import Order, Trade
from freqtrade.persistence.pm_stream_journal import PMStreamJournal


pytestmark = pytest.mark.usefixtures("init_persistence")
PAIR = "DASH/USDT:USDT"
ORDER_ID = "10660398840"
CLIENT_ID = "ft13ff22a52bad4a7fb82c42020657"


def record(pair=PAIR, order_id=ORDER_ID, client_id=CLIENT_ID, **changes):
    return PMStreamJournal.record_unresolved(
        pair,
        order_id,
        client_id,
        changes.pop("order_data", {"i": int(order_id), "c": client_id, "X": "FILLED"}),
        changes.pop("reason", "unknown_order"),
        **changes,
    )


def local_order():
    trade = Trade(
        pair=PAIR,
        stake_amount=25.0,
        open_rate=69.0,
        amount=0.36,
        is_open=True,
        exchange="binance",
        fee_open=0.001,
        fee_close=0.001,
    )
    order = Order(
        ft_pair=PAIR,
        order_id=ORDER_ID,
        ft_order_side="buy",
        ft_is_open=False,
        ft_amount=0.36,
        ft_price=69.0,
        status="closed",
    )
    trade.orders.append(order)
    Trade.session.add(trade)
    Trade.session.flush()
    return order


def test_init_db_creates_journal_with_instrument_key_and_order_fk():
    assert PMStreamJournal.session is Trade.session
    inspector = inspect(Trade.session.get_bind())
    assert "pm_stream_journal" in inspector.get_table_names()
    assert inspector.get_unique_constraints("pm_stream_journal")[0]["column_names"] == [
        "pair",
        "exchange_order_id",
    ]
    fk = inspector.get_foreign_keys("pm_stream_journal")[0]
    assert fk["referred_table"] == "orders"
    assert fk["constrained_columns"] == ["linked_order_pk"]


def test_pg17_compatible_ddl_uses_boolean_timestamp_unique_and_fk():
    ddl = str(CreateTable(PMStreamJournal.__table__).compile(dialect=postgresql.dialect()))
    assert "unresolved BOOLEAN NOT NULL" in ddl
    assert "TIMESTAMP" in ddl and "DATETIME" not in ddl
    assert "UNIQUE (pair, exchange_order_id)" in ddl
    assert "FOREIGN KEY(linked_order_pk) REFERENCES orders (id)" in ddl


def test_incident_preserves_first_evidence_and_deduplicates_before_commit():
    first, notify = record()
    assert notify is True
    created_at, updated_at = first.created_at, first.updated_at
    second, notify = record(order_data={"X": "NEW"}, reason="different_reason")
    assert second is first and notify is False
    assert json.loads(second.order_data)["X"] == "FILLED"
    assert second.reason == "unknown_order"
    assert second.created_at == created_at and second.updated_at == updated_at
    assert PMStreamJournal.get(PAIR, ORDER_ID) is first
    assert PMStreamJournal.get_unresolved() == [first]
    Trade.commit()
    Trade.session.expire_all()
    assert PMStreamJournal.session.query(PMStreamJournal).count() == 1
    assert PMStreamJournal.get(PAIR, ORDER_ID).client_id == CLIENT_ID
    assert record()[1] is False


def test_order_ids_are_scoped_by_canonical_instrument():
    dash, _ = record()
    eth, _ = record(pair="ETH/USDT:USDT")
    Trade.commit()
    assert dash.id != eth.id
    assert len(PMStreamJournal.get_unresolved()) == 2


def test_client_conflict_preserves_incident():
    row, _ = record()
    Trade.commit()
    with pytest.raises(ValueError, match="client ID conflicts"):
        record(client_id="another_client", order_data={"X": "NEW"})
    assert row.client_id == CLIENT_ID
    assert json.loads(row.order_data)["X"] == "FILLED"
    assert row.unresolved


def test_record_does_not_commit_and_survives_restart_only_after_caller_commit():
    record()
    Trade.session.flush()
    Trade.session.rollback()
    assert PMStreamJournal.get(PAIR, ORDER_ID) is None
    record()
    Trade.commit()
    Trade.session.remove()
    assert PMStreamJournal.get(PAIR, ORDER_ID).unresolved


def test_resolve_links_local_pk_atomically_and_rollback_keeps_gate():
    order = local_order()
    order_pk = order.id
    record()
    Trade.commit()
    assert PMStreamJournal.resolve(PAIR, ORDER_ID, order.id)
    assert PMStreamJournal.get_unresolved() == []
    Trade.session.flush()
    Trade.session.rollback()
    row = PMStreamJournal.get(PAIR, ORDER_ID)
    assert row.unresolved and row.linked_order_pk is None
    assert PMStreamJournal.resolve(PAIR, ORDER_ID, order.id)
    Trade.commit()
    Trade.session.remove()
    row = PMStreamJournal.get(PAIR, ORDER_ID)
    assert not row.unresolved and row.linked_order_pk == order_pk
    assert PMStreamJournal.get_unresolved() == []


def test_resolved_incident_can_reopen_and_alert_once_with_fresh_evidence():
    order = local_order()
    row, _ = record()
    Trade.commit()
    PMStreamJournal.resolve(PAIR, ORDER_ID, order.id)
    Trade.commit()
    row, notify = record(order_data={"X": "FILLED", "z": "999"}, reason="overfill")
    assert notify and row.unresolved and row.linked_order_pk is None
    assert row.reason == "overfill"
    assert json.loads(row.order_data)["z"] == "999"
    assert PMStreamJournal.get_unresolved() == [row]
    assert record()[1] is False


def test_resolve_is_idempotent_and_rejects_different_local_order_link():
    order = local_order()
    record()
    assert PMStreamJournal.resolve(PAIR, ORDER_ID, order.id)
    assert PMStreamJournal.resolve(PAIR, ORDER_ID, order.id)
    with pytest.raises(ValueError, match="existing Order link"):
        PMStreamJournal.resolve(PAIR, ORDER_ID, order.id + 1)
    assert not PMStreamJournal.resolve(PAIR, "unknown", order.id)


@pytest.mark.parametrize("pk", [None, 0, -1, True, "1"])
def test_resolve_requires_local_integer_primary_key(pk):
    record()
    with pytest.raises(ValueError, match="local Order primary key"):
        PMStreamJournal.resolve(PAIR, ORDER_ID, pk)


def test_unique_constraint_is_enforced():
    record()
    Trade.commit()
    Trade.session.add(
        PMStreamJournal(
            pair=PAIR,
            exchange_order_id=ORDER_ID,
            client_id=CLIENT_ID,
            order_data="{}",
            reason="duplicate",
            unresolved=True,
        )
    )
    with pytest.raises(IntegrityError):
        Trade.commit()
    Trade.session.rollback()


def test_sqlite_fk_rejects_dangling_order_link():
    Trade.session.execute(text("PRAGMA foreign_keys=ON"))
    row, _ = record()
    row.linked_order_pk = 987654321
    with pytest.raises(IntegrityError):
        Trade.commit()
    Trade.session.rollback()
