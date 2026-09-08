"""
PostgreSQL fault-injection tests for the PM order-consistency pipeline.

These tests cover the crash windows the go-live gate demands:

1. crash BEFORE the POST (intent+outbox committed, POST never sent),
2. crash AFTER the exchange ACK but BEFORE the local Trade/Order commit,
3. atomicity of the LINK step (intent LINKED in the same transaction as the
   local order commit - a rollback must leave the intent ACKED, never deleted),
4. advisory-lock mutual exclusion for the pipeline sections.

Run with a scratch PostgreSQL URL:

    FREQTRADE_TEST_PG_URL=postgresql://user:pass@localhost:5432/scratch \
        pytest tests/freqtradebot/test_pm_pg_fault_injection.py

They are skipped when the URL (or the psycopg2 driver) is missing.
"""

import os
import threading
import time
from unittest.mock import MagicMock

import pytest

from freqtrade.exchange.binance import Binance
from freqtrade.exchange.pm_locks import pm_pipeline_lock
from freqtrade.persistence import PMOrderIntent, PMOutbox, init_db
from tests.conftest import get_markets, get_patched_exchange

pytest.importorskip("psycopg2")

PG_URL = os.environ.get("FREQTRADE_TEST_PG_URL")
if not PG_URL:
    pytest.skip(
        "Set FREQTRADE_TEST_PG_URL to a scratch PostgreSQL URL to run the PM "
        "fault-injection tests.",
        allow_module_level=True,
    )

PM_ORDER = {
    "orderId": 123,
    "clientOrderId": "ftpm_abc",
    "symbol": "ETHUSDT",
    "status": "NEW",
    "type": "LIMIT",
    "side": "BUY",
    "origQty": "1",
    "executedQty": "0",
    "price": "100",
    "avgPrice": "0",
    "cumQuote": "0",
    "timeInForce": "GTC",
}


@pytest.fixture()
def pm_pg(mocker, default_conf_usdt):
    init_db(PG_URL)
    # Clean slate for the PM tables.
    PMOutbox.session.query(PMOutbox).delete()
    PMOrderIntent.session.query(PMOrderIntent).delete()
    PMOrderIntent.session.commit()
    conf = default_conf_usdt.copy()
    conf["dry_run"] = False
    conf["trading_mode"] = "futures"
    conf["margin_mode"] = "cross"
    conf["stake_currency"] = "USDT"
    conf["exchange"] = conf["exchange"].copy()
    conf["exchange"]["name"] = "binance"
    conf["exchange"]["key"] = "dummy_key"
    conf["exchange"]["secret"] = "dummy_secret"
    conf["exchange"]["pair_whitelist"] = ["ETH/USDT:USDT"]
    conf["exchange"]["portfolio_margin"] = True
    conf["exchange"]["portfolio_margin_risk"] = {"min_uni_mmr": 1.5}
    markets = get_markets()
    markets["ETH/USDT:USDT"]["id"] = "ETHUSDT"
    exchange = get_patched_exchange(
        mocker, conf, api_mock=MagicMock(), exchange="binance", mock_markets=markets
    )
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)
    yield exchange


def test_crash_before_post_is_redispatched_exactly_once(pm_pg):
    """
    The intent+outbox are committed and the process dies BEFORE the POST.
    A restarted exchange drains the outbox and POSTs exactly once.
    """
    exchange = pm_pg
    # The "crash before POST" is simulated by enqueueing WITHOUT dispatching;
    # the relay then POSTs exactly once and the exchange ACKs.
    exchange._papi_request = MagicMock(return_value=PM_ORDER)

    # 1. enqueue only (no dispatch) - the crash window.
    exchange._pm_enqueue(
        "ftcrash1",
        {
            "kind": "order",
            "pair": "ETH/USDT:USDT",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 100.0,
            "reduce_only": False,
        },
        payload={"symbol": "ETHUSDT", "newClientOrderId": "ftcrash1"},
    )
    assert len(PMOutbox.get_pending()) == 1
    assert PMOrderIntent.get_unresolved()[0].state == "PREPARED"

    # 2. "restart": the relay drains the outbox; the (mocked) exchange answers.
    report = exchange.pm_drain_outbox()
    assert report["acked"] == 1
    intents = PMOrderIntent.get_unresolved()
    assert len(intents) == 1
    assert intents[0].state == "ACKED"
    assert intents[0].exchange_order_id == "123"
    # Exactly one POST reached the exchange.
    posts = [c for c in exchange._papi_request.call_args_list if c[0][1] == "POST"]
    assert len(posts) == 1


def test_crash_after_ack_before_local_commit_keeps_block(pm_pg):
    """
    Crash between the exchange ACK and the local Trade/Order commit: the intent
    stays ACKED with full evidence, new exposure stays blocked, and a second
    drain never duplicates the POST.
    """
    exchange = pm_pg
    exchange._papi_request = MagicMock(return_value=PM_ORDER)

    exchange._pm_place_order(
        "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
    )
    # No local order was ever created (simulated crash right after the ACK).
    intents = PMOrderIntent.get_unresolved()
    assert len(intents) == 1
    assert intents[0].state == "ACKED"
    assert intents[0].exchange_order_id == "123"
    assert "123" in (intents[0].raw_response or "")
    assert PMOutbox.get_by_client_id(intents[0].client_id).state == "ACKED"

    # A fresh drain must NOT re-POST (resolve-by-id finds the order).
    report = exchange.pm_drain_outbox()
    assert report["acked"] == 0
    posts = [c for c in exchange._papi_request.call_args_list if c[0][1] == "POST"]
    assert len(posts) == 1
    assert exchange.pm_has_unresolved_intents() is True


def test_link_and_local_commit_are_atomic(pm_pg):
    """
    LINKING the intent must happen in the same transaction as the local order
    commit: a rollback leaves the intent ACKED (never tombstoned).
    """
    exchange = pm_pg
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    exchange._pm_place_order(
        "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
    )
    client_id = PMOrderIntent.get_unresolved()[0].client_id

    exchange.pm_link_intent_in_session(client_id, "123", None)
    # Simulate the local Trade commit failing.
    PMOrderIntent.session.rollback()
    intent = PMOrderIntent.get_by_client_id(client_id)
    assert intent.state == "ACKED"  # evidence preserved, still blocking
    assert intent.linked_order_id is None


def test_advisory_lock_is_exclusive_across_threads(pm_pg):
    """
    Two sessions cannot hold the PM pipeline advisory lock at the same time.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(PG_URL)
    Session = sessionmaker(bind=engine)
    s1 = Session()
    s2 = Session()

    with pm_pipeline_lock(s1, timeout_seconds=3):
        start = time.monotonic()
        acquired_in_time = False

        def contender():
            nonlocal acquired_in_time
            try:
                with pm_pipeline_lock(s2, timeout_seconds=1.5):
                    acquired_in_time = True
            except RuntimeError:
                pass  # expected: the lock is held by s1 and the timeout fires

        thread = threading.Thread(target=contender)
        thread.start()
        thread.join()
        assert acquired_in_time is False  # timed out while s1 held it
        assert time.monotonic() - start >= 1.4

    # After release the lock is acquirable again.
    with pm_pipeline_lock(s2, timeout_seconds=2):
        pass
    s1.close()
    s2.close()

def test_advisory_lock_survives_business_session_commit(pm_pg):
    """A business Session commit must not release/change the physical advisory-lock owner."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(PG_URL)
    Session = sessionmaker(bind=engine)
    owner_session = Session()
    contender_session = Session()

    with pm_pipeline_lock(owner_session, timeout_seconds=2):
        # This is the exact regression: order paths commit the ORM session while
        # still inside pm_pipeline_lock.
        owner_session.execute(text("SELECT 1"))
        owner_session.commit()

        acquired = False

        def contender():
            nonlocal acquired
            try:
                with pm_pipeline_lock(contender_session, timeout_seconds=0.6):
                    acquired = True
            except RuntimeError:
                pass

        thread = threading.Thread(target=contender)
        thread.start()
        thread.join()
        assert acquired is False

        # Nested scope in the owner thread must reuse the same dedicated lock
        # connection instead of self-deadlocking on a second pooled connection.
        with pm_pipeline_lock(owner_session, timeout_seconds=0.2):
            owner_session.commit()

    with pm_pipeline_lock(contender_session, timeout_seconds=1):
        pass
    owner_session.close()
    contender_session.close()
    engine.dispose()
