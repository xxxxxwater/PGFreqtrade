"""
End-to-end tests for the PM order outbox flow (PREPARED -> ACKED -> LINKED ->
RECONCILED) against a real SQLite database with a mocked PAPI transport.
"""

from unittest.mock import MagicMock

import pytest

from freqtrade.exceptions import OperationalException, TemporaryError
from freqtrade.exchange.binance import Binance
from freqtrade.persistence import PMOrderIntent, PMOutbox, init_db
from tests.conftest import get_patched_exchange

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


@pytest.fixture(autouse=True)
def pm_db(default_conf_usdt):
    init_db(default_conf_usdt["db_url"])


def make_exchange(mocker, default_conf_usdt):
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
    conf["exchange"]["portfolio_margin_risk"] = {
        "min_uni_mmr": 1.5,
        "user_stream_enabled": False,
    }
    return get_patched_exchange(mocker, conf, api_mock=MagicMock(), exchange="binance")


def test_enqueue_ack_link_tombstone_lifecycle(mocker, default_conf_usdt):
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)
    exchange._papi_request = MagicMock(return_value=PM_ORDER)

    order = exchange._pm_place_order(
        "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
    )
    assert order["id"] == "123"

    # PREPARED -> ACKED with exchange evidence, outbox mirrors it.
    intents = PMOrderIntent.get_unresolved()
    assert len(intents) == 1
    intent = intents[0]
    assert intent.state == "ACKED"
    assert intent.exchange_order_id == "123"
    assert "123" in (intent.raw_response or "")
    client_id = intent.client_id
    outbox = PMOutbox.get_by_client_id(client_id)
    assert outbox.state == "ACKED"
    assert outbox.exchange_order_id == "123"

    # The local Trade/Order commit marks LINKED (in-session + commit).
    exchange.pm_link_intent_in_session(client_id, "123", None)
    PMOrderIntent.session.commit()
    assert PMOrderIntent.has_unresolved() is False
    assert len(PMOrderIntent.get_linked()) == 1
    assert PMOutbox.get_by_client_id(client_id).state == "LINKED"

    # Reconciliation confirms -> tombstone intent, outbox RECONCILED.
    exchange.pm_tombstone_reconciled_intent(client_id)
    assert PMOrderIntent.get_by_client_id(client_id) is None
    assert PMOutbox.get_by_client_id(client_id).state == "RECONCILED"


def test_crash_between_ack_and_local_commit_keeps_evidence(mocker, default_conf_usdt):
    """
    Simulate a crash after the exchange ACK but before the local Trade/Order
    commit: the intent must stay ACKED with the exchange id + raw response and
    must keep blocking new exposure.
    """
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)
    exchange._papi_request = MagicMock(return_value=PM_ORDER)

    exchange._pm_place_order(
        "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
    )

    # "Restart": a fresh exchange object resolves the persisted intent.
    exchange2 = make_exchange(mocker, default_conf_usdt)
    intents = exchange2.list_pm_pending_intents()
    assert len(intents) == 1
    assert intents[0]["state"] == "ACKED"
    assert intents[0]["exchange_order_id"] == "123"
    assert exchange2.pm_has_unresolved_intents() is True  # still blocks entries


def test_deterministic_rejection_tombstones_intent_keeps_outbox(mocker, default_conf_usdt):
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=Exception("bad"))
    exchange._pm_reject  # noqa: B018 - method exists
    exchange._papi_request.side_effect = __import__("ccxt").BadRequest("bad request")

    with pytest.raises(__import__("ccxt").BadRequest):
        exchange._pm_place_order(
            "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
        )

    assert PMOrderIntent.get_unresolved() == []
    rejected = (
        PMOutbox.session.query(PMOutbox).filter(PMOutbox.state == "REJECTED").all()
    )
    assert len(rejected) == 1


def test_outbox_relay_redispaches_pending_row(mocker, default_conf_usdt):
    """
    A PENDING outbox row whose POST never happened is re-dispatched by the relay
    exactly once (resolve-by-client-id first, then POST).
    """
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)
    # First POST fails transiently, the lookup finds nothing, the row stays
    # PENDING; the relay then POSTs again (mocked to succeed).
    exchange._papi_request = MagicMock(side_effect=[TemporaryError("timeout"), PM_ORDER])
    exchange._pm_fetch_order_by_client_id = MagicMock(return_value=None)

    with pytest.raises(TemporaryError):
        exchange._pm_place_order(
            "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
        )

    pending = PMOutbox.get_pending()
    assert len(pending) == 1

    report = exchange.pm_drain_outbox()
    assert report["acked"] == 1
    intents = PMOrderIntent.get_unresolved()
    assert len(intents) == 1
    assert intents[0].state == "ACKED"


def test_enqueue_supersedes_stale_prepared_same_pair(mocker, default_conf_usdt):
    """A newer same-kind intent for the same pair supersedes a stale PREPARED one."""
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)
    exchange._papi_request = MagicMock(
        side_effect=[TemporaryError("timeout"), PM_ORDER]
    )
    exchange._pm_fetch_order_by_client_id = MagicMock(return_value=None)

    with pytest.raises(TemporaryError):
        exchange._pm_place_order(
            "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
        )
    # First intent left PREPARED/PENDING.
    assert len(PMOrderIntent.get_unresolved()) == 1

    # Second attempt for the same pair: supersedes the stale row.
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    order = exchange._pm_place_order(
        "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
    )
    assert order["id"] == "123"
    unresolved = PMOrderIntent.get_unresolved()
    assert len(unresolved) == 1
    assert unresolved[0].state == "ACKED"
    superseded = (
        PMOutbox.session.query(PMOutbox)
        .filter(PMOutbox.state == "REJECTED")
        .all()
    )
    assert len(superseded) == 1
    assert "superseded" in (superseded[0].last_error or "")


def test_outbox_row_missing_fails_closed(mocker, default_conf_usdt):
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    with pytest.raises(OperationalException, match="outbox row missing"):
        exchange._pm_dispatch_order("ftnone", "ETH/USDT:USDT", {"symbol": "ETHUSDT"})
