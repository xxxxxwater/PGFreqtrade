"""
End-to-end tests for the PM order outbox flow (PREPARED -> ACKED -> LINKED ->
RECONCILED) against a real SQLite database with a mocked PAPI transport.
"""

from unittest.mock import MagicMock

import pytest

from freqtrade.exceptions import InvalidOrderException, OperationalException, TemporaryError
from freqtrade.exchange.binance import Binance
from freqtrade.persistence import PMOrderIntent, PMOutbox, init_db
from tests.conftest import get_markets, get_patched_exchange

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
    markets = get_markets()
    markets["ETH/USDT:USDT"]["id"] = "ETHUSDT"
    return get_patched_exchange(
        mocker, conf, api_mock=MagicMock(), exchange="binance", mock_markets=markets
    )


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


def test_transient_submit_absent_lookup_never_reposts(mocker, default_conf_usdt):
    """Once dispatch_started_at is durable, an absent lookup can NEVER cause a second POST."""
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)
    exchange._papi_request = MagicMock(side_effect=TemporaryError("timeout"))
    exchange._pm_fetch_order_by_client_id = MagicMock(return_value=None)

    with pytest.raises(TemporaryError, match="will NOT be re-POSTed"):
        exchange._pm_place_order(
            "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
        )

    intent = PMOrderIntent.get_unresolved()[0]
    outbox = PMOutbox.get_by_client_id(intent.client_id)
    assert intent.state == "UNKNOWN"
    assert outbox.dispatch_started_at is not None
    assert outbox.dispatch_attempts == 1
    first_posts = [c for c in exchange._papi_request.call_args_list if c[0][1] == "POST"]
    assert len(first_posts) == 1

    # Relay/restart path: same-id lookup still absent.  It must remain UNKNOWN
    # and never call POST again.
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    report = exchange.pm_drain_outbox()
    assert report["acked"] == 0
    assert report["deferred"] == 1
    assert [c for c in exchange._papi_request.call_args_list if c[0][1] == "POST"] == []
    assert PMOrderIntent.get_by_client_id(intent.client_id).state == "UNKNOWN"


def test_pristine_unsent_prepared_can_be_superseded(mocker, default_conf_usdt):
    """Only a PREPARED row with no dispatch marker/attempt is safe to supersede."""
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._pm_enqueue(
        "ft-pristine-1",
        {
            "kind": "order",
            "pair": "ETH/USDT:USDT",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 100.0,
            "reduce_only": False,
        },
        payload={"symbol": "ETHUSDT", "newClientOrderId": "ft-pristine-1"},
    )
    old = PMOutbox.get_by_client_id("ft-pristine-1")
    assert old.dispatch_started_at is None
    assert old.dispatch_attempts == 0

    exchange._pm_enqueue(
        "ft-pristine-2",
        {
            "kind": "order",
            "pair": "ETH/USDT:USDT",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 101.0,
            "reduce_only": False,
        },
        payload={"symbol": "ETHUSDT", "newClientOrderId": "ft-pristine-2"},
    )
    assert PMOrderIntent.get_by_client_id("ft-pristine-1") is None
    assert PMOutbox.get_by_client_id("ft-pristine-1").state == "REJECTED"
    assert PMOrderIntent.get_by_client_id("ft-pristine-2").state == "PREPARED"


def test_may_have_been_sent_intent_cannot_be_superseded(mocker, default_conf_usdt):
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=TemporaryError("timeout"))
    exchange._pm_fetch_order_by_client_id = MagicMock(return_value=None)
    with pytest.raises(TemporaryError):
        exchange._pm_place_order(
            "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
        )
    old = PMOrderIntent.get_unresolved()[0]
    assert old.state == "UNKNOWN"

    with pytest.raises(OperationalException, match="refusing a fresh client id"):
        exchange._pm_enqueue(
            "ft-new-id",
            {
                "kind": "order",
                "pair": "ETH/USDT:USDT",
                "side": "buy",
                "type": "limit",
                "amount": 1.0,
                "price": 101.0,
                "reduce_only": False,
            },
            payload={"symbol": "ETHUSDT", "newClientOrderId": "ft-new-id"},
        )
    assert PMOrderIntent.get_by_client_id(old.client_id) is not None
    assert PMOrderIntent.get_by_client_id("ft-new-id") is None


def test_conditional_transient_absent_lookup_never_reposts(mocker, default_conf_usdt):
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=TemporaryError("timeout"))
    exchange.fetch_stoploss_order = MagicMock(side_effect=InvalidOrderException("absent"))
    request = {
        "algoType": "CONDITIONAL",
        "symbol": "ETHUSDT",
        "side": "SELL",
        "type": "STOP_MARKET",
        "reduceOnly": "true",
        "triggerPrice": 90,
        "workingType": "CONTRACT_PRICE",
        "clientAlgoId": "st-boundary-1",
        "quantity": 1,
    }
    exchange._pm_enqueue(
        "st-boundary-1",
        {
            "kind": "conditional",
            "pair": "ETH/USDT:USDT",
            "side": "sell",
            "type": "STOP_MARKET",
            "amount": 1.0,
            "stop_price": 90,
            "reduce_only": True,
        },
        payload=request,
    )
    with pytest.raises(TemporaryError, match="will NOT be re-POSTed"):
        exchange._pm_dispatch_conditional("st-boundary-1", "ETH/USDT:USDT", request)
    outbox = PMOutbox.get_by_client_id("st-boundary-1")
    assert outbox.dispatch_started_at is not None
    assert outbox.dispatch_attempts == 1

    exchange._papi_request = MagicMock(return_value={"algoId": 999})
    with pytest.raises(TemporaryError, match="automatic re-POST is forbidden"):
        exchange._pm_dispatch_conditional("st-boundary-1", "ETH/USDT:USDT", request)
    assert [c for c in exchange._papi_request.call_args_list if c[0][1] == "POST"] == []


def test_outbox_row_missing_fails_closed(mocker, default_conf_usdt):
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    with pytest.raises(OperationalException, match="outbox row missing"):
        exchange._pm_dispatch_order("ftnone", "ETH/USDT:USDT", {"symbol": "ETHUSDT"})

def test_intent_without_outbox_stays_unresolved_and_blocks_fresh_client_id(
    mocker, default_conf_usdt
):
    exchange = make_exchange(mocker, default_conf_usdt)
    PMOrderIntent.session.add(
        PMOrderIntent(
            client_id="ft-missing-outbox",
            kind="order",
            pair="ETH/USDT:USDT",
            side="buy",
            order_type="limit",
            amount=1.0,
            reduce_only=False,
            state="UNKNOWN",
        )
    )
    PMOrderIntent.session.commit()
    exchange._pm_fetch_order_by_client_id = MagicMock()

    intent = exchange.list_pm_pending_intents()[0]
    assert intent["outbox_present"] is False
    result = exchange.resolve_pm_pending_intent(intent)
    assert result["uncertain"] is True
    assert "outbox" in result["error"]
    exchange._pm_fetch_order_by_client_id.assert_not_called()

    with pytest.raises(OperationalException, match="refusing a fresh client id"):
        exchange._pm_enqueue(
            "ft-fresh-bypass",
            {
                "kind": "order",
                "pair": "ETH/USDT:USDT",
                "side": "buy",
                "type": "limit",
                "amount": 1.0,
                "price": 100.0,
                "reduce_only": False,
            },
            payload={"symbol": "ETHUSDT", "newClientOrderId": "ft-fresh-bypass"},
        )
    assert PMOrderIntent.get_by_client_id("ft-missing-outbox") is not None
    assert PMOrderIntent.get_by_client_id("ft-fresh-bypass") is None

def test_marker_committed_crash_before_post_never_redispatches(mocker, default_conf_usdt):
    """Crash after the durable send marker but before POST is lookup-only forever."""
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._pm_enqueue(
        "ft-marker-crash",
        {
            "kind": "order",
            "pair": "ETH/USDT:USDT",
            "side": "buy",
            "type": "limit",
            "amount": 1.0,
            "price": 100.0,
            "reduce_only": False,
        },
        payload={"symbol": "ETHUSDT", "newClientOrderId": "ft-marker-crash"},
    )
    exchange._pm_dispatch_marker_set("ft-marker-crash")
    row = PMOutbox.get_by_client_id("ft-marker-crash")
    assert row.dispatch_started_at is not None
    assert row.dispatch_attempts == 1

    # Process disappears HERE: no POST happened. A restart must still refuse to
    # infer "unsent" from an absent lookup because the durable marker means the
    # process may have crossed the network boundary.
    exchange._pm_fetch_order_by_client_id = MagicMock(return_value=None)
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    report = exchange.pm_drain_outbox()

    assert report["acked"] == 0
    assert report["deferred"] == 1
    assert [c for c in exchange._papi_request.call_args_list if c.args[1] == "POST"] == []
    intent = PMOrderIntent.get_by_client_id("ft-marker-crash")
    assert intent is not None and intent.state == "UNKNOWN"
    assert PMOutbox.get_by_client_id("ft-marker-crash").dispatch_started_at is not None


def test_conditional_marker_committed_crash_before_post_never_redispatches(
    mocker, default_conf_usdt
):
    """Protective conditional marker-before-POST crash also stays same-ID lookup-only."""
    exchange = make_exchange(mocker, default_conf_usdt)
    request = {
        "algoType": "CONDITIONAL",
        "symbol": "ETHUSDT",
        "side": "SELL",
        "type": "STOP_MARKET",
        "reduceOnly": "true",
        "triggerPrice": 90,
        "workingType": "CONTRACT_PRICE",
        "clientAlgoId": "st-marker-crash",
        "quantity": 1,
    }
    exchange._pm_enqueue(
        "st-marker-crash",
        {
            "kind": "conditional",
            "pair": "ETH/USDT:USDT",
            "side": "sell",
            "type": "STOP_MARKET",
            "amount": 1.0,
            "stop_price": 90,
            "reduce_only": True,
            "origin_trade_id": 17,
        },
        payload=request,
    )
    exchange._pm_dispatch_marker_set("st-marker-crash")
    exchange.fetch_stoploss_order = MagicMock(side_effect=InvalidOrderException("not visible"))
    exchange._papi_request = MagicMock(return_value={"algoId": 777})

    report = exchange.pm_drain_outbox()

    assert report["acked"] == 0
    assert report["deferred"] == 1
    assert [c for c in exchange._papi_request.call_args_list if c.args[1] == "POST"] == []
    intent = PMOrderIntent.get_by_client_id("st-marker-crash")
    assert intent is not None and intent.state == "UNKNOWN"
    assert intent.origin_trade_id == 17
