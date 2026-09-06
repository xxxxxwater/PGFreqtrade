"""PM symbols must retain instrument ownership across every transport boundary."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from freqtrade.exceptions import OperationalException
from freqtrade.exchange.binance import Binance
from freqtrade.exchange.pm_identity import canonical_pm_pair
from freqtrade.persistence import PMOrderIntent, PMOutbox, init_db


PAIR = "DASH/USDT:USDT"


@pytest.fixture
def identity_markets():
    # Spot comes first, as in the production failure. Raw IDs collide.
    return {
        "DASH/USDT": {
            "id": "DASHUSDT",
            "symbol": "DASH/USDT",
            "spot": True,
            "contract": False,
            "settle": None,
            "inverse": False,
        },
        PAIR: {
            "id": "DASHUSDT",
            "symbol": PAIR,
            "spot": False,
            "swap": True,
            "contract": True,
            "settle": "USDT",
            "inverse": False,
            "linear": True,
        },
        "DASH/USD:DASH": {
            "id": "DASHUSD_PERP",
            "symbol": "DASH/USD:DASH",
            "spot": False,
            "contract": True,
            "settle": "DASH",
            "inverse": True,
            "linear": False,
        },
        "DASH/USDC:USDC": {
            "id": "DASHUSDC",
            "symbol": "DASH/USDC:USDC",
            "spot": False,
            "contract": True,
            "settle": "USDC",
            "inverse": False,
            "linear": True,
        },
    }


def make_exchange(markets):
    exchange = Binance.__new__(Binance)
    exchange._markets = markets
    exchange._exchange_ws = exchange._api_async = exchange._ws_async = exchange.loop = None
    exchange._api = SimpleNamespace(safe_symbol=MagicMock(return_value="DASH/USDT"))
    exchange._portfolio_margin = True
    exchange._config = {"dry_run": False, "exchange": {}}
    return exchange


@pytest.mark.parametrize("source", ["DASHUSDT", "DASH/USDT", PAIR])
def test_um_identity_never_selects_first_spot_market(identity_markets, source):
    assert canonical_pm_pair(identity_markets, source) == PAIR
    assert canonical_pm_pair(dict(reversed(list(identity_markets.items()))), source) == PAIR


def test_namespace_and_settlement_are_part_of_identity(identity_markets):
    assert canonical_pm_pair(identity_markets, "DASHUSD_PERP", namespace="cm") == "DASH/USD:DASH"
    assert canonical_pm_pair(identity_markets, "DASHUSDC") == "DASH/USDC:USDC"
    for source, namespace in [("DASHUSD_PERP", "um"), ("DASHUSDT", "cm"), (PAIR, "cm")]:
        with pytest.raises(OperationalException):
            canonical_pm_pair(identity_markets, source, namespace=namespace)
    with pytest.raises(OperationalException):
        canonical_pm_pair(identity_markets, "DASH/USDT:USDC")


@pytest.mark.parametrize("source", ["DASHUSDT", "DASH/USDT"])
def test_ambiguous_derivative_mapping_refuses_guess(identity_markets, source):
    duplicate = "DASH/USDT:USDT-261225"
    identity_markets[duplicate] = {**identity_markets[PAIR], "symbol": duplicate, "swap": False}
    with pytest.raises(OperationalException, match="ambiguous"):
        canonical_pm_pair(identity_markets, source)
    # A full settled/delivery symbol remains unambiguous.
    assert canonical_pm_pair(identity_markets, PAIR) == PAIR


@pytest.mark.parametrize(
    "changes",
    [{"spot": True}, {"contract": False}, {"settle": None}, {"inverse": None}, {"option": True}],
)
def test_incomplete_or_noncontract_metadata_is_rejected(identity_markets, changes):
    identity_markets[PAIR].update(changes)
    with pytest.raises(OperationalException):
        canonical_pm_pair(identity_markets, "DASHUSDT")


def test_legacy_alias_requires_explicit_allowance_and_loaded_contract(identity_markets):
    with pytest.raises(OperationalException):
        canonical_pm_pair(identity_markets, "DASH/USDT", allow_legacy_alias=False)
    del identity_markets[PAIR]
    with pytest.raises(OperationalException):
        canonical_pm_pair(identity_markets, "DASH/USDT")


def test_terminal_events_can_resolve_inactive_contract(identity_markets):
    identity_markets[PAIR]["active"] = False
    assert canonical_pm_pair(identity_markets, "DASHUSDT") == PAIR


@pytest.mark.parametrize(
    "parser,payload",
    [
        ("_parse_pm_order", {"symbol": "DASHUSDT", "orderId": 10660398840, "status": "FILLED"}),
        ("_parse_pm_trade", {"symbol": "DASHUSDT", "orderId": 10660398840, "id": 71}),
        ("_parse_pm_conditional_order", {"symbol": "DASHUSDT", "algoId": 72}),
        ("_parse_pm_position", {"symbol": "DASHUSDT", "positionAmt": "723.589"}),
    ],
)
def test_all_rest_parsers_share_settled_identity(identity_markets, parser, payload):
    exchange = make_exchange(identity_markets)
    assert getattr(exchange, parser)(payload)["symbol"] == PAIR
    exchange._api.safe_symbol.assert_not_called()


@pytest.mark.parametrize(
    "parser", ["_parse_pm_order", "_parse_pm_trade", "_parse_pm_conditional_order"]
)
def test_response_must_match_requested_instrument(identity_markets, parser):
    exchange = make_exchange(identity_markets)
    with pytest.raises(OperationalException, match="does not match"):
        getattr(exchange, parser)({"symbol": "DASHUSDC"}, PAIR)


def test_unknown_position_is_not_silently_reported_as_zero(identity_markets):
    exchange = make_exchange(identity_markets)
    with pytest.raises(OperationalException):
        exchange._parse_pm_position({"symbol": "UNKNOWNUSDT", "positionAmt": "5"})


def test_intent_enqueue_and_legacy_recovery_use_same_canonical_key(
    identity_markets, default_conf_usdt
):
    init_db(default_conf_usdt["db_url"])
    exchange = make_exchange(identity_markets)
    exchange._pm_enqueue(
        "ftidentity", {"kind": "order", "pair": "DASH/USDT"}, payload={"symbol": "DASHUSDT"}
    )
    assert PMOrderIntent.get_by_client_id("ftidentity").pair == PAIR
    assert PMOutbox.get_by_client_id("ftidentity") is not None
    # Old aliases are resolved at read boundaries, without a bulk database rewrite.
    PMOrderIntent.get_by_client_id("ftidentity").pair = "DASH/USDT"
    PMOrderIntent.session.commit()
    assert exchange.list_pm_pending_intents()[0]["pair"] == PAIR
    assert exchange.pm_has_unresolved_intents_for_pair(PAIR)
    exchange._pm_fetch_order_by_client_id = MagicMock(return_value=None)
    report = exchange.resolve_pm_pending_intent({"client_id": "ftidentity", "pair": "DASH/USDT"})
    assert report["pair"] == PAIR
    exchange._pm_fetch_order_by_client_id.assert_called_once_with("ftidentity", PAIR)


def test_outbox_payload_identity_mismatch_is_rejected_before_persist(
    identity_markets, default_conf_usdt
):
    init_db(default_conf_usdt["db_url"])
    exchange = make_exchange(identity_markets)
    with pytest.raises(OperationalException, match="does not match"):
        exchange._pm_enqueue(
            "ftwrong", {"kind": "order", "pair": PAIR}, payload={"symbol": "DASHUSDC"}
        )
    assert PMOrderIntent.get_by_client_id("ftwrong") is None
    assert PMOutbox.get_by_client_id("ftwrong") is None


@pytest.mark.parametrize(
    "intent",
    [
        {},
        {"pair": PAIR},
        {"client_id": "ftmissing"},
        {"client_id": "ftunknown", "pair": "UNKNOWN/USDT"},
    ],
)
def test_malformed_or_unknown_intent_stays_uncertain(identity_markets, intent):
    exchange = make_exchange(identity_markets)
    exchange._pm_fetch_order_by_client_id = MagicMock()
    report = exchange.resolve_pm_pending_intent(intent)
    assert report["uncertain"] is True
    assert report["resolved"] is False
    assert report["exists"] is None
    exchange._pm_fetch_order_by_client_id.assert_not_called()


@pytest.mark.parametrize("method", ["_pm_dispatch_order", "_pm_dispatch_conditional"])
def test_dispatch_revalidates_payload_before_post(identity_markets, method):
    exchange = make_exchange(identity_markets)
    exchange._papi_request = MagicMock()
    with pytest.raises(OperationalException, match="does not match"):
        getattr(exchange, method)("ftwrong", PAIR, {"symbol": "DASHUSDC"})
    exchange._papi_request.assert_not_called()


def test_legacy_pending_intent_supersession_uses_canonical_pair(
    identity_markets, default_conf_usdt
):
    init_db(default_conf_usdt["db_url"])
    exchange = make_exchange(identity_markets)
    exchange._pm_enqueue("ftlegacy", {"kind": "order", "pair": PAIR})
    PMOrderIntent.get_by_client_id("ftlegacy").pair = "DASH/USDT"
    PMOrderIntent.session.commit()
    exchange._pm_enqueue("ftnew", {"kind": "order", "pair": PAIR})
    assert PMOrderIntent.get_by_client_id("ftlegacy") is None
    assert PMOutbox.get_by_client_id("ftlegacy").state == "REJECTED"
    assert PMOrderIntent.get_by_client_id("ftnew").pair == PAIR


@pytest.mark.parametrize(
    "operation,method,raw_id",
    [
        ("order", "_pm_dispatch_order", {"orderId": 10660398840}),
        ("conditional", "_pm_dispatch_conditional", {"algoId": 10660398840}),
    ],
)
def test_identity_mismatch_after_ack_keeps_raw_evidence_and_blocks_replacement(
    identity_markets, default_conf_usdt, operation, method, raw_id
):
    init_db(default_conf_usdt["db_url"])
    exchange = make_exchange(identity_markets)
    exchange._pm_enqueue(
        "ftbadack", {"kind": operation, "pair": PAIR}, payload={"symbol": "DASHUSDT"}
    )
    exchange._papi_request = MagicMock(return_value={**raw_id, "symbol": "DASHUSDC"})
    with pytest.raises(OperationalException, match="does not match"):
        getattr(exchange, method)("ftbadack", PAIR, {"symbol": "DASHUSDT"})
    intent = PMOrderIntent.get_by_client_id("ftbadack")
    assert intent.state == "UNKNOWN"
    assert intent.exchange_order_id == "10660398840"
    assert "DASHUSDC" in intent.raw_response
    outbox = PMOutbox.get_by_client_id("ftbadack")
    assert outbox.state == "ACKED"
    assert "DASHUSDC" in outbox.raw_response
    assert outbox not in PMOutbox.get_pending()
    assert exchange.pm_has_unresolved_intents_for_pair(PAIR)
