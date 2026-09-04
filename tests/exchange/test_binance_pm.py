"""
Tests for the Binance standard Portfolio Margin adapter (PAPI).

Covers PAPI routing (no FAPI fallback), error-code classification for the raw
HTTP fallback, clientOrderId idempotency and the PM order-query surface.
"""

import logging
from unittest.mock import MagicMock

import ccxt
import pytest

from freqtrade.exceptions import (
    InsufficientFundsError,
    InvalidOrderException,
    OperationalException,
    TemporaryError,
)
from freqtrade.exchange.binance import Binance
from tests.conftest import get_patched_exchange


@pytest.fixture(autouse=True)
def pm_intent_db(default_conf_usdt):
    """Bind the main DB (incl. the pm_order_intents table) for PM exchange tests."""
    from freqtrade.persistence import init_db

    init_db(default_conf_usdt["db_url"])


def get_patched_pm_exchange(mocker, default_conf_usdt, api_mock=None, mock_markets=True):
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
    api_mock = api_mock if api_mock is not None else MagicMock()
    return get_patched_exchange(
        mocker, conf, api_mock=api_mock, exchange="binance", mock_markets=mock_markets
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


PM_ALGO_ORDER = {
    "algoId": 55,
    "clientAlgoId": "stabc",
    "algoStatus": "NEW",
    "algoType": "CONDITIONAL",
    "symbol": "ETHUSDT",
    "side": "SELL",
    "orderType": "STOP_MARKET",
    "quantity": "1",
    "triggerPrice": "95",
}


def test_papi_request_uses_papi_namespace(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._api.request = MagicMock(return_value={"ok": 1})

    result = exchange._papi_request("/papi/v1/um/order", "POST", {"symbol": "ETHUSDT"})

    assert result == {"ok": 1}
    # Path is normalized and the ccxt "papi" namespace is used (never "private"/"fapi").
    assert exchange._api.request.call_args[0][0] == "um/order"
    assert exchange._api.request.call_args[0][1] == "papi"
    assert exchange._api.request.call_args[0][2] == "POST"
    assert getattr(exchange, "_last_papi_success_time", None) is not None


def test_pm_tradable_pairs_uses_account_symbol_configuration(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(
        mocker,
        default_conf_usdt,
        mock_markets={
            "BTC/USDT:USDT": {"id": "BTCUSDT", "settle": "USDT", "inverse": False},
            "AAPL/USDT:USDT": {"id": "AAPLUSDT", "settle": "USDT", "inverse": False},
            "ETH/USDT:USDT": {"id": "ETHUSDT", "settle": "USDT", "inverse": False},
        },
    )
    exchange._papi_request = MagicMock(
        return_value=[
            {"symbol": "BTCUSDT", "marginType": "CROSSED", "maxNotionalValue": "100000"},
            {"symbol": "AAPLUSDT", "marginType": "CROSSED", "maxNotionalValue": "5000"},
            {"symbol": "ETHUSDT", "marginType": "ISOLATED", "maxNotionalValue": "100000"},
        ]
    )

    assert exchange.get_pm_tradable_pairs() == {"BTC/USDT:USDT", "AAPL/USDT:USDT"}
    exchange._papi_request.assert_called_once_with("um/symbolConfig", "GET")
    # A cached symbol configuration avoids a private PAPI request on every pairlist refresh.
    assert exchange.get_pm_tradable_pairs() == {"BTC/USDT:USDT", "AAPL/USDT:USDT"}
    exchange._papi_request.assert_called_once()


@pytest.mark.parametrize(
    "code,expected",
    [
        (-2015, "auth"),
        (-2014, "auth"),
        (-1022, "auth"),
        (-1002, "auth"),
        (-1003, "rate_limit"),
        (-1015, "rate_limit"),
        (-2018, "insufficient_funds"),
        (-2019, "insufficient_funds"),
        (-2010, "order_rejected"),
        (-2022, "order_rejected"),
        (-1021, "invalid_request"),
        (-1102, "invalid_request"),
        (-12345, "other"),
        (None, "unknown"),
    ],
)
def test_binance_error_category(default_conf_usdt, mocker, code, expected):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    assert exchange._binance_error_category(code) == expected


def test_papi_http_exception_400_is_not_always_auth(default_conf_usdt, mocker):
    """
    A HTTP 400 with an invalid-parameter code must NOT be reported as an auth failure.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)

    exc = exchange._papi_http_exception(
        "um/order", "POST", '{"code": -1102, "msg": "Mandatory param empty."}', 400
    )
    assert isinstance(exc, InvalidOrderException)
    assert not isinstance(exc, OperationalException)

    auth_exc = exchange._papi_http_exception(
        "um/order", "POST", '{"code": -2015, "msg": "Rejected by MBX."}', 400
    )
    assert isinstance(auth_exc, OperationalException)

    funds_exc = exchange._papi_http_exception(
        "um/order", "POST", '{"code": -2018, "msg": "Balance not sufficient."}', 400
    )
    assert isinstance(funds_exc, InsufficientFundsError)


def test_create_order_pm_routes_to_papi_and_uses_client_order_id(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    exchange.assert_pm_risk_allows_order = MagicMock()
    exchange._lev_prep = MagicMock()
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=100)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)

    exchange.create_order(
        pair="ETH/USDT:USDT",
        ordertype="limit",
        side="buy",
        amount=1,
        rate=100,
        leverage=5,
    )

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/order"
    assert method == "POST"
    assert params["newClientOrderId"].startswith("ft")
    assert len(params["newClientOrderId"]) <= 32
    assert params["side"] == "BUY"
    assert params["type"] == "LIMIT"
    assert params["symbol"] == "ETH_USDT"


def test_pm_leverage_prep_skips_standard_margin_mode_and_uses_papi(default_conf_usdt, mocker):
    """PM must never call FAPI/CCXT setMarginMode before a PAPI order."""
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._api.set_margin_mode = MagicMock()
    exchange._papi_request = MagicMock(return_value={"leverage": 1})

    exchange._lev_prep("ETH/USDT:USDT", 1.0, "buy")

    exchange._api.set_margin_mode.assert_not_called()
    exchange._papi_request.assert_called_once_with(
        "um/leverage", "POST", {"symbol": "ETH_USDT", "leverage": 1}
    )


def test_pm_stoploss_channel_check_uses_algo_endpoint_and_caches(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._config["order_types"] = {"stoploss_on_exchange": True}
    exchange._papi_request = MagicMock(return_value=[])

    exchange.assert_pm_stoploss_channel_available()
    exchange.assert_pm_stoploss_channel_available()

    exchange._papi_request.assert_called_once_with(
        "um/algo/openAlgoOrders", "GET", {"algoType": "CONDITIONAL"}
    )


def test_pm_stoploss_channel_check_blocks_new_exposure_when_unavailable(
    default_conf_usdt, mocker
):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._config["order_types"] = {"stoploss_on_exchange": True}
    exchange._papi_request = MagicMock(side_effect=ccxt.ExchangeNotAvailable("404 Not Found"))

    with pytest.raises(OperationalException, match="stoploss channel"):
        exchange.assert_pm_stoploss_channel_available()


def test_create_stoploss_pm_routes_to_algo_order(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=PM_ALGO_ORDER)
    exchange._lev_prep = MagicMock()
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=95)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)

    order = exchange.create_stoploss(
        pair="ETH/USDT:USDT",
        amount=1,
        stop_price=95,
        side="sell",
        leverage=5,
        order_types={"stoploss": "market"},
    )

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/algo/order"
    assert method == "POST"
    assert params.get("reduceOnly") == "true"
    assert params.get("algoType") == "CONDITIONAL"
    assert params.get("triggerPrice") is not None
    assert params.get("type") == "STOP_MARKET"
    assert params.get("clientAlgoId", "").startswith("st")
    assert len(params.get("clientAlgoId", "")) <= 32
    assert params["side"] == "SELL"
    # The stored id is the client strategy id, which freqtrade later fetches/cancels by.
    assert order["id"].startswith("st")


def test_fetch_stoploss_order_pm_routes_to_algo_order(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=PM_ALGO_ORDER)

    order = exchange.fetch_stoploss_order("stabc", "ETH/USDT:USDT")

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/algo/algoOrder"
    assert method == "GET"
    assert params["clientAlgoId"] == "stabc"
    assert order["status"] == "open"


def test_cancel_stoploss_order_pm_routes_to_algo_order(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value={"complete": True})

    result = exchange.cancel_stoploss_order("stabc", "ETH/USDT:USDT")

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/algo/order"
    assert method == "DELETE"
    assert params["clientAlgoId"] == "stabc"
    assert result["status"] == "canceled"


def test_cancel_stoploss_order_gone_is_definitive_no_retry(default_conf_usdt, mocker):
    """
    -2011 (Unknown order sent) means the conditional is DEFINITIVELY gone.
    It must surface as InvalidOrderException (never retried by the @retrier
    layer) so a redelivered ORDER_TRADE_UPDATE cannot produce a 5x retry
    storm for every event.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(
        side_effect=ccxt.OperationRejected('binance {"code":-2011,"msg":"Unknown order sent."}')
    )

    with pytest.raises(InvalidOrderException, match="no longer exists"):
        exchange.cancel_stoploss_order("stabc", "ETH/USDT:USDT")

    # The @retrier wrapper must have called the underlying method exactly once.
    assert exchange._papi_request.call_count == 1


def test_cancel_stoploss_order_transient_error_still_temporary(default_conf_usdt, mocker):
    """A genuinely transient error keeps its retryable classification."""
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(
        side_effect=ccxt.ExchangeError("binance temporary gateway failure")
    )

    with pytest.raises(TemporaryError, match="Could not cancel Binance PM stoploss"):
        exchange.cancel_stoploss_order("stabc", "ETH/USDT:USDT")



def test_cancel_order_pm_strips_stop_flag(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)

    exchange.cancel_order("123", "ETH/USDT:USDT", params={"stop": True})

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/order"
    assert method == "DELETE"
    assert "stop" not in params


def test_fetch_order_pm_routes_to_papi(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)

    exchange.fetch_order("123", "ETH/USDT:USDT")

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/order"
    assert method == "GET"
    assert params["orderId"] == "123"
    assert params["symbol"] == "ETH_USDT"


def test_fetch_open_orders_pm_routes_to_papi(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=[PM_ORDER])
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)

    orders = exchange.fetch_open_orders("ETH/USDT:USDT")

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/openOrders"
    assert method == "GET"
    assert params["symbol"] == "ETH_USDT"
    assert len(orders) == 1


def test_fetch_orders_pm_routes_to_papi(default_conf_usdt, mocker):
    from freqtrade.util.datetime_helpers import dt_utc

    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=[PM_ORDER])
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)

    exchange.fetch_orders("ETH/USDT:USDT", dt_utc(2024, 1, 1))

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/allOrders"
    assert method == "GET"
    assert "startTime" in params


def test_get_trades_for_order_pm_routes_to_papi_user_trades(default_conf_usdt, mocker):
    from freqtrade.util.datetime_helpers import dt_utc

    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(
        return_value=[
            {
                "id": 1,
                "orderId": 123,
                "symbol": "ETHUSDT",
                "side": "BUY",
                "price": "100",
                "qty": "1",
                "quoteQty": "100",
                "commission": "0.1",
                "commissionAsset": "USDT",
                "time": 1700000000000,
                "maker": True,
            }
        ]
    )
    exchange._trades_contracts_to_amount = MagicMock(side_effect=lambda t: t)

    exchange.get_trades_for_order("123", "ETH/USDT:USDT", dt_utc(2024, 1, 1))

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/userTrades"
    assert method == "GET"
    assert params["symbol"] == "ETH_USDT"


def test_pm_place_order_idempotent_resolves_existing(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)

    # First POST raises a transient error; the follow-up GET by clientOrderId
    # returns the already-placed order. No duplicate must be submitted.
    exchange._papi_request = MagicMock(
        side_effect=[
            TemporaryError("timeout"),
            dict(PM_ORDER),
        ]
    )

    order = exchange._pm_place_order(
        "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
    )

    assert order["id"] == "123"
    assert exchange._papi_request.call_count == 2
    # Second call must be a lookup by clientOrderId, not another POST.
    assert exchange._papi_request.call_args_list[1][0][1] == "GET"
    assert exchange._papi_request.call_args_list[1][0][0] == "um/order"


def test_pm_place_order_reraises_when_not_found(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    # POST fails and the follow-up lookup also fails (order unknown) -> re-raise.
    exchange._papi_request = MagicMock(side_effect=TemporaryError("timeout"))
    exchange._pm_fetch_order_by_client_id = MagicMock(return_value=None)

    with pytest.raises(TemporaryError):
        exchange._pm_place_order(
            "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
        )


@pytest.mark.parametrize(
    "error",
    [
        ccxt.InvalidOrder("invalid order"),
        ccxt.InsufficientFunds("insufficient funds"),
        ccxt.BadRequest("bad request"),
        ccxt.OperationRejected("operation rejected"),
    ],
)
def test_pm_place_order_deterministic_rejections_skip_idempotency_lookup(
    default_conf_usdt, mocker, error
):
    """Known rejections are not reclassified as uncertain transient failures."""
    from freqtrade.persistence.pm_order_intent import PMOrderIntent
    from freqtrade.persistence.pm_outbox import PMOutbox

    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=error)
    exchange._pm_fetch_order_by_client_id = MagicMock()

    with pytest.raises(type(error)):
        exchange._pm_place_order(
            "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
        )

    exchange._pm_fetch_order_by_client_id.assert_not_called()
    # Deterministic rejection: intent is tombstoned (never uncertain), and the
    # outbox keeps a REJECTED audit row with the error.
    assert PMOrderIntent.get_unresolved() == []
    rejected = (
        PMOutbox.session.query(PMOutbox).filter(PMOutbox.state == "REJECTED").all()
    )
    assert len(rejected) == 1
    assert str(error) in (rejected[0].last_error or "")


def test_pm_place_order_returns_confirmed_order_when_intent_cleanup_fails(
    default_conf_usdt, mocker, caplog
):
    """
    A submitted order must reach the trade lifecycle even if the ACK evidence
    cannot be persisted. The intent stays PREPARED and keeps blocking new
    exposure until recovery links it - the local order row itself preserves the
    exchange id as evidence.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._pm_ack = MagicMock(side_effect=OperationalException("database unavailable"))
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda order: order)

    with caplog.at_level(logging.CRITICAL):
        order = exchange._pm_place_order(
            "ETH/USDT:USDT", "limit", "buy", 1, 100, {}, log_tag="papi_create_order"
        )

    assert order["id"] == "123"
    assert "new exposure stays blocked" in caplog.text
    intents = exchange.list_pm_pending_intents()
    assert len(intents) == 1
    assert intents[0]["state"] == "PREPARED"


def test_pm_new_client_order_id_is_unique(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    ids = {exchange._pm_new_client_order_id() for _ in range(100)}
    assert len(ids) == 100
    assert all(i.startswith("ft") and len(i) <= 32 for i in ids)


def test_get_funding_fees_pm_routes_to_papi_income(default_conf_usdt, mocker):
    from freqtrade.util.datetime_helpers import dt_utc

    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(
        return_value=[{"income": "-0.5", "symbol": "ETHUSDT", "type": "FUNDING_FEE"}]
    )

    total = exchange._get_funding_fees_from_exchange("ETH/USDT:USDT", dt_utc(2024, 1, 1))

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/income"
    assert method == "GET"
    assert params["incomeType"] == "FUNDING_FEE"
    assert total == -0.5


def test_fetch_stoploss_order_falls_back_to_history(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(
        side_effect=[
            ccxt.OrderNotFound("Order does not exist"),
            [{**PM_ALGO_ORDER, "algoStatus": "CANCELED"}],
        ]
    )

    order = exchange.fetch_stoploss_order("stabc", "ETH/USDT:USDT")

    assert exchange._papi_request.call_count == 2
    assert exchange._papi_request.call_args_list[1][0][0] == "um/algo/allAlgoOrders"
    assert order["status"] == "canceled"


def test_fetch_order_by_client_id_propagates_transient_error(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=TemporaryError("network down"))

    # A transient lookup failure must NOT be reported as "order does not exist".
    with pytest.raises(TemporaryError):
        exchange._pm_fetch_order_by_client_id("ftabc", "ETH/USDT:USDT")


def test_fetch_order_by_client_id_returns_none_on_not_found(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=ccxt.OrderNotFound("Order does not exist"))

    assert exchange._pm_fetch_order_by_client_id("ftabc", "ETH/USDT:USDT") is None


TRIGGERED_CONDITIONAL = {
    "algoId": 55,
    "clientAlgoId": "stabc",
    "algoStatus": "TRIGGERED",
    "algoType": "CONDITIONAL",
    "symbol": "ETHUSDT",
    "side": "SELL",
    "orderType": "STOP_MARKET",
    "quantity": "1",
    "triggerPrice": "95",
    # Official Query UM Algo Order response fields after trigger:
    "actualOrderId": 999,
    "actualOrderStatus": "FILLED",
    "triggerTime": 1700000000000,
}


def _real_pm_order(order_id=999, status="FILLED", filled=1.0, average=94.0):
    return {
        "id": str(order_id),
        "clientOrderId": None,
        "timestamp": 1700000000000,
        "datetime": None,
        "lastTradeTimestamp": None,
        "symbol": "ETH/USDT:USDT",
        "type": "market",
        "timeInForce": "GTC",
        "side": "sell",
        "price": 94.0,
        "average": average if status == "FILLED" else None,
        "amount": 1.0,
        "filled": filled,
        "remaining": max(1.0 - filled, 0.0),
        "cost": filled * average,
        "status": "closed" if status == "FILLED" else "open",
        "fee": {"cost": 0.01, "currency": "USDT"},
        "trades": [],
        "info": {},
    }


def test_fetch_stoploss_triggered_resolves_real_filled_order(default_conf_usdt, mocker):
    """
    STOP_MARKET triggered + real order FILLED: the stoploss order must carry the
    REAL fill data (filled/average/cost/fee) and close - never TRIGGERED=>closed
    with fabricated zero fills.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(
        side_effect=[ccxt.OrderNotFound("Order does not exist"), [dict(TRIGGERED_CONDITIONAL)]]
    )
    exchange.fetch_order = MagicMock(return_value=_real_pm_order())

    order = exchange.fetch_stoploss_order("stabc", "ETH/USDT:USDT")

    assert exchange._papi_request.call_count == 2
    assert exchange.fetch_order.call_args[0][0] == "999"
    assert order["id"] == "stabc"  # freqtrade keeps matching by the strategy id
    assert order["id_stop"] == "999"
    assert order["status_stop"] == "triggered"
    assert order["status"] == "closed"
    assert order["filled"] == 1.0
    assert order["average"] == 94.0
    assert order["cost"] == 94.0
    assert order["fee"] == {"cost": 0.01, "currency": "USDT"}
    assert order["info"]["actual_order_id"] == "999"


def test_fetch_stoploss_triggered_real_order_still_open_stays_open(default_conf_usdt, mocker):
    """
    STOP (limit) triggered but the real order has NOT filled yet: the strategy must
    stay open - closing it would leave the position unprotected / wrongly flat.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(
        side_effect=[ccxt.OrderNotFound("Order does not exist"), [dict(TRIGGERED_CONDITIONAL)]]
    )
    exchange.fetch_order = MagicMock(return_value=_real_pm_order(status="NEW", filled=0.0))

    order = exchange.fetch_stoploss_order("stabc", "ETH/USDT:USDT")

    assert order["status"] == "open"
    assert order["filled"] == 0.0
    assert order["id_stop"] == "999"
    assert order["status_stop"] == "triggered"


def test_fetch_stoploss_triggered_partially_filled_stays_open(
    default_conf_usdt, mocker
):
    """
    TRIGGERED + real order PARTIALLY_FILLED: the strategy must STAY OPEN and carry
    the real partial fill data - a partial fill must never close the trade.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(
        side_effect=[ccxt.OrderNotFound("Order does not exist"), [dict(TRIGGERED_CONDITIONAL)]]
    )
    exchange.fetch_order = MagicMock(
        return_value=_real_pm_order(status="PARTIALLY_FILLED", filled=0.4, average=94.0)
    )

    order = exchange.fetch_stoploss_order("stabc", "ETH/USDT:USDT")

    assert order["status"] == "open"
    assert order["filled"] == 0.4
    assert order["remaining"] == 0.6
    assert order["id"] == "stabc"
    assert order["id_stop"] == "999"
    assert order["info"]["actual_order_id"] == "999"


def test_fetch_stoploss_expired_marks_expired(default_conf_usdt, mocker):
    """
    algoStatus=EXPIRED: the conditional order is definitively finished without
    a fill - status must be 'expired', never fabricated as filled/closed.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    expired_conditional = dict(TRIGGERED_CONDITIONAL)
    expired_conditional["algoStatus"] = "EXPIRED"
    expired_conditional.pop("actualOrderId", None)
    exchange._papi_request = MagicMock(
        side_effect=[ccxt.OrderNotFound("Order does not exist"), [expired_conditional]]
    )

    order = exchange.fetch_stoploss_order("stabc", "ETH/USDT:USDT")

    assert order["status"] == "expired"
    assert order["filled"] == 0.0
    assert order["cost"] == 0.0


def test_conditional_status_mapping():
    """Direct mapping sanity for the official algoStatus values."""
    assert Binance._pm_conditional_status("NEW") == "open"
    assert Binance._pm_conditional_status("TRIGGERED") == "open"
    assert Binance._pm_conditional_status("CANCELLED") == "canceled"
    assert Binance._pm_conditional_status("EXPIRED") == "expired"
    assert Binance._pm_conditional_status("FINISHED") == "closed"
    assert Binance._pm_conditional_status(None) is None


def test_fetch_stoploss_triggered_missing_order_id_fails_closed(default_conf_usdt, mocker):
    """
    TRIGGERED without a real ``actualOrderId`` (unexpected): never fabricate a fill.
    The strategy stays open and the diagnostics flag it.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    triggered_without_real_order = {
        k: v for k, v in TRIGGERED_CONDITIONAL.items() if k != "actualOrderId"
    }
    exchange._papi_request = MagicMock(
        side_effect=[
            ccxt.OrderNotFound("Order does not exist"),
            [triggered_without_real_order],
        ]
    )

    order = exchange.fetch_stoploss_order("stabc", "ETH/USDT:USDT")

    assert order["status"] == "open"
    assert order["filled"] == 0.0
    assert order["info"]["actual_order_id"] is None


def test_fetch_stoploss_transient_open_lookup_propagates(default_conf_usdt, mocker):
    """
    A transient openOrder lookup failure must propagate as TemporaryError so the
    caller aborts instead of treating the stoploss as absent (no duplicate SL).
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=ccxt.NetworkError("timeout"))

    with pytest.raises(TemporaryError):
        exchange.fetch_stoploss_order("stabc", "ETH/USDT:USDT")


def test_fetch_stoploss_definitively_absent_raises_invalid_order(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(
        side_effect=[
            ccxt.OrderNotFound("Order does not exist"),
            ccxt.OrderNotFound("Order does not exist"),
        ]
    )

    with pytest.raises(InvalidOrderException):
        exchange.fetch_stoploss_order("stabc", "ETH/USDT:USDT")


def test_create_stoploss_persists_intent_before_post_and_keeps_ack_evidence(
    default_conf_usdt, mocker, tmp_path
):
    """
    The intent + outbox are persisted BEFORE the POST; after the ACK the intent
    stays ACKED with the exchange id and raw response (it is only tombstoned
    once the local Trade/Order commit links it).
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._config["user_data_dir"] = str(tmp_path)
    exchange._papi_request = MagicMock(
        return_value={
            "strategyId": 55,
            "newClientStrategyId": "stabc",
            "strategyStatus": "NEW",
            "symbol": "ETHUSDT",
            "side": "SELL",
            "strategyType": "STOP_MARKET",
            "quantity": "1",
            "stopPrice": "95",
        }
    )
    exchange._lev_prep = MagicMock()
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=95)

    seen = {}
    real_enqueue = exchange._pm_enqueue

    def spy_enqueue(client_id, intent, *, payload=None):
        seen["put_before_post"] = exchange._papi_request.call_count == 0
        real_enqueue(client_id, intent, payload=payload)

    exchange._pm_enqueue = spy_enqueue
    order = exchange.create_stoploss(
        pair="ETH/USDT:USDT",
        amount=1,
        stop_price=95,
        side="sell",
        leverage=5,
        order_types={"stoploss": "market"},
    )

    assert seen["put_before_post"] is True
    assert order["id"] == "stabc"
    # ACKed (not deleted): evidence preserved until the local commit links it.
    intents = exchange.list_pm_pending_intents()
    assert len(intents) == 1
    assert intents[0]["state"] == "ACKED"
    assert intents[0]["exchange_order_id"] == "stabc"
    assert intents[0]["reduce_only"] is True
    assert "stabc" in (intents[0]["raw_response"] or "")


def test_create_stoploss_timeout_marks_intent_unknown(default_conf_usdt, mocker, tmp_path):
    """
    A POST timeout leaves a UNKNOWN intent so a process restart can resolve the
    same client strategy id instead of submitting a duplicate stoploss.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._config["user_data_dir"] = str(tmp_path)
    exchange._papi_request = MagicMock(side_effect=ccxt.NetworkError("timeout"))
    exchange._lev_prep = MagicMock()
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=95)

    with pytest.raises(TemporaryError):
        exchange.create_stoploss(
            pair="ETH/USDT:USDT",
            amount=1,
            stop_price=95,
            side="sell",
            leverage=5,
            order_types={"stoploss": "market"},
        )

    intents = exchange.list_pm_pending_intents()
    assert len(intents) == 1
    assert intents[0]["state"] == "UNKNOWN"
    assert intents[0]["kind"] == "conditional"
    assert intents[0]["client_id"].startswith("st")


def test_place_order_uncertain_marks_intent_unknown(default_conf_usdt, mocker, tmp_path):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._config["user_data_dir"] = str(tmp_path)
    exchange._papi_request = MagicMock(side_effect=TemporaryError("submit failed"))
    exchange._pm_fetch_order_by_client_id = MagicMock(side_effect=TemporaryError("lookup failed"))

    with pytest.raises(TemporaryError):
        exchange._pm_place_order(
            "ETH/USDT:USDT",
            "limit",
            "buy",
            1.0,
            100.0,
            {},
            log_tag="papi_create_order",
        )

    intents = exchange.list_pm_pending_intents()
    assert len(intents) == 1
    assert intents[0]["state"] == "UNKNOWN"
    assert intents[0]["kind"] == "order"
    assert intents[0]["pair"] == "ETH/USDT:USDT"


def test_resolve_pm_pending_intent_cases(default_conf_usdt, mocker, tmp_path):
    """Startup intent resolution: absent/existing/uncertain each resolve correctly."""
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._config["user_data_dir"] = str(tmp_path)
    intent = {"client_id": "stabc", "kind": "conditional", "pair": "ETH/USDT:USDT"}

    exchange.fetch_stoploss_order = MagicMock(side_effect=InvalidOrderException("gone"))
    report = exchange.resolve_pm_pending_intent(intent)
    assert report["resolved"] is True
    assert report["exists"] is False

    exchange.fetch_stoploss_order = MagicMock(return_value={"id": "stabc", "status": "open"})
    report = exchange.resolve_pm_pending_intent(intent)
    assert report["resolved"] is True
    assert report["exists"] is True
    assert report["order"]["id"] == "stabc"

    exchange.fetch_stoploss_order = MagicMock(side_effect=TemporaryError("down"))
    report = exchange.resolve_pm_pending_intent(intent)
    assert report["resolved"] is False
    assert report["uncertain"] is True


def test_fetch_open_conditional_orders_routes_papi(default_conf_usdt, mocker):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=[PM_ALGO_ORDER])

    orders = exchange.fetch_open_conditional_orders()

    path, method, params = exchange._papi_request.call_args[0]
    assert path == "um/algo/openAlgoOrders"
    assert method == "GET"
    assert params["algoType"] == "CONDITIONAL"
    assert len(orders) == 1
    assert orders[0]["id"] == "stabc"


# ---------------------------------------------------------------------------
# Round 4 P0: durable intent store fail-closed + same-id idempotency
# ---------------------------------------------------------------------------


def test_intent_store_write_failure_blocks_order_post(default_conf_usdt, mocker):
    """
    P0-1: if the intent cannot be durably committed, the PAPI POST must never
    run (0 calls) - for normal orders AND conditional stoploss orders.
    """
    from freqtrade.persistence.pm_order_intent import PMOrderIntent

    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    exchange._lev_prep = MagicMock()
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=100)
    exchange.assert_pm_risk_allows_order = MagicMock()
    # Gate reads work; the WRITE fails (e.g. disk full).
    mocker.patch.object(PMOrderIntent, "has_unresolved", return_value=False)
    mocker.patch.object(PMOrderIntent, "get_unresolved", return_value=[])
    mocker.patch.object(PMOrderIntent.session, "add", side_effect=RuntimeError("disk full"))

    # Normal order (entry)
    with pytest.raises(OperationalException):
        exchange.create_order(
            pair="ETH/USDT:USDT",
            ordertype="limit",
            side="buy",
            amount=1.0,
            rate=100,
            leverage=1,
            reduceOnly=False,
        )
    assert exchange._papi_request.call_count == 0

    # Conditional stoploss order
    with pytest.raises(OperationalException):
        exchange.create_stoploss(
            pair="ETH/USDT:USDT",
            amount=1,
            stop_price=95,
            side="sell",
            leverage=5,
            order_types={"stoploss": "market"},
        )
    assert exchange._papi_request.call_count == 0


def test_unresolved_intent_gate_blocks_new_orders_and_allows_reduceonly(
    default_conf_usdt, mocker
):
    """
    P0-2: a UNKNOWN intent blocks every new entry/DCA/stoploss immediately in
    the same process, but reduceOnly exit orders are never blocked.
    """
    from freqtrade.persistence.pm_order_intent import PMOrderIntent

    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=PM_ORDER)
    exchange._lev_prep = MagicMock()
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=100)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)
    exchange.assert_pm_risk_allows_order = MagicMock()

    PMOrderIntent.session.add(
        PMOrderIntent(
            client_id="ftdead",
            kind="order",
            pair="ETH/USDT:USDT",
            reduce_only=False,
            state="UNKNOWN",
        )
    )
    PMOrderIntent.session.commit()

    # Entry (initial) blocked
    with pytest.raises(TemporaryError):
        exchange.create_order(
            pair="ETH/USDT:USDT",
            ordertype="limit",
            side="buy",
            amount=1.0,
            rate=100,
            leverage=1,
            reduceOnly=False,
            entry_mode="initial",
        )
    # DCA blocked
    with pytest.raises(TemporaryError):
        exchange.create_order(
            pair="ETH/USDT:USDT",
            ordertype="limit",
            side="buy",
            amount=1.0,
            rate=100,
            leverage=1,
            reduceOnly=False,
            entry_mode="pos_adjust",
        )
    # Stoploss blocked (no second strategy id may be generated)
    with pytest.raises(TemporaryError):
        exchange.create_stoploss(
            pair="ETH/USDT:USDT",
            amount=1,
            stop_price=95,
            side="sell",
            leverage=5,
            order_types={"stoploss": "market"},
        )
    # Nothing was ever POSTed
    assert exchange._papi_request.call_count == 0

    # reduceOnly exit order is NOT blocked by the intent gate
    exchange.create_order(
        pair="ETH/USDT:USDT",
        ordertype="market",
        side="sell",
        amount=1.0,
        rate=100,
        leverage=1,
        reduceOnly=True,
    )
    assert exchange._papi_request.call_count == 1


def test_normal_order_timeout_no_second_client_id_or_post(default_conf_usdt, mocker):
    """
    P0-2: after a normal-order POST timeout with a failed idempotency lookup,
    the next strategy trigger (entry/DCA) must not generate a second client id
    and must not POST again.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=TemporaryError("submit timeout"))
    exchange._pm_fetch_order_by_client_id = MagicMock(side_effect=TemporaryError("lookup down"))
    exchange._lev_prep = MagicMock()
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=100)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)
    exchange.assert_pm_risk_allows_order = MagicMock()
    new_id_spy = mocker.spy(exchange, "_pm_new_client_order_id")

    # First trigger: POST times out, lookup fails -> UNKNOWN intent, TemporaryError.
    with pytest.raises(TemporaryError):
        exchange.create_order(
            pair="ETH/USDT:USDT",
            ordertype="limit",
            side="buy",
            amount=1.0,
            rate=100,
            leverage=1,
            reduceOnly=False,
            entry_mode="initial",
        )
    assert exchange._papi_request.call_count == 1
    assert new_id_spy.call_count == 1
    assert exchange.list_pm_pending_intents()[0]["state"] == "UNKNOWN"

    # Second trigger (strategy) and third (DCA): blocked by the durable gate.
    for mode in ("initial", "pos_adjust"):
        with pytest.raises(TemporaryError):
            exchange.create_order(
                pair="ETH/USDT:USDT",
                ordertype="limit",
                side="buy",
                amount=1.0,
                rate=100,
                leverage=1,
                reduceOnly=False,
                entry_mode=mode,
            )
    assert exchange._papi_request.call_count == 1
    assert new_id_spy.call_count == 1


def test_normal_order_timeout_same_id_lookup_returns_existing(default_conf_usdt, mocker):
    """P0-2: when the same-client-id lookup succeeds, the existing order is returned."""
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=TemporaryError("submit timeout"))
    exchange._pm_fetch_order_by_client_id = MagicMock(
        return_value={
            "id": "777",
            "symbol": "ETH/USDT:USDT",
            "status": "open",
            "side": "buy",
            "filled": 0.0,
        }
    )
    exchange._lev_prep = MagicMock()
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=100)
    exchange.assert_pm_risk_allows_order = MagicMock()

    order = exchange.create_order(
        pair="ETH/USDT:USDT",
        ordertype="limit",
        side="buy",
        amount=1.0,
        rate=100,
        leverage=1,
        reduceOnly=False,
    )

    assert order["id"] == "777"
    # New contract: the intent stays ACKED with the exchange id as evidence until
    # the local Trade/Order commit links it (nothing is deleted on the ACK).
    intents = exchange.list_pm_pending_intents()
    assert len(intents) == 1
    assert intents[0]["state"] == "ACKED"
    assert intents[0]["exchange_order_id"] == "777"
    # Exactly one POST; no duplicate was ever submitted.
    assert exchange._papi_request.call_count == 1


def test_conditional_timeout_no_second_strategy_id_or_post(default_conf_usdt, mocker):
    """
    P0-2: after a conditional POST timeout with a failed same-strategy-id lookup,
    the next stoploss management attempt must query the SAME strategy id and must
    never generate a second strategy id / POST.
    """
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=ccxt.NetworkError("timeout"))
    exchange._lev_prep = MagicMock()
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=95)
    exchange.fetch_stoploss_order = MagicMock(side_effect=TemporaryError("lookup down"))
    new_strategy_id_spy = mocker.spy(exchange, "_pm_new_client_strategy_id")

    with pytest.raises(TemporaryError):
        exchange.create_stoploss(
            pair="ETH/USDT:USDT",
            amount=1,
            stop_price=95,
            side="sell",
            leverage=5,
            order_types={"stoploss": "market"},
        )
    assert exchange._papi_request.call_count == 1
    assert new_strategy_id_spy.call_count == 1
    assert exchange.list_pm_pending_intents()[0]["state"] == "UNKNOWN"

    # Second management attempt: blocked by the durable gate; same-id query only.
    with pytest.raises(TemporaryError):
        exchange.create_stoploss(
            pair="ETH/USDT:USDT",
            amount=1,
            stop_price=95,
            side="sell",
            leverage=5,
            order_types={"stoploss": "market"},
        )
    assert exchange._papi_request.call_count == 1
    assert new_strategy_id_spy.call_count == 1


def test_conditional_timeout_same_id_lookup_returns_existing(default_conf_usdt, mocker):
    """P0-2: same-strategy-id lookup success returns the existing stoploss order."""
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(side_effect=ccxt.NetworkError("timeout"))
    exchange._lev_prep = MagicMock()
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=95)
    exchange.fetch_stoploss_order = MagicMock(
        return_value={
            "id": "stabc",
            "symbol": "ETH/USDT:USDT",
            "status": "open",
            "side": "sell",
            "stopPrice": 95.0,
            "filled": 0.0,
            "info": {},
        }
    )

    order = exchange.create_stoploss(
        pair="ETH/USDT:USDT",
        amount=1,
        stop_price=95,
        side="sell",
        leverage=5,
        order_types={"stoploss": "market"},
    )

    assert order["id"] == "stabc"
    # New contract: ACKED with evidence, not deleted.
    intents = exchange.list_pm_pending_intents()
    assert len(intents) == 1
    assert intents[0]["state"] == "ACKED"
    assert intents[0]["exchange_order_id"] == "stabc"
    assert exchange._papi_request.call_count == 1
