from datetime import UTC, datetime
from random import randint
from unittest.mock import MagicMock

import ccxt
import pytest

from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exceptions import DependencyException, InvalidOrderException
from tests.conftest import EXMS, get_patched_exchange
from tests.exchange.test_exchange import ccxt_exceptionhandlers


@pytest.mark.parametrize(
    "limitratio,expected,side",
    [
        (None, 220 * 0.99, "sell"),
        (0.99, 220 * 0.99, "sell"),
        (0.98, 220 * 0.98, "sell"),
    ],
)
def test_create_stoploss_order_htx(default_conf, mocker, limitratio, expected, side):
    api_mock = MagicMock()
    order_id = f"test_prod_buy_{randint(0, 10**6)}"
    order_type = "stop-limit"

    api_mock.create_order = MagicMock(return_value={"id": order_id, "info": {"foo": "bar"}})
    default_conf["dry_run"] = False
    mocker.patch(f"{EXMS}.amount_to_precision", lambda s, x, y: y)
    mocker.patch(f"{EXMS}.price_to_precision", lambda s, x, y, **kwargs: y)

    exchange = get_patched_exchange(mocker, default_conf, api_mock, "htx")

    with pytest.raises(InvalidOrderException):
        order = exchange.create_stoploss(
            pair="ETH/BTC",
            amount=1,
            stop_price=190,
            order_types={"stoploss_on_exchange_limit_ratio": 1.05},
            side=side,
            leverage=1.0,
        )

    api_mock.create_order.reset_mock()
    order_types = {} if limitratio is None else {"stoploss_on_exchange_limit_ratio": limitratio}
    order = exchange.create_stoploss(
        pair="ETH/BTC", amount=1, stop_price=220, order_types=order_types, side=side, leverage=1.0
    )

    assert "id" in order
    assert "info" in order
    assert order["id"] == order_id
    assert api_mock.create_order.call_args_list[0][1]["symbol"] == "ETH/BTC"
    assert api_mock.create_order.call_args_list[0][1]["type"] == order_type
    assert api_mock.create_order.call_args_list[0][1]["side"] == "sell"
    assert api_mock.create_order.call_args_list[0][1]["amount"] == 1
    # Price should be 1% below stopprice
    assert api_mock.create_order.call_args_list[0][1]["price"] == expected
    assert api_mock.create_order.call_args_list[0][1]["params"] == {
        "stopPrice": 220,
        "operator": "lte",
    }

    # test exception handling
    with pytest.raises(DependencyException):
        api_mock.create_order = MagicMock(side_effect=ccxt.InsufficientFunds("0 balance"))
        exchange = get_patched_exchange(mocker, default_conf, api_mock, "htx")
        exchange.create_stoploss(
            pair="ETH/BTC", amount=1, stop_price=220, order_types={}, side=side, leverage=1.0
        )

    with pytest.raises(InvalidOrderException):
        api_mock.create_order = MagicMock(
            side_effect=ccxt.InvalidOrder("binance Order would trigger immediately.")
        )
        exchange = get_patched_exchange(mocker, default_conf, api_mock, "binance")
        exchange.create_stoploss(
            pair="ETH/BTC", amount=1, stop_price=220, order_types={}, side=side, leverage=1.0
        )

    ccxt_exceptionhandlers(
        mocker,
        default_conf,
        api_mock,
        "htx",
        "create_stoploss",
        "create_order",
        retries=1,
        pair="ETH/BTC",
        amount=1,
        stop_price=220,
        order_types={},
        side=side,
        leverage=1.0,
    )


def test_create_stoploss_order_dry_run_htx(default_conf, mocker):
    api_mock = MagicMock()
    order_type = "stop-limit"
    default_conf["dry_run"] = True
    mocker.patch(f"{EXMS}.amount_to_precision", lambda s, x, y: y)
    mocker.patch(f"{EXMS}.price_to_precision", lambda s, x, y, **kwargs: y)

    exchange = get_patched_exchange(mocker, default_conf, api_mock, "htx")

    with pytest.raises(InvalidOrderException):
        order = exchange.create_stoploss(
            pair="ETH/BTC",
            amount=1,
            stop_price=190,
            order_types={"stoploss_on_exchange_limit_ratio": 1.05},
            side="sell",
            leverage=1.0,
        )

    api_mock.create_order.reset_mock()

    order = exchange.create_stoploss(
        pair="ETH/BTC", amount=1, stop_price=220, order_types={}, side="sell", leverage=1.0
    )

    assert "id" in order
    assert "info" in order
    assert "type" in order

    assert order["type"] == order_type
    assert order["price"] == 217.8
    assert order["stopPrice"] == 220
    assert order["amount"] == 1


def test_stoploss_adjust_htx(mocker, default_conf):
    exchange = get_patched_exchange(mocker, default_conf, exchange="htx")
    order = {
        "type": "stop",
        "price": 1500,
        "stopPrice": "1500",
    }
    assert exchange.stoploss_adjust(1501, order, "sell")
    assert not exchange.stoploss_adjust(1499, order, "sell")
    # Test with invalid order case
    assert exchange.stoploss_adjust(1501, order, "sell")


def test_htx_futures_get_params(default_conf, mocker):
    default_conf["trading_mode"] = TradingMode.FUTURES
    default_conf["margin_mode"] = MarginMode.ISOLATED

    exchange = get_patched_exchange(mocker, default_conf, exchange="htx")

    assert exchange._get_params(
        side="buy",
        ordertype="limit",
        leverage=3.0,
        reduceOnly=False,
    ) == {"marginMode": "isolated"}
    assert exchange._get_params(
        side="sell",
        ordertype="limit",
        leverage=3.0,
        reduceOnly=True,
    ) == {"reduceOnly": True, "marginMode": "isolated"}


def test_htx_futures_additional_exchange_init(default_conf_usdt, mocker):
    api_mock = MagicMock()
    api_mock.has = {"setPositionMode": True}
    api_mock.set_position_mode = MagicMock(return_value={})

    default_conf_usdt["dry_run"] = False
    default_conf_usdt["trading_mode"] = TradingMode.FUTURES
    default_conf_usdt["margin_mode"] = MarginMode.ISOLATED
    default_conf_usdt["exchange"]["pair_whitelist"] = ["ETH/USDT:USDT", "ETH/USDT"]

    get_patched_exchange(mocker, default_conf_usdt, api_mock, exchange="htx")

    api_mock.set_position_mode.assert_called_once_with(
        False, symbol="ETH/USDT:USDT", params={"marginMode": "isolated"}
    )


def test_htx_futures_lev_prep(default_conf_usdt, mocker):
    api_mock = MagicMock()
    api_mock.has = {
        "setLeverage": True,
        "setMarginMode": False,
        "setPositionMode": True,
    }
    api_mock.set_leverage = MagicMock(return_value={})
    api_mock.set_position_mode = MagicMock(return_value={})

    default_conf_usdt["dry_run"] = False
    default_conf_usdt["trading_mode"] = TradingMode.FUTURES
    default_conf_usdt["margin_mode"] = MarginMode.ISOLATED

    exchange = get_patched_exchange(mocker, default_conf_usdt, api_mock, exchange="htx")
    exchange._lev_prep("ETH/USDT:USDT", 3.7, "buy")

    api_mock.set_position_mode.assert_called_once_with(
        False, symbol="ETH/USDT:USDT", params={"marginMode": "isolated"}
    )
    api_mock.set_leverage.assert_called_once_with(
        symbol="ETH/USDT:USDT", leverage=3, params={"marginMode": "isolated"}
    )


def test_htx_futures_set_margin_mode(default_conf_usdt, mocker):
    api_mock = MagicMock()
    api_mock.has = {"setMarginMode": True}
    api_mock.set_margin_mode = MagicMock(return_value={})

    default_conf_usdt["dry_run"] = False
    default_conf_usdt["trading_mode"] = TradingMode.FUTURES
    default_conf_usdt["margin_mode"] = MarginMode.ISOLATED

    exchange = get_patched_exchange(mocker, default_conf_usdt, api_mock, exchange="htx")
    exchange.set_margin_mode("ETH/USDT:USDT", MarginMode.ISOLATED)

    api_mock.set_margin_mode.assert_called_once_with(
        "isolated", "ETH/USDT:USDT", {"marginMode": "isolated"}
    )


@pytest.mark.parametrize(
    "order_types,expected_type,expected_price",
    [
        ({"stoploss": "market"}, "market", None),
        (
            {"stoploss": "limit", "stoploss_on_exchange_limit_ratio": 0.99},
            "limit",
            188.1,
        ),
    ],
)
def test_create_stoploss_order_htx_futures(
    default_conf_usdt, mocker, order_types, expected_type, expected_price
):
    api_mock = MagicMock()
    api_mock.has = {}
    api_mock.create_order = MagicMock(
        return_value={"id": "sl123", "symbol": "ETH/USDT:USDT", "amount": 1, "info": {}}
    )
    mocker.patch(f"{EXMS}.amount_to_precision", lambda s, x, y: y)
    mocker.patch(f"{EXMS}.price_to_precision", lambda s, x, y, **kwargs: y)

    default_conf_usdt["dry_run"] = False
    default_conf_usdt["trading_mode"] = TradingMode.FUTURES
    default_conf_usdt["margin_mode"] = MarginMode.ISOLATED

    exchange = get_patched_exchange(mocker, default_conf_usdt, api_mock, exchange="htx")
    order = exchange.create_stoploss(
        pair="ETH/USDT:USDT",
        amount=10,
        stop_price=190,
        order_types=order_types,
        side="sell",
        leverage=3.0,
    )

    assert order["id"] == "sl123"
    assert api_mock.create_order.call_args_list[0][1] == {
        "symbol": "ETH/USDT:USDT",
        "type": expected_type,
        "side": "sell",
        "amount": 1.0,
        "price": expected_price,
        "params": {
            "stopLossPrice": 190,
            "marginMode": "isolated",
            "reduceOnly": True,
        },
    }


def test_fetch_stoploss_order_htx_futures(default_conf_usdt, mocker):
    api_mock = MagicMock()
    api_mock.has = {}
    api_mock.fetch_open_orders = MagicMock(
        return_value=[
            {"id": "other", "symbol": "ETH/USDT:USDT"},
            {
                "id": "sl123",
                "symbol": "ETH/USDT:USDT",
                "triggerPrice": 190,
                "amount": 1,
                "status": "open",
            },
        ]
    )
    api_mock.fetch_orders = MagicMock(return_value=[])

    default_conf_usdt["dry_run"] = False
    default_conf_usdt["trading_mode"] = TradingMode.FUTURES
    default_conf_usdt["margin_mode"] = MarginMode.ISOLATED

    exchange = get_patched_exchange(mocker, default_conf_usdt, api_mock, exchange="htx")
    order = exchange.fetch_stoploss_order("sl123", "ETH/USDT:USDT")

    assert order["type"] == "stoploss"
    assert order["triggerPrice"] == 190
    api_mock.fetch_open_orders.assert_called_once_with(
        "ETH/USDT:USDT",
        params={"marginMode": "isolated", "stopLossTakeProfit": True},
    )
    api_mock.fetch_orders.assert_not_called()


def test_fetch_stoploss_order_htx_futures_history_canceled(default_conf_usdt, mocker):
    api_mock = MagicMock()
    api_mock.has = {}
    api_mock.fetch_open_orders = MagicMock(return_value=[])
    api_mock.fetch_orders = MagicMock(
        return_value=[
            {
                "id": "sl123",
                "symbol": "ETH/USDT:USDT",
                "triggerPrice": 190,
                "amount": 1,
                "status": "closed",
                "info": {
                    "status": 6,
                    "tpsl_order_type": "sl",
                    "relation_order_id": "-1",
                },
            },
        ]
    )

    default_conf_usdt["dry_run"] = False
    default_conf_usdt["trading_mode"] = TradingMode.FUTURES
    default_conf_usdt["margin_mode"] = MarginMode.ISOLATED

    exchange = get_patched_exchange(mocker, default_conf_usdt, api_mock, exchange="htx")
    order = exchange.fetch_stoploss_order("sl123", "ETH/USDT:USDT")

    assert order["type"] == "stoploss"
    assert order["status"] == "canceled"
    api_mock.fetch_orders.assert_called_once_with(
        "ETH/USDT:USDT",
        params={"marginMode": "isolated", "stopLossTakeProfit": True},
    )


def test_fetch_stoploss_order_htx_futures_history_triggered(default_conf_usdt, mocker):
    api_mock = MagicMock()
    api_mock.has = {"fetchOrder": True}
    api_mock.fetch_open_orders = MagicMock(return_value=[])
    api_mock.fetch_orders = MagicMock(
        return_value=[
            {
                "id": "sl123",
                "symbol": "ETH/USDT:USDT",
                "triggerPrice": 190,
                "amount": 1,
                "status": "open",
                "info": {
                    "status": 4,
                    "tpsl_order_type": "sl",
                    "relation_order_id": "normal123",
                },
            },
        ]
    )
    api_mock.fetch_order = MagicMock(
        return_value={
            "id": "normal123",
            "symbol": "ETH/USDT:USDT",
            "amount": 1,
            "status": "closed",
        }
    )

    default_conf_usdt["dry_run"] = False
    default_conf_usdt["trading_mode"] = TradingMode.FUTURES
    default_conf_usdt["margin_mode"] = MarginMode.ISOLATED

    exchange = get_patched_exchange(mocker, default_conf_usdt, api_mock, exchange="htx")
    order = exchange.fetch_stoploss_order("sl123", "ETH/USDT:USDT")

    assert order["id"] == "sl123"
    assert order["id_stop"] == "normal123"
    assert order["type"] == "stoploss"
    assert order["status"] == "closed"
    assert order["status_stop"] == "triggered"
    assert order["triggerPrice"] == 190
    api_mock.fetch_order.assert_called_once_with(
        "normal123", "ETH/USDT:USDT", params={"marginMode": "isolated"}
    )


def test_cancel_stoploss_order_htx_futures(default_conf_usdt, mocker):
    api_mock = MagicMock()
    api_mock.has = {}
    api_mock.cancel_order = MagicMock(return_value={"id": "sl123"})

    default_conf_usdt["dry_run"] = False
    default_conf_usdt["trading_mode"] = TradingMode.FUTURES
    default_conf_usdt["margin_mode"] = MarginMode.ISOLATED

    exchange = get_patched_exchange(mocker, default_conf_usdt, api_mock, exchange="htx")
    exchange.cancel_stoploss_order("sl123", "ETH/USDT:USDT")

    api_mock.cancel_order.assert_called_once_with(
        "sl123",
        "ETH/USDT:USDT",
        params={"marginMode": "isolated", "stopLossTakeProfit": True},
    )


def test_htx_futures_private_calls_use_isolated_margin(default_conf_usdt, mocker):
    api_mock = MagicMock()
    api_mock.has = {
        "fetchFundingHistory": True,
        "fetchMyTrades": True,
        "fetchOrder": True,
        "fetchOrders": True,
    }
    api_mock.fetch_balance = MagicMock(return_value={"USDT": {"free": 1.0}, "info": {}})
    api_mock.fetch_order = MagicMock(return_value={"id": "123"})
    api_mock.cancel_order = MagicMock(return_value={"id": "123"})
    api_mock.fetch_positions = MagicMock(return_value=[])
    api_mock.fetch_orders = MagicMock(return_value=[])
    api_mock.fetch_my_trades = MagicMock(return_value=[])
    api_mock.fetch_funding_history = MagicMock(return_value=[{"amount": 0.1}, {"amount": -0.02}])

    default_conf_usdt["dry_run"] = False
    default_conf_usdt["trading_mode"] = TradingMode.FUTURES
    default_conf_usdt["margin_mode"] = MarginMode.ISOLATED

    exchange = get_patched_exchange(mocker, default_conf_usdt, api_mock, exchange="htx")
    since = datetime(2024, 1, 1, tzinfo=UTC)

    assert exchange.get_balances() == {"USDT": {"free": 1.0}}
    exchange.fetch_order("123", "ETH/USDT:USDT")
    exchange.cancel_order("123", "ETH/USDT:USDT")
    exchange.fetch_positions("ETH/USDT:USDT")
    exchange._fetch_orders("ETH/USDT:USDT", since)
    exchange.get_trades_for_order("123", "ETH/USDT:USDT", since)
    assert exchange._get_funding_fees_from_exchange("ETH/USDT:USDT", since) == 0.08

    margin_params = {"marginMode": "isolated"}
    api_mock.fetch_balance.assert_called_once_with(margin_params)
    api_mock.fetch_order.assert_called_once_with("123", "ETH/USDT:USDT", params=margin_params)
    api_mock.cancel_order.assert_called_once_with("123", "ETH/USDT:USDT", params=margin_params)
    api_mock.fetch_positions.assert_called_once_with(["ETH/USDT:USDT"], params=margin_params)
    api_mock.fetch_orders.assert_called_once_with(
        "ETH/USDT:USDT", since=int((since.timestamp() - 10) * 1000), params=margin_params
    )
    api_mock.fetch_my_trades.assert_called_once_with(
        "ETH/USDT:USDT",
        int((since.replace(tzinfo=UTC).timestamp() - 5) * 1000),
        params=margin_params,
    )
    api_mock.fetch_funding_history.assert_called_once_with(
        symbol="ETH/USDT:USDT", since=int(since.timestamp() * 1000), params=margin_params
    )
