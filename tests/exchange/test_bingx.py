from unittest.mock import MagicMock, PropertyMock

from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exchange.bingx import Bingx
from tests.conftest import EXMS, get_patched_exchange


def test_bingx_supported_trading_modes():
    assert (TradingMode.SPOT, MarginMode.NONE) in Bingx._supported_trading_mode_margin_pairs
    assert (TradingMode.FUTURES, MarginMode.CROSS) in Bingx._supported_trading_mode_margin_pairs
    assert (TradingMode.FUTURES, MarginMode.ISOLATED) in Bingx._supported_trading_mode_margin_pairs


def test_additional_exchange_init_bingx(default_conf, mocker):
    default_conf["dry_run"] = False
    default_conf["trading_mode"] = TradingMode.FUTURES
    default_conf["margin_mode"] = MarginMode.ISOLATED
    api_mock = MagicMock()
    api_mock.set_position_mode = MagicMock(return_value={"dualSidePosition": False})

    get_patched_exchange(mocker, default_conf, exchange="bingx", api_mock=api_mock)
    api_mock.set_position_mode.assert_called_once_with(False)


def test__lev_prep_bingx(default_conf, mocker):
    api_mock = MagicMock()
    api_mock.set_margin_mode = MagicMock()
    api_mock.set_leverage = MagicMock()
    type(api_mock).has = PropertyMock(return_value={"setMarginMode": True, "setLeverage": True})

    exchange = get_patched_exchange(mocker, default_conf, api_mock, exchange="bingx")
    exchange._lev_prep("BTC/USDT:USDT", 3.2, "buy")

    assert api_mock.set_margin_mode.call_count == 0
    assert api_mock.set_leverage.call_count == 0

    api_mock.reset_mock()
    default_conf["dry_run"] = False
    default_conf["trading_mode"] = TradingMode.FUTURES
    default_conf["margin_mode"] = MarginMode.ISOLATED

    exchange = get_patched_exchange(mocker, default_conf, api_mock, exchange="bingx")
    exchange._lev_prep("BTC/USDT:USDT", 3.2, "buy")

    api_mock.set_margin_mode.assert_called_once_with("isolated", "BTC/USDT:USDT", {})
    api_mock.set_leverage.assert_called_once_with(
        leverage=3.2,
        symbol="BTC/USDT:USDT",
        params={"side": "BOTH"},
    )


def test_create_stoploss_order_bingx_futures(default_conf, mocker):
    api_mock = MagicMock()
    api_mock.create_order = MagicMock(return_value={"id": "42", "info": {"foo": "bar"}})
    type(api_mock).has = PropertyMock(return_value={"setMarginMode": True, "setLeverage": True})

    default_conf["dry_run"] = False
    default_conf["trading_mode"] = TradingMode.FUTURES
    default_conf["margin_mode"] = MarginMode.ISOLATED

    mocker.patch(f"{EXMS}.amount_to_precision", lambda s, x, y: y)
    mocker.patch(f"{EXMS}.price_to_precision", lambda s, x, y, **kwargs: y)

    exchange = get_patched_exchange(mocker, default_conf, api_mock, "bingx")

    order = exchange.create_stoploss(
        pair="ETH/USDT:USDT",
        amount=1,
        stop_price=220,
        order_types={"stoploss": "market", "stoploss_price_type": "mark"},
        side="sell",
        leverage=2.0,
    )

    assert order["id"] == "42"
    api_mock.create_order.assert_called_once_with(
        symbol="ETH/USDT:USDT",
        type="stop_market",
        side="sell",
        amount=1,
        price=None,
        params={
            "stopLossPrice": 220,
            "reduceOnly": True,
            "workingType": "MARK_PRICE",
        },
    )
