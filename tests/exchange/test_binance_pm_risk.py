import asyncio
from threading import RLock
from unittest.mock import MagicMock

import pytest

from freqtrade.exceptions import OperationalException
from freqtrade.exchange.binance import Binance


PAIR = "BTC/USDT:USDT"


def set_minimal_exchange_cleanup_attrs(exchange):
    exchange._exchange_ws = None
    exchange._api_async = None
    exchange._ws_async = None
    exchange.loop = None


def make_pm_exchange(risk_config):
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {
        "dry_run": False,
        "exchange": {"portfolio_margin_risk": risk_config},
    }
    exchange.fetch_pm_account_information = MagicMock(
        return_value={
            "accountStatus": "NORMAL",
            "uniMMR": "10",
            "accountEquity": "1000",
            "accountInitialMargin": "50",
            "accountMaintMargin": "5",
        }
    )
    exchange.fetch_positions = MagicMock(
        return_value=[{"symbol": PAIR, "initialMargin": 5.0, "leverage": 10.0}]
    )
    exchange.get_rate = MagicMock(return_value=60000)
    return exchange


def test_pm_replace_entry_does_not_double_count_notional_caps():
    exchange = make_pm_exchange({"max_total_notional": 100})

    exchange.assert_pm_risk_allows_order(pair=PAIR, amount=0.001, leverage=5, entry_mode="replace")

    exchange.fetch_positions.assert_not_called()
    exchange.get_rate.assert_not_called()


def test_pm_position_adjustment_counts_incremental_notional():
    exchange = make_pm_exchange({"max_total_notional": 100})

    with pytest.raises(OperationalException, match="entry_mode=pos_adjust"):
        exchange.assert_pm_risk_allows_order(
            pair=PAIR, amount=0.001, leverage=5, entry_mode="pos_adjust"
        )


def test_pm_create_order_passes_entry_mode_to_risk_check():
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {"dry_run": False, "exchange": {"portfolio_margin_risk": {}}}
    exchange._get_params = MagicMock(return_value={})
    exchange.amount_to_precision = MagicMock(side_effect=lambda pair, amount: amount)
    exchange._amount_to_contracts = MagicMock(side_effect=lambda pair, amount: amount)
    exchange._order_needs_price = MagicMock(return_value=False)
    exchange.assert_pm_risk_allows_order = MagicMock()
    exchange._lev_prep = MagicMock()
    exchange._pm_namespace_for_pair = MagicMock(return_value="um")
    exchange._pm_order_params = MagicMock(return_value={"symbol": "BTCUSDT"})
    exchange._papi_request = MagicMock(return_value={"orderId": 1, "status": "NEW"})
    exchange._log_exchange_response = MagicMock()
    exchange._parse_pm_order = MagicMock(return_value={"id": "1", "status": "open"})
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda order: order)

    exchange.create_order(
        pair=PAIR,
        ordertype="market",
        side="buy",
        amount=0.001,
        rate=60000,
        leverage=5,
        entry_mode="pos_adjust",
    )

    exchange.assert_pm_risk_allows_order.assert_called_once_with(
        pair=PAIR, amount=0.001, leverage=5, entry_mode="pos_adjust"
    )


def test_pm_risk_config_rejects_unknown_keys():
    exchange = Binance.__new__(Binance)
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)

    with pytest.raises(OperationalException, match="Unknown Binance PM risk config key"):
        exchange._validate_pm_risk_config(
            {"exchange": {"portfolio_margin_risk": {"min_unimmr": 1.5}}}
        )


class _MarketsApi:
    def __init__(self) -> None:
        self.has = {"fetchCurrencies": True}
        self.session = None
        self.fetch_currencies_values: list[bool] = []

    async def load_markets(self, reload=False, params=None):
        self.fetch_currencies_values.append(self.has["fetchCurrencies"])
        return {}

    async def close(self):
        return None


def test_pm_reload_markets_skips_ccxt_fetch_currencies():
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._api_async = _MarketsApi()

    asyncio.run(exchange._api_reload_markets(reload=True))

    assert exchange._api_async.fetch_currencies_values == [False]
    assert exchange._api_async.has["fetchCurrencies"] is True


def test_non_pm_reload_markets_keeps_ccxt_fetch_currencies():
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = False
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._api_async = _MarketsApi()

    asyncio.run(exchange._api_reload_markets(reload=True))

    assert exchange._api_async.fetch_currencies_values == [True]
    assert exchange._api_async.has["fetchCurrencies"] is True
