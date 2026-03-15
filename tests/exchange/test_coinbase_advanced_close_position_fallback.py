import ccxt
import pytest

from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exceptions import OperationalException
from freqtrade.exchange.coinbase import Coinbase
from freqtrade.exchange.coinbase_advanced_compat import (
    build_coinbase_close_position_params,
    should_use_coinbase_close_position_fallback,
)


class _DummyApi:
    def __init__(self):
        self.options = {}
        self.calls = []

    def create_order(self, pair, ordertype, side, amount, rate, params):
        self.calls.append(
            {
                "pair": pair,
                "ordertype": ordertype,
                "side": side,
                "amount": amount,
                "rate": rate,
                "params": dict(params),
            }
        )
        if len(self.calls) == 1:
            raise ccxt.InvalidOrder("PREVIEW_REDUCE_ONLY_NOT_ALLOWED_ON_VENUE")
        return {"id": "fallback-ok", "symbol": pair, "status": None, "type": None, "amount": amount}


class _DummyCoinbase(Coinbase):
    @property
    def name(self):
        return "coinbase"

    @property
    def markets(self):
        return {"BTC/USDC:USDC": {"swap": True, "contract": True, "quote": "USDC", "settle": "USDC"}}

    def _order_needs_price(self, side, ordertype):
        return True

    def price_to_precision(self, pair, rate):
        return rate

    def amount_to_precision(self, pair, amount):
        return amount

    def _amount_to_contracts(self, pair, amount):
        return amount

    def _order_contracts_to_amount(self, order):
        return order

    def _log_exchange_response(self, log_name, response):
        return None

    def _get_params(self, side, ordertype, leverage, reduceOnly, time_in_force="GTC"):
        return {
            "reduceOnly": reduceOnly,
            "marginMode": "isolated",
            "timeInForce": time_in_force,
            "leverage": leverage,
        }

    def _lev_prep(self, pair, leverage, side, accept_fail=False):
        return None



def test_should_use_close_position_fallback_on_preview_error():
    assert should_use_coinbase_close_position_fallback("PREVIEW_REDUCE_ONLY_NOT_ALLOWED_ON_VENUE")
    assert should_use_coinbase_close_position_fallback("close position required by venue")



def test_build_close_position_params_contains_flag_and_normalized_side():
    params = build_coinbase_close_position_params(side="BUY")
    assert params["close_position"] is True
    assert params["side"] == "buy"



def test_create_order_retries_reduce_only_exit_with_close_position():
    inst = object.__new__(_DummyCoinbase)
    inst._config = {
        "dry_run": False,
        "stake_currency": "USDC",
        "exchange": {"portfolio": "portfolio-1"},
    }
    inst.trading_mode = TradingMode.FUTURES
    inst.margin_mode = MarginMode.ISOLATED
    inst._api = _DummyApi()

    order = inst.create_order(
        pair="BTC/USDC",
        ordertype="market",
        side="sell",
        amount=1.0,
        rate=100.0,
        leverage=2.0,
        reduceOnly=True,
        initial_order=False,
    )

    assert order["id"] == "fallback-ok"
    assert len(inst._api.calls) == 2
    assert inst._api.calls[0]["params"]["reduceOnly"] is True
    assert inst._api.calls[1]["params"]["close_position"] is True
    assert inst._api.calls[1]["params"]["side"] == "sell"
    assert inst._api.calls[1]["params"]["portfolio"] == "portfolio-1"



def test_create_order_requires_portfolio_for_futures():
    inst = object.__new__(_DummyCoinbase)
    inst._config = {"dry_run": False, "stake_currency": "USDC", "exchange": {}}
    inst.trading_mode = TradingMode.FUTURES
    inst.margin_mode = MarginMode.ISOLATED
    inst._api = _DummyApi()

    with pytest.raises(OperationalException):
        inst.create_order(
            pair="BTC/USDC",
            ordertype="market",
            side="sell",
            amount=1.0,
            rate=100.0,
            leverage=2.0,
            reduceOnly=True,
            initial_order=False,
        )
