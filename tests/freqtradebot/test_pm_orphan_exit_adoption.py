"""Regression tests for PM ACK->local-commit orphaned reduce-only exit adoption."""

import pytest

from freqtrade.persistence import Trade
from tests.freqtradebot.test_pm_recovery import ccxt_order, make_open_trade, make_pm_bot


@pytest.fixture
def pm_conf(default_conf_usdt):
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
    return conf


def _closed_entry_trade(pm_conf, amount=11.0):
    trade = make_open_trade(pm_conf, order_id="entry1", side="buy", amount=amount)
    entry = trade.orders[-1]
    entry.status = "closed"
    entry.ft_is_open = False
    entry.filled = amount
    entry.remaining = 0.0
    entry.average = trade.open_rate
    Trade.commit()
    return trade


def test_adopts_provably_owned_reduce_only_exit(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = _closed_entry_trade(pm_conf, amount=11.0)
    order = ccxt_order("exit-orphan-1", "closed", "sell", amount=11.0, filled=11.0)
    order["clientOrderId"] = "ft-orphan-1"
    order["info"] = {"reduceOnly": True}
    intent = {
        "client_id": "ft-orphan-1",
        "exchange_order_id": "exit-orphan-1",
        "kind": "order",
        "pair": trade.pair,
        "side": "sell",
        "amount": 11.0,
        "reduce_only": True,
    }

    adopted = bot._pm_adopt_orphan_reduce_only_order(intent, order)

    assert adopted is not None
    adopted_trade, adopted_order = adopted
    assert adopted_trade.id == trade.id
    assert adopted_order.order_id == "exit-orphan-1"
    assert adopted_order.ft_order_side == trade.exit_side
    assert adopted_order in trade.orders
    assert trade.exit_reason == "pm_recovered_exit"


def test_refuses_orphan_without_strict_reduce_only_identity(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = _closed_entry_trade(pm_conf, amount=11.0)
    order = ccxt_order("foreign-1", "closed", "sell", amount=11.0, filled=11.0)
    order["clientOrderId"] = "different-client"
    order["info"] = {"reduceOnly": True}
    intent = {
        "client_id": "ft-expected",
        "exchange_order_id": "foreign-1",
        "kind": "order",
        "pair": trade.pair,
        "side": "sell",
        "amount": 11.0,
        "reduce_only": True,
    }

    assert bot._pm_adopt_orphan_reduce_only_order(intent, order) is None
    assert all(o.order_id != "foreign-1" for o in trade.orders)
