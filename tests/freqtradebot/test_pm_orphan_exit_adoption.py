"""Strict regression tests for PM ACK->commit orphan exit adoption."""

from copy import deepcopy

import pytest

from freqtrade.persistence import PMOrderIntent, Trade
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


def _acked_exit_intent(trade, *, client_id="ft-orphan-1", order_id="exit-orphan-1"):
    row = PMOrderIntent(
        client_id=client_id,
        kind="order",
        pair=trade.pair,
        side=trade.exit_side,
        order_type="market",
        amount=trade.amount,
        reduce_only=True,
        origin_trade_id=trade.id,
        state="ACKED",
        exchange_order_id=order_id,
    )
    PMOrderIntent.session.add(row)
    PMOrderIntent.session.commit()
    return row


def _exchange_exit(trade, *, client_id="ft-orphan-1", order_id="exit-orphan-1"):
    order = ccxt_order(order_id, "closed", trade.exit_side, amount=trade.amount, filled=trade.amount)
    order["clientOrderId"] = client_id
    order["symbol"] = trade.pair
    order["info"] = {"reduceOnly": True}
    return order


def test_adopts_only_with_durable_origin_trade_and_complete_exchange_identity(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = _closed_entry_trade(pm_conf)
    intent = _acked_exit_intent(trade)
    order = _exchange_exit(trade)

    adopted = bot._pm_adopt_orphan_reduce_only_order(intent.to_dict(), order)

    assert adopted is not None
    adopted_trade, adopted_order = adopted
    assert adopted_trade.id == trade.id
    assert adopted_order.order_id == "exit-orphan-1"
    assert adopted_order.ft_order_side == trade.exit_side


@pytest.mark.parametrize(
    "missing_field",
    ["id", "symbol", "clientOrderId", "side", "reduceOnly", "amount"],
)
def test_refuses_when_any_required_exchange_identity_field_is_missing(
    mocker, pm_conf, missing_field
):
    bot = make_pm_bot(mocker, pm_conf)
    trade = _closed_entry_trade(pm_conf)
    intent = _acked_exit_intent(trade)
    order = deepcopy(_exchange_exit(trade))

    if missing_field == "reduceOnly":
        order.pop("reduceOnly", None)
        order["info"].pop("reduceOnly", None)
    else:
        order.pop(missing_field, None)

    assert bot._pm_adopt_orphan_reduce_only_order(intent.to_dict(), order) is None
    assert all(o.order_id != "exit-orphan-1" for o in trade.orders)


def test_refuses_wrong_durable_origin_trade_id_even_if_pair_side_and_size_match(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = _closed_entry_trade(pm_conf)
    intent = _acked_exit_intent(trade)
    intent.origin_trade_id = trade.id + 999
    PMOrderIntent.session.commit()

    assert bot._pm_adopt_orphan_reduce_only_order(intent.to_dict(), _exchange_exit(trade)) is None


def test_refuses_when_persisted_ack_id_disagrees_with_exchange(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = _closed_entry_trade(pm_conf)
    intent = _acked_exit_intent(trade, order_id="expected-order")
    order = _exchange_exit(trade, order_id="different-order")

    assert bot._pm_adopt_orphan_reduce_only_order(intent.to_dict(), order) is None
