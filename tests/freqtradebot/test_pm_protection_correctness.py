"""Protection Correctness: deterministic counterexamples from the pre-staging review.

Every test in this file reproduces a safety counterexample that the previous
protection replacement would have failed:

- a replacement that is rejected / missing status / closed / wrong instrument /
  wrong side / wrong positionSide / not reduce-only / undersized must NEVER
  retire the old protection;
- a closed replacement must have its fill processed BEFORE anything else;
- DCA / entry fills use the same safe replacement primitive;
- protection verification is independent of open entry/DCA/exit orders and
  compares COVERAGE QUANTITY, not bare order ids;
- the dual OLD+NEW window never cancels a replacement and never double-reduces.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, call

import pytest

from freqtrade.exceptions import InvalidOrderException, TemporaryError
from freqtrade.persistence import Order, Trade
from tests.freqtradebot.test_pm_order_ownership import event
from tests.freqtradebot.test_pm_recovery import ccxt_order, make_pm_bot, make_stoploss_trade
from tests.freqtradebot.test_pm_recovery import pm_conf as _pm_conf


pm_conf = _pm_conf


def conditional(
    order_id,
    pair="ETH/USDT:USDT",
    status="open",
    side="sell",
    amount=11.0,
    info=None,
):
    """A parsed PM conditional-order shape (as fetch_stoploss_order returns)."""
    info = dict(info or {})
    return {
        "id": order_id,
        "clientAlgoId": order_id,
        "symbol": pair,
        "type": "stoploss",
        "timeInForce": None,
        "side": side,
        "price": None,
        "average": None,
        "stopPrice": None,
        "amount": amount,
        "filled": 0.0,
        "remaining": amount,
        "cost": 0.0,
        "status": status,
        "fee": None,
        "trades": [],
        "info": info,
    }


def switch_bot(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stold", side="sell")
    trade.open_sl_orders[0].order_date = datetime.now(UTC) - timedelta(seconds=120)
    bot.exchange.create_stoploss = MagicMock(
        return_value=ccxt_order("stnew", "open", "sell", filled=0)
    )
    bot.exchange.cancel_stoploss_order_with_result = MagicMock()
    bot.rpc.send_msg = MagicMock()
    bot._pm_link_intent_for_order = MagicMock()
    return bot, trade


# ---------------------------------------------------------------------------
# P0-1/P0-2: invalid replacements never retire the old protection.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "check",
    [
        conditional("stnew", status="rejected"),
        conditional("stnew", status=None),
        conditional("stnew", status="canceled"),
        conditional("stnew", status="expired"),
        conditional("stnew", pair="BTC/USDT:USDT"),  # wrong instrument
        conditional("stnew", side="buy"),  # wrong side
        conditional("stnew", info={"positionSide": "SHORT", "reduceOnly": True}),
        conditional("stnew", info={"reduceOnly": False}),  # not reduce-only
        conditional("stnew", amount=1.0),  # undersized
        None,  # exchange returned nothing
    ],
)
def test_invalid_replacement_never_retires_old_protection(mocker, pm_conf, check):
    bot, trade = switch_bot(mocker, pm_conf)
    bot.exchange.fetch_stoploss_order = MagicMock(return_value=check)

    verdict = bot._pm_replace_stop_protection(trade, ["stold"], new_stop_price=0.009)

    assert verdict == "kept_old"
    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()
    assert {sl.order_id for sl in trade.open_sl_orders} == {"stold", "stnew"}
    assert "stop_protection_missing" in bot._pm_blocked_order_reasons()
    bot.rpc.send_msg.assert_called_once()  # one SAFE_HOLD alert


def test_wrong_order_id_rejected_as_valid_protection(mocker, pm_conf):
    bot, trade = switch_bot(mocker, pm_conf)
    check = conditional("stnew", info={"reduceOnly": True})
    check["id"] = "stother"
    bot.exchange.fetch_stoploss_order = MagicMock(return_value=check)

    verdict = bot._pm_replace_stop_protection(trade, ["stold"], new_stop_price=0.009)

    assert verdict == "kept_old"
    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()


def test_replacement_verify_fetch_failure_keeps_old_protection(mocker, pm_conf):
    bot, trade = switch_bot(mocker, pm_conf)
    bot.exchange.fetch_stoploss_order = MagicMock(side_effect=TemporaryError("network down"))

    verdict = bot._pm_replace_stop_protection(trade, ["stold"], new_stop_price=0.009)

    assert verdict == "kept_old"
    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()
    assert "stold" in {sl.order_id for sl in trade.open_sl_orders}


def test_closed_replacement_processes_fill_first_and_keeps_old(mocker, pm_conf):
    bot, trade = switch_bot(mocker, pm_conf)
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=conditional("stnew", status="closed", info={"reduceOnly": True})
    )
    bot.update_trade_state = MagicMock()

    verdict = bot._pm_replace_stop_protection(trade, ["stold"], new_stop_price=0.009)

    assert verdict == "terminal"
    # The fill was processed FIRST through the full lifecycle...
    bot.update_trade_state.assert_called_once()
    assert bot.update_trade_state.call_args.args[1] == "stnew"
    # ...and the old protection was NOT blindly canceled.
    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()
    assert {sl.order_id for sl in trade.open_sl_orders} == {"stold", "stnew"}
    assert "stop_protection_missing" not in bot._pm_blocked_order_reasons()


def test_valid_replacement_retires_only_the_old_id(mocker, pm_conf):
    bot, trade = switch_bot(mocker, pm_conf)
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=conditional("stnew", amount=11.0, info={"reduceOnly": True})
    )
    bot.exchange.cancel_stoploss_order_with_result = MagicMock(
        return_value=ccxt_order("stold", "canceled", "sell", filled=0)
    )

    def close_old(trade_, order_id, action_order, **kwargs):
        for sl in trade_.open_sl_orders:
            if str(sl.order_id) == str(order_id):
                sl.ft_is_open = False
                sl.status = "canceled"
        return True

    bot.update_trade_state = MagicMock(side_effect=close_old)

    verdict = bot._pm_replace_stop_protection(trade, ["stold"], new_stop_price=0.009)

    assert verdict == "active"
    bot.exchange.cancel_stoploss_order_with_result.assert_called_once_with(
        "stold", trade.pair, trade.amount
    )
    assert {sl.order_id for sl in trade.open_sl_orders} == {"stnew"}
    assert "stop_protection_missing" not in bot._pm_blocked_order_reasons()


# ---------------------------------------------------------------------------
# P0-3: the DCA / entry-fill path uses the same safe primitive.
# ---------------------------------------------------------------------------
def test_update_trade_after_fill_uses_safe_resize_for_pm(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    mocker.patch("freqtrade.freqtradebot.update_liquidation_prices")
    from tests.freqtradebot.test_pm_recovery import make_open_trade

    trade = make_open_trade(pm_conf, order_id="e1", side="buy", amount=11.0)
    entry = trade.orders[-1]
    entry.status = "closed"
    entry.ft_is_open = False
    trade.amount = 11.0
    Trade.commit()
    bot._pm_resize_stop_protection = MagicMock()
    bot.cancel_stoploss_on_exchange = MagicMock()

    bot._update_trade_after_fill(trade, entry, True)

    bot._pm_resize_stop_protection.assert_called_once_with(trade)
    bot.cancel_stoploss_on_exchange.assert_not_called()


def test_dca_fill_kill_between_stages_keeps_old_protection(mocker, pm_conf):
    """DCA fill -> candidate accepted -> kill before validation -> the OLD
    protection must still exist (it was never canceled)."""
    bot, trade = switch_bot(mocker, pm_conf)
    bot.exchange.fetch_stoploss_order = MagicMock(
        side_effect=TemporaryError("kill -9 between create and validate")
    )

    verdict = bot._pm_resize_stop_protection(trade)

    assert verdict == "kept_old"
    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()
    assert {sl.order_id for sl in trade.open_sl_orders} == {"stold", "stnew"}
    assert "stop_protection_missing" in bot._pm_blocked_order_reasons()


# ---------------------------------------------------------------------------
# P0-4/P0-5: strict verification, independent of open orders, quantity-aware.
# ---------------------------------------------------------------------------
def test_stop_verification_runs_despite_open_dca_order(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.strategy.order_types["stoploss_on_exchange"] = True
    trade = make_stoploss_trade(pm_conf, order_id="stX", side="sell", amount=11.0)
    # An open DCA entry order must NOT exempt the trade from protection checks.
    trade.orders.append(
        Order(
            ft_order_side="buy",
            ft_pair=trade.pair,
            ft_is_open=True,
            ft_amount=5.0,
            ft_price=0.01,
            order_id="dca1",
            status="open",
            symbol=trade.pair,
            order_type="limit",
            side="buy",
            price=0.01,
            average=0.01,
            filled=0.0,
            remaining=5.0,
            cost=0.0,
            order_date=trade.open_date,
        )
    )
    Trade.commit()
    bot.exchange.fetch_open_conditional_orders.return_value = []
    bot.exchange.fetch_stoploss_order = MagicMock(side_effect=InvalidOrderException("gone"))
    bot.exchange.fetch_order = MagicMock(side_effect=InvalidOrderException("gone"))

    report = bot._pm_reconcile_open_orders()

    assert report["unprotected_positions"]
    assert "stop_protection_missing" in bot._pm_blocked_order_reasons()


@pytest.mark.parametrize(
    "qty,offender",
    [(0, True), (1, True), (10, True), (99, True), (100, False), (101, False)],
)
def test_protection_quantity_coverage_matrix(mocker, pm_conf, qty, offender):
    bot = make_pm_bot(mocker, pm_conf)
    bot.strategy.order_types["stoploss_on_exchange"] = True
    trade = make_stoploss_trade(pm_conf, order_id="stX", side="sell", amount=100.0)
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=conditional("stX", pair=trade.pair, side="sell", amount=100.0)
    )
    bot.exchange.fetch_open_conditional_orders.return_value = [
        conditional(
            "stX",
            pair=trade.pair,
            side="sell",
            amount=float(qty),
            info={"reduceOnly": True},
        )
    ]

    report = bot._pm_reconcile_open_orders()

    assert bool(report["unprotected_positions"]) is offender


@pytest.mark.parametrize(
    "check",
    [
        conditional("stX", pair="BTC/USDT:USDT"),  # same id, wrong instrument
        conditional("stX", status="closed"),  # already fired
        conditional("stX", status="rejected"),
        conditional("stX", side="buy"),  # wrong side
        conditional("stX", info={"reduceOnly": False}),  # non-reduce
        conditional("stX", amount=10.0),  # insufficient coverage
    ],
)
def test_stop_verification_rejects_fake_protection_fixtures(mocker, pm_conf, check):
    bot = make_pm_bot(mocker, pm_conf)
    bot.strategy.order_types["stoploss_on_exchange"] = True
    trade = make_stoploss_trade(pm_conf, order_id="stX", side="sell", amount=100.0)
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=conditional("stX", pair=trade.pair, side="sell", amount=100.0)
    )
    bot.exchange.fetch_open_conditional_orders.return_value = [check]

    report = bot._pm_reconcile_open_orders()

    assert report["unprotected_positions"]
    assert "stop_protection_missing" in bot._pm_blocked_order_reasons()


# ---------------------------------------------------------------------------
# P0-6: dual protection window.
# ---------------------------------------------------------------------------
def test_dual_protection_window_never_touches_second_protection(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stold", side="sell")
    # A second protection already active from a previous dual window.
    trade.orders.append(
        Order(
            ft_order_side="stoploss",
            ft_pair=trade.pair,
            ft_is_open=True,
            ft_amount=trade.amount,
            ft_price=0.008,
            order_id="stmid",
            status="open",
            symbol=trade.pair,
            order_type="stoploss",
            side=trade.exit_side,
            price=0.008,
            average=0.008,
            filled=0.0,
            remaining=trade.amount,
            cost=0.0,
            order_date=trade.open_date,
        )
    )
    Trade.commit()
    for sl in trade.open_sl_orders:
        sl.order_date = datetime.now(UTC) - timedelta(seconds=120)
    bot.exchange.stoploss_adjust = MagicMock(return_value=True)
    bot.exchange.create_stoploss = MagicMock(
        return_value=ccxt_order("stnew", "open", "sell", filled=0)
    )
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=conditional("stnew", amount=11.0, info={"reduceOnly": True})
    )
    bot.exchange.cancel_stoploss_order_with_result = MagicMock(
        return_value=ccxt_order("stold", "canceled", "sell", filled=0)
    )

    def close_old(trade_, order_id, action_order, **kwargs):
        for sl in trade_.open_sl_orders:
            if str(sl.order_id) == str(order_id):
                sl.ft_is_open = False
                sl.status = "canceled"
        return True

    bot.update_trade_state = MagicMock(side_effect=close_old)
    bot._pm_link_intent_for_order = MagicMock()

    bot.handle_trailing_stoploss_on_exchange(
        trade, ccxt_order("stold", "open", "sell", filled=0)
    )

    # Only the OLD id was retired; the pre-existing protection is untouched.
    assert bot.exchange.cancel_stoploss_order_with_result.call_args_list == [
        call("stold", trade.pair, trade.amount)
    ]
    assert {sl.order_id for sl in trade.open_sl_orders} == {"stmid", "stnew"}


def test_stop_fill_event_never_retires_other_protection(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stold", side="sell")
    trade.orders.append(
        Order(
            ft_order_side="stoploss",
            ft_pair=trade.pair,
            ft_is_open=True,
            ft_amount=trade.amount,
            ft_price=0.008,
            order_id="stnew",
            status="open",
            symbol=trade.pair,
            order_type="stoploss",
            side=trade.exit_side,
            price=0.008,
            average=0.008,
            filled=0.0,
            remaining=trade.amount,
            cost=0.0,
            order_date=trade.open_date,
        )
    )
    Trade.commit()
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=conditional("stold", status="closed", info={"reduceOnly": True})
    )
    bot.exchange.cancel_stoploss_order_with_result = MagicMock()
    bot.update_trade_state = MagicMock()
    ev = event(order_id="stold", client="", status="FILLED", filled="11")

    assert bot._pm_handle_order_trade_update(ev, {}) is True

    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()
    assert {sl.order_id for sl in trade.open_sl_orders} == {"stold", "stnew"}
