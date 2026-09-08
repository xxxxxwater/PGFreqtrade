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
from freqtrade.persistence import Order, PMOrderIntent, PMOutbox, Trade
from tests.freqtradebot.test_pm_order_ownership import event
from tests.freqtradebot.test_pm_recovery import ccxt_order, make_open_trade, make_pm_bot, make_stoploss_trade
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
    assert "stop_retire_unresolved" not in bot._pm_blocked_order_reasons()
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
    def close_trade(*args, **kwargs):
        trade.is_open = False
        trade.amount = 0.0
        return False

    bot.update_trade_state = MagicMock(side_effect=close_trade)
    bot.emergency_exit = MagicMock()

    verdict = bot._pm_replace_stop_protection(trade, ["stold"], new_stop_price=0.009)

    assert verdict == "terminal"
    # The fill was processed FIRST through the full lifecycle...
    bot.update_trade_state.assert_called_once()
    assert bot.update_trade_state.call_args.args[1] == "stnew"
    bot.emergency_exit.assert_not_called()
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
    assert "stop_retire_unresolved" not in bot._pm_blocked_order_reasons()


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

def test_retire_not_found_then_history_absent_keeps_local_pending(mocker, pm_conf):
    """DELETE not-found + no lifecycle proof must never be rewritten as canceled."""
    bot, trade = switch_bot(mocker, pm_conf)
    old = trade.open_sl_orders[0]
    bot.exchange.cancel_stoploss_order_with_result = MagicMock(
        side_effect=InvalidOrderException("delete says not found")
    )
    bot.exchange.fetch_stoploss_order = MagicMock(
        side_effect=InvalidOrderException("not open, not in history")
    )

    resolved = bot._pm_retire_stop_ids(trade, [str(old.order_id)])

    assert resolved is False
    assert old.ft_is_open is True
    assert old.status not in {"canceled", "cancelled"}
    assert "stop_retire_unresolved" in bot._pm_blocked_order_reasons()
    bot.update_trade_state.assert_not_called() if isinstance(bot.update_trade_state, MagicMock) else None


def test_retire_not_found_then_triggered_child_processes_lifecycle(mocker, pm_conf):
    """If history proves the old strategy triggered, process the child instead of canceling it."""
    bot, trade = switch_bot(mocker, pm_conf)
    old = trade.open_sl_orders[0]
    bot.exchange.cancel_stoploss_order_with_result = MagicMock(
        side_effect=InvalidOrderException("delete says not found")
    )
    triggered = conditional(
        str(old.order_id),
        status="closed",
        info={"algo_status": "TRIGGERED", "actual_order_id": "real-777", "actual_order": {"id": "real-777"}, "reduceOnly": True},
    )
    triggered["status_stop"] = "triggered"
    bot.exchange.fetch_stoploss_order = MagicMock(return_value=triggered)
    bot.update_trade_state = MagicMock()

    resolved = bot._pm_retire_stop_ids(trade, [str(old.order_id)])

    assert resolved is True
    bot.update_trade_state.assert_called_once_with(
        trade, str(old.order_id), triggered, stoploss_order=True
    )
    assert old.status != "canceled"


def test_replace_verified_new_but_old_retire_ambiguous_stays_fail_closed(mocker, pm_conf):
    bot, trade = switch_bot(mocker, pm_conf)
    bot.exchange.fetch_stoploss_order = MagicMock(
        side_effect=[
            conditional("stnew", amount=11.0, info={"reduceOnly": True}),
            InvalidOrderException("old absent from lifecycle"),
        ]
    )
    bot.exchange.cancel_stoploss_order_with_result = MagicMock(
        side_effect=InvalidOrderException("old cancel not found")
    )

    verdict = bot._pm_replace_stop_protection(trade, ["stold"], new_stop_price=0.009)

    assert verdict == "kept_old"
    old = next(sl for sl in trade.open_sl_orders if sl.order_id == "stold")
    assert old.ft_is_open is True
    assert old.status != "canceled"
    assert "stop_retire_unresolved" in bot._pm_blocked_order_reasons()
    assert "stop_protection_missing" not in bot._pm_blocked_order_reasons()

def test_triggered_child_partial_fill_is_pending_not_terminal(mocker, pm_conf):
    bot, trade = switch_bot(mocker, pm_conf)
    check = conditional("stnew", status="open", amount=11.0, info={"reduceOnly": True, "actual_order": {"id": "child1"}})
    check["status_stop"] = "triggered"
    check["filled"] = 4.0
    check["remaining"] = 7.0
    bot.exchange.fetch_stoploss_order = MagicMock(return_value=check)
    bot.update_trade_state = MagicMock()
    bot.emergency_exit = MagicMock()

    verdict = bot._pm_replace_stop_protection(trade, ["stold"], new_stop_price=0.009)

    assert verdict == "triggered_pending"
    bot.update_trade_state.assert_called_once()
    bot.emergency_exit.assert_not_called()
    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()
    assert "stop_triggered_exit_pending" in bot._pm_blocked_order_reasons()


def test_triggered_child_terminal_partial_books_fill_then_exits_remainder(mocker, pm_conf):
    bot, trade = switch_bot(mocker, pm_conf)
    check = conditional("stnew", status="canceled", amount=11.0, info={"reduceOnly": True, "actual_order": {"id": "child1"}})
    check["status_stop"] = "triggered"
    check["filled"] = 4.0
    check["remaining"] = 7.0
    bot.exchange.fetch_stoploss_order = MagicMock(return_value=check)

    def apply_partial(*args, **kwargs):
        trade.amount = 7.0
        trade.is_open = True
        return False

    bot.update_trade_state = MagicMock(side_effect=apply_partial)
    bot.emergency_exit = MagicMock()

    verdict = bot._pm_replace_stop_protection(trade, ["stold"], new_stop_price=0.009)

    assert verdict == "triggered_failed"
    bot.update_trade_state.assert_called_once()
    bot.emergency_exit.assert_called_once_with(trade, trade.stoploss_or_liquidation)
    assert "stop_protection_missing" in bot._pm_blocked_order_reasons()


def test_uncertain_stop_dispatch_escalates_bot_trade_after_grace(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="entry1", side="buy", amount=11.0)
    # This fixture models already-confirmed exposure; remove the synthetic open
    # entry order so protection management sees no local stop candidate.
    trade.orders.clear()
    Trade.commit()
    bot.config["exchange"]["portfolio_margin_risk"]["uncertain_stop_emergency_exit_seconds"] = 5
    client_id = "st-uncertain-owned"
    request = {
        "algoType": "CONDITIONAL",
        "symbol": "ETHUSDT",
        "side": "SELL",
        "type": "STOP_MARKET",
        "reduceOnly": "true",
        "triggerPrice": 0.009,
        "clientAlgoId": client_id,
        "quantity": 11.0,
    }
    bot.exchange._pm_enqueue(
        client_id,
        {
            "kind": "conditional",
            "pair": trade.pair,
            "side": trade.exit_side,
            "type": "STOP_MARKET",
            "amount": trade.amount,
            "stop_price": 0.009,
            "reduce_only": True,
            "origin_trade_id": trade.id,
        },
        payload=request,
    )
    bot.exchange._pm_dispatch_marker_set(client_id)
    intent = PMOrderIntent.get_by_client_id(client_id)
    intent.state = "UNKNOWN"
    outbox = PMOutbox.get_by_client_id(client_id)
    outbox.dispatch_started_at = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=10)
    Trade.commit()
    bot.emergency_exit = MagicMock()

    handled = bot._pm_handle_uncertain_stop_dispatch(trade)

    assert handled is True
    bot.emergency_exit.assert_called_once_with(trade, trade.stoploss_or_liquidation)
    assert PMOrderIntent.get_by_client_id(client_id).state == "UNKNOWN"
    assert PMOutbox.get_by_client_id(client_id).dispatch_started_at is not None


def test_uncertain_stop_before_grace_blocks_without_new_exit(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="entry2", side="buy", amount=11.0)
    trade.orders.clear()
    Trade.commit()
    bot.config["exchange"]["portfolio_margin_risk"]["uncertain_stop_emergency_exit_seconds"] = 30
    client_id = "st-uncertain-young"
    bot.exchange._pm_enqueue(
        client_id,
        {
            "kind": "conditional",
            "pair": trade.pair,
            "side": trade.exit_side,
            "type": "STOP_MARKET",
            "amount": trade.amount,
            "stop_price": 0.009,
            "reduce_only": True,
            "origin_trade_id": trade.id,
        },
        payload={"symbol": "ETHUSDT", "clientAlgoId": client_id},
    )
    bot.exchange._pm_dispatch_marker_set(client_id)
    PMOrderIntent.get_by_client_id(client_id).state = "UNKNOWN"
    Trade.commit()
    bot.emergency_exit = MagicMock()

    assert bot._pm_handle_uncertain_stop_dispatch(trade) is True
    bot.emergency_exit.assert_not_called()
    assert "stop_protection_missing" in bot._pm_blocked_order_reasons()



@pytest.mark.parametrize("child", [None, {"id": "real-pending"}])
def test_retire_triggered_child_unconfirmed_never_completes(mocker, pm_conf, child):
    bot, trade = switch_bot(mocker, pm_conf)
    old = trade.open_sl_orders[0]
    bot.exchange.cancel_stoploss_order_with_result = MagicMock(
        side_effect=InvalidOrderException("not found")
    )
    info = {"reduceOnly": True}
    if child is not None:
        info["actual_order"] = child
    check = conditional(str(old.order_id), status="open", info=info)
    check["status_stop"] = "triggered"
    check["filled"] = 4.0
    bot.exchange.fetch_stoploss_order = MagicMock(return_value=check)
    bot.update_trade_state = MagicMock()

    assert bot._pm_retire_stop_ids(trade, [str(old.order_id)]) is False
    assert old.ft_is_open
    assert "stop_triggered_exit_pending" in bot._pm_blocked_order_reasons()
    assert bot.update_trade_state.call_count == (0 if child is None else 1)


@pytest.mark.parametrize("bad_symbol", [None, "BTC/USDT:USDT"])
@pytest.mark.parametrize("path", ["scheduled", "stop_handler"])
def test_wrong_identity_terminal_stop_never_updates_trade(mocker, pm_conf, bad_symbol, path):
    bot, trade = switch_bot(mocker, pm_conf)
    bot.strategy.order_types["stoploss_on_exchange"] = True
    bad = conditional("stold", status="canceled", pair=bad_symbol)
    bot.exchange.fetch_open_conditional_orders = MagicMock(return_value=[])
    bot.exchange.fetch_stoploss_order = MagicMock(return_value=bad)
    bot.update_trade_state = MagicMock()
    bot.emergency_exit = MagicMock()
    mocker.patch.object(Trade, "get_open_trades", return_value=[trade])
    if path == "scheduled":
        assert bot._pm_verify_stop_protection()
    else:
        assert bot.handle_stoploss_on_exchange(trade) is False
    bot.update_trade_state.assert_not_called()
    bot.emergency_exit.assert_not_called()
    assert trade.open_sl_orders[0].ft_is_open


@pytest.mark.parametrize("quantity", [float("nan"), float("inf"), -1.0])
def test_nonfinite_protection_quantity_never_proves_coverage(mocker, pm_conf, quantity):
    bot, trade = switch_bot(mocker, pm_conf)
    check = conditional("stold", amount=quantity, info={"reduceOnly": True})
    verdict, _ = bot._pm_validate_protection_order(trade, "stold", check)
    assert verdict == "invalid"
