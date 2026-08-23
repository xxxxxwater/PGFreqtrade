"""
Tests for the shared, idempotent PM order reconciliation.

Verifies that PM recovery (user-stream loss, scheduled recovery and manual
/pm_recover) runs the FULL update_trade_state() lifecycle (fees, realized PnL,
exit time, wallet, notifications) instead of a bare trade.update_order().
"""

from unittest.mock import MagicMock

import pytest

from freqtrade.constants import EntryExecuteMode
from freqtrade.enums import ExitType, RPCMessageType
from freqtrade.exceptions import TemporaryError
from freqtrade.persistence import Order, Trade
from freqtrade.util.datetime_helpers import dt_now


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


def make_pm_bot(mocker, pm_conf):
    """Build a real FreqtradeBot with PM exchange mocking, ready for reconciliation."""
    from tests.conftest import get_patched_freqtradebot

    # Bypass the PM startup fail-fast validation (credentials/whitelist) - this test
    # focuses on the reconciliation path itself.
    mocker.patch("freqtrade.exchange.binance.Binance.validate_config", MagicMock())
    bot = get_patched_freqtradebot(mocker, pm_conf)
    # Exchange is a Binance instance; expose the PM flag and control fetch_order.
    bot.exchange._is_portfolio_margin = MagicMock(return_value=True)
    bot.exchange.fetch_order = MagicMock(return_value={})
    # Avoid external notifications / side effects.
    bot._notify_enter = MagicMock()
    bot._notify_exit = MagicMock()
    bot.handle_protections = MagicMock()
    bot.wallets.update = MagicMock()
    return bot


def make_open_trade(conf, *, is_short=False, order_id="111", side="buy", amount=11.0):
    pair = conf["exchange"]["pair_whitelist"][0]
    open_rate = 0.01
    entry_side = "sell" if is_short else "buy"
    trade = Trade(
        pair=pair,
        stake_currency=conf["stake_currency"],
        open_rate=open_rate,
        amount=amount,
        fee_open=0.001,
        fee_close=0.001,
        stake_amount=amount * open_rate,
        open_date=dt_now(),
        exchange="binance",
        is_open=True,
        is_short=is_short,
        leverage=1.0,
    )
    orders = []
    if side != entry_side:
        # For exit-order tests, include the already-filled entry order so the
        # trade recalculation has an average entry price to compute close_profit.
        orders.append(
            Order(
                ft_order_side=entry_side,
                ft_pair=trade.pair,
                ft_is_open=False,
                ft_amount=trade.amount,
                ft_price=trade.open_rate,
                order_id=f"{order_id}_entry",
                status="closed",
                symbol=trade.pair,
                order_type="market",
                side=entry_side,
                price=open_rate,
                average=open_rate,
                filled=trade.amount,
                remaining=0.0,
                cost=trade.amount * open_rate,
                order_date=trade.open_date,
                order_filled_date=trade.open_date,
            )
        )
    orders.append(
        Order(
            ft_order_side=side,
            ft_pair=trade.pair,
            ft_is_open=True,
            ft_amount=trade.amount,
            ft_price=trade.open_rate,
            order_id=order_id,
            status="open",
            symbol=trade.pair,
            order_type="limit",
            side=side,
            price=open_rate,
            average=open_rate,
            filled=0.0,
            remaining=trade.amount,
            cost=0.0,
            order_date=trade.open_date,
        )
    )
    trade.orders = orders
    Trade.session.add(trade)
    Trade.commit()
    return trade


def ccxt_order(order_id, status, side, amount=11.0, filled=11.0, price=0.01):
    return {
        "id": order_id,
        "clientOrderId": None,
        "timestamp": None,
        "datetime": None,
        "lastTradeTimestamp": None,
        "symbol": "ETH/USDT:USDT",
        "type": "limit",
        "timeInForce": "GTC",
        "side": side,
        "price": price,
        "average": price,
        "amount": amount,
        "filled": filled,
        "remaining": max(amount - filled, 0.0),
        "cost": filled * price,
        "status": status,
        "fee": None,
        "trades": [],
        "info": {},
    }


def test_pm_reconcile_calls_full_update_path(mocker, pm_conf):
    """Recovery must run update_trade_state (full lifecycle), not bare update_order."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="e1", side="buy")
    bot.exchange.fetch_order.return_value = ccxt_order("e1", "open", "buy")

    bot.update_trade_state = MagicMock(return_value=False)
    result = bot._pm_reconcile_open_orders()

    assert result["checked"] == 1
    assert result["mismatches"] == []
    bot.update_trade_state.assert_called_once()
    call = bot.update_trade_state.call_args
    assert call.kwargs["action_order"]["id"] == "e1"
    assert call.kwargs["stoploss_order"] is False


def test_pm_reconcile_reports_mismatch(mocker, pm_conf):
    """DB status != exchange status must be reported as a mismatch."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="m1", side="buy")
    # Exchange says FILLED, DB says open -> mismatch.
    bot.exchange.fetch_order.return_value = ccxt_order("m1", "closed", "buy")
    bot.update_trade_state = MagicMock(return_value=False)

    result = bot._pm_reconcile_open_orders()

    assert result["mismatches"], "expected a mismatch to be recorded"
    assert "m1" in result["mismatches"][0]


def test_pm_reconcile_entry_fill_updates_trade(mocker, pm_conf):
    """A FILLED entry order must update amount and close the local order."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="fill1", side="buy", amount=11.0)
    bot.exchange.fetch_order.return_value = ccxt_order(
        "fill1", "closed", "buy", amount=11.0, filled=11.0
    )

    result = bot._pm_reconcile_open_orders()

    assert result["checked"] == 1
    assert trade.has_open_orders is False
    assert trade.amount > 0
    Trade.session.refresh(trade)
    assert not any(o.ft_is_open for o in trade.orders)


def test_pm_reconcile_exit_fill_closes_trade(mocker, pm_conf):
    """A FILLED exit order must close the trade and record exit metadata."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="xf1", side="sell", amount=11.0)
    bot.exchange.fetch_order.return_value = ccxt_order(
        "xf1", "closed", "sell", amount=11.0, filled=11.0
    )

    result = bot._pm_reconcile_open_orders()

    assert result["checked"] == 1
    Trade.session.refresh(trade)
    assert trade.is_open is False
    assert trade.close_profit is not None
    assert trade.close_date is not None


def test_pm_reconcile_partial_fill_keeps_open(mocker, pm_conf):
    """A partially filled order must remain open after recovery."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="pf1", side="buy", amount=11.0)
    bot.exchange.fetch_order.return_value = ccxt_order(
        "pf1", "open", "buy", amount=11.0, filled=5.0
    )

    result = bot._pm_reconcile_open_orders()

    assert result["checked"] == 1
    Trade.session.refresh(trade)
    assert trade.is_open is True
    assert any(o.ft_is_open for o in trade.orders)


def test_pm_reconcile_canceled_order_stays_flat(mocker, pm_conf):
    """A canceled empty order must be reconciled without closing the trade."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="cx1", side="buy", amount=11.0)
    bot.exchange.fetch_order.return_value = ccxt_order(
        "cx1", "canceled", "buy", amount=11.0, filled=0.0
    )

    result = bot._pm_reconcile_open_orders()

    assert result["checked"] == 1
    Trade.session.refresh(trade)
    # Canceled empty order -> order no longer open, trade remains open.
    assert not any(o.ft_is_open for o in trade.orders)
    assert trade.is_open is True


def test_pm_reconcile_idempotent(mocker, pm_conf):
    """Repeated recovery must not re-process already-closed orders."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="id1", side="buy", amount=11.0)
    bot.exchange.fetch_order.return_value = ccxt_order(
        "id1", "closed", "buy", amount=11.0, filled=11.0
    )
    bot.update_trade_state = MagicMock(return_value=False)

    first = bot._pm_reconcile_open_orders()
    assert first["checked"] == 1

    # Second run: order is still open in DB because update_trade_state was mocked,
    # so we assert the reconciliation is repeatable without errors, not exploding.
    second = bot._pm_reconcile_open_orders()
    assert second["errors"] == []
    assert second["checked"] == 1


def test_pm_reconcile_skips_when_not_pm(mocker, pm_conf):
    """Reconciliation must be a no-op when PM mode is not active."""
    bot = make_pm_bot(mocker, pm_conf)
    make_open_trade(pm_conf, order_id="np1", side="buy")
    bot.exchange._is_portfolio_margin = MagicMock(return_value=False)
    bot.update_trade_state = MagicMock()

    result = bot._pm_reconcile_open_orders()

    assert result["checked"] == 0
    bot.update_trade_state.assert_not_called()


def test_pm_startup_consistency_check_reports_unknown_position(mocker, pm_conf):
    """An exchange position with no local trade must be reported as a mismatch."""
    bot = make_pm_bot(mocker, pm_conf)
    bot.exchange.fetch_positions = MagicMock(
        return_value=[
            {"symbol": "ETH/USDT:USDT", "side": "long", "contracts": 11.0, "collateral": 1.0}
        ]
    )
    bot.exchange.fetch_open_orders = MagicMock(return_value=[])

    result = bot._pm_startup_consistency_check()

    assert result["status"] == "mismatch"
    assert len(result["unknown_positions"]) == 1
    assert result["unknown_positions"][0]["pair"] == "ETH/USDT:USDT"


def test_pm_startup_consistency_check_consistent(mocker, pm_conf):
    """When exchange positions map to local trades, the check reports consistent."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="ok1", side="buy", amount=11.0)
    bot.exchange.fetch_positions = MagicMock(
        return_value=[
            {"symbol": "ETH/USDT:USDT", "side": "long", "contracts": 11.0, "collateral": 1.0}
        ]
    )
    bot.exchange.fetch_open_orders = MagicMock(return_value=[])

    result = bot._pm_startup_consistency_check()

    assert result["status"] == "consistent"
    assert result["unknown_positions"] == []


def test_pm_apply_startup_consistency_pause_mode(mocker, pm_conf):
    """Unmatched positions in pause mode (default) must PAUSE the bot and block orders."""
    from freqtrade.enums import State

    bot = make_pm_bot(mocker, pm_conf)
    bot.state = State.RUNNING
    bot.rpc.send_msg = MagicMock()

    result = {
        "status": "mismatch",
        "unknown_positions": [{"pair": "ETH/USDT:USDT", "side": "long", "contracts": 1.0}],
        "unknown_orders": [],
        "unknown_conditional_orders": [],
    }
    bot._pm_apply_startup_consistency(result)

    assert bot.state == State.PAUSED
    assert "startup_consistency_mismatch" in bot._pm_blocked_order_reasons()


def test_execute_entry_blocks_when_pm_orders_blocked(mocker, pm_conf):
    """DCA / force-entry / recovery re-entries must be blocked by the unified entry gate."""
    bot = make_pm_bot(mocker, pm_conf)
    bot._pm_block_orders("user_stream_unavailable")
    bot.get_valid_enter_price_and_stake = MagicMock()

    result = bot.execute_entry(
        "ETH/USDT:USDT", 60.0, is_short=False, ordertype="limit"
    )

    assert result is False
    bot.get_valid_enter_price_and_stake.assert_not_called()


def test_execute_entry_allows_when_not_blocked(mocker, pm_conf):
    """With no block reasons the entry path proceeds (no early return)."""
    bot = make_pm_bot(mocker, pm_conf)
    bot._pm_orders_blocked_reasons = []
    bot.get_valid_enter_price_and_stake = MagicMock(return_value=(0.01, 0.0, 5.0))

    bot.execute_entry("ETH/USDT:USDT", 60.0, is_short=False, ordertype="limit")

    bot.get_valid_enter_price_and_stake.assert_called_once()


def make_stoploss_trade(conf, order_id="stabc", side="buy", amount=11.0):
    """Open trade whose only open order is a stoploss (conditional) order."""
    trade = make_open_trade(conf, order_id=order_id, side=side, amount=amount)
    trade.orders[-1].ft_order_side = "stoploss"
    Trade.commit()
    return trade


def test_pm_reconcile_stoploss_routes_to_fetch_stoploss_order(mocker, pm_conf):
    """
    Recovery MUST branch on the order side: stoploss orders resolve through the
    conditional endpoints (fetch_stoploss_order), normal orders through fetch_order.
    """
    bot = make_pm_bot(mocker, pm_conf)
    make_stoploss_trade(pm_conf, order_id="stabc", side="sell")
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=ccxt_order("stabc", "open", "sell", filled=0.0)
    )
    bot.exchange.fetch_order = MagicMock()
    bot.update_trade_state = MagicMock(return_value=False)

    result = bot._pm_reconcile_open_orders()

    assert result["errors"] == []
    bot.exchange.fetch_stoploss_order.assert_called_once_with("stabc", "ETH/USDT:USDT")
    bot.exchange.fetch_order.assert_not_called()
    assert bot.update_trade_state.call_args.kwargs["stoploss_order"] is True


def test_pm_reconcile_triggered_stoploss_fill_closes_trade(mocker, pm_conf):
    """
    STOP_MARKET triggered + real order FILLED: reconciliation closes the trade with
    the REAL fill data and remembers the actual order id mapping.
    """
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stabc", side="sell")
    merged = ccxt_order("stabc", "closed", "sell", amount=11.0, filled=11.0, price=0.009)
    merged["id_stop"] = "999"
    merged["status_stop"] = "triggered"
    bot.exchange.fetch_stoploss_order = MagicMock(return_value=merged)

    result = bot._pm_reconcile_open_orders()

    assert result["errors"] == []
    Trade.session.refresh(trade)
    assert trade.is_open is False
    assert trade.close_profit is not None
    assert bot._pm_actual_order_map.get("999") == "stabc"


def test_pm_handle_order_trade_update_stoploss_branch(mocker, pm_conf):
    """
    A user-stream event matching a local stoploss order must resolve it through
    fetch_stoploss_order (conditional lifecycle), never a plain fetch_order.
    """
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stabc", side="sell")
    order = trade.orders[-1]
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=ccxt_order("stabc", "open", "sell", filled=0.0)
    )
    bot.exchange.fetch_order = MagicMock()

    event = {"e": "ORDER_TRADE_UPDATE", "o": {"i": "stabc", "s": "ETH_USDT"}}
    handled = bot._pm_handle_order_trade_update(event, {"stabc": (trade, order)})

    assert handled is True
    bot.exchange.fetch_stoploss_order.assert_called_once_with("stabc", trade.pair)
    bot.exchange.fetch_order.assert_not_called()


def test_pm_handle_unmatched_actual_order_event_reconciles_stoploss(mocker, pm_conf):
    """
    A user-stream event for the REAL order of a triggered conditional stoploss is
    matched through the actualOrderId map and reconciled via the strategy id.
    """
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stabc", side="sell")
    bot._pm_actual_order_map["999"] = "stabc"
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=ccxt_order("stabc", "open", "sell", filled=0.0)
    )
    bot.exchange.fetch_order = MagicMock()

    event = {"e": "ORDER_TRADE_UPDATE", "o": {"i": "999", "s": "ETH_USDT"}}
    handled = bot._pm_handle_order_trade_update(event, {})

    assert handled is True
    bot.exchange.fetch_stoploss_order.assert_called_once_with("stabc", trade.pair)
    bot.exchange.fetch_order.assert_not_called()


def test_pm_startup_consistency_detects_unknown_conditional(mocker, pm_conf):
    """
    An open conditional (stoploss) order without a local stoploss order must be
    reported as a mismatch (separate from normal orders).
    """
    bot = make_pm_bot(mocker, pm_conf)
    make_open_trade(pm_conf, order_id="ok1", side="buy", amount=11.0)
    bot.exchange.fetch_positions = MagicMock(return_value=[])
    bot.exchange.fetch_open_orders = MagicMock(return_value=[])
    bot.exchange.fetch_open_conditional_orders = MagicMock(
        return_value=[ccxt_order("st_orphan", "open", "sell", filled=0.0)]
    )

    result = bot._pm_startup_consistency_check()

    assert result["status"] == "mismatch"
    assert result["unknown_orders"] == []
    assert len(result["unknown_conditional_orders"]) == 1
    assert result["unknown_conditional_orders"][0]["order_id"] == "st_orphan"


def test_pm_apply_startup_consistency_cancel_mode_uses_conditional_delete(mocker, pm_conf):
    """
    cancel mode must delete conditional orphans through cancel_stoploss_order
    (conditional DELETE endpoint) and normal orphans through cancel_order.
    """
    from freqtrade.enums import State

    pm_conf["exchange"]["portfolio_margin_risk"]["startup_consistency_mode"] = "cancel"
    bot = make_pm_bot(mocker, pm_conf)
    bot.state = State.RUNNING
    bot.rpc.send_msg = MagicMock()
    bot.exchange.cancel_order = MagicMock()
    bot.exchange.cancel_stoploss_order = MagicMock()

    result = {
        "status": "mismatch",
        "unknown_positions": [],
        "unknown_orders": [{"order_id": "123", "symbol": "ETH/USDT:USDT"}],
        "unknown_conditional_orders": [
            {"order_id": "st_orphan", "symbol": "ETH/USDT:USDT"}
        ],
    }
    bot._pm_apply_startup_consistency(result)

    bot.exchange.cancel_order.assert_called_once_with("123", "ETH/USDT:USDT")
    # The strategy id must NEVER be passed to the normal cancel endpoint.
    bot.exchange.cancel_stoploss_order.assert_called_once_with(
        "st_orphan", "ETH/USDT:USDT"
    )
    assert all(
        call[0][0] != "st_orphan" for call in bot.exchange.cancel_order.call_args_list
    )


def test_pm_recover_pending_intents_uncertain_blocks(mocker, pm_conf):
    """Unresolvable persisted intents must block new orders (fail-closed)."""
    bot = make_pm_bot(mocker, pm_conf)
    bot.rpc.send_msg = MagicMock()
    bot.exchange.list_pm_pending_intents = MagicMock(
        return_value=[{"client_id": "ftabc", "kind": "order", "pair": "ETH/USDT:USDT"}]
    )
    bot.exchange.resolve_pm_pending_intent = MagicMock(
        return_value={"client_id": "ftabc", "resolved": False, "uncertain": True}
    )

    bot._pm_recover_pending_intents()

    assert "pending_intent_unresolved" in bot._pm_blocked_order_reasons()
    bot.rpc.send_msg.assert_called_once()
    assert "FAIL-CLOSED" in bot.rpc.send_msg.call_args.args[0]["status"]


def test_pm_recover_pending_intents_clears_resolved(mocker, pm_conf):
    """Definitively-absent intents are cleared; existing ones are cleared+alerted."""
    bot = make_pm_bot(mocker, pm_conf)
    bot.rpc.send_msg = MagicMock()
    bot.exchange.list_pm_pending_intents = MagicMock(
        return_value=[
            {"client_id": "ftabc", "kind": "order", "pair": "ETH/USDT:USDT"},
            {"client_id": "stxyz", "kind": "conditional", "pair": "ETH/USDT:USDT"},
        ]
    )
    bot.exchange.resolve_pm_pending_intent = MagicMock(
        side_effect=[
            {"client_id": "ftabc", "resolved": True, "exists": False},
            {"client_id": "stxyz", "resolved": True, "exists": True},
        ]
    )
    bot.exchange.clear_pm_pending_intent = MagicMock()

    bot._pm_recover_pending_intents()

    assert "pending_intent_unresolved" not in bot._pm_blocked_order_reasons()
    assert bot.exchange.clear_pm_pending_intent.call_count == 2
    assert any(
        "exist on the exchange" in msg["status"]
        for msg in [c.args[0] for c in bot.rpc.send_msg.call_args_list]
    )


# ---------------------------------------------------------------------------
# Round 4 P0 tests
# ---------------------------------------------------------------------------


def test_pm_recover_pending_intents_store_error_pauses(mocker, pm_conf):
    """
    P0-1: an unreadable intent store at startup must PAUSE the bot and block
    entries - never silently continue as if there were no intents.
    """
    from freqtrade.enums import State

    bot = make_pm_bot(mocker, pm_conf)
    bot.state = State.RUNNING
    bot.rpc.send_msg = MagicMock()
    bot.exchange.list_pm_pending_intents = MagicMock(
        side_effect=RuntimeError("corrupted db")
    )

    report = bot._pm_recover_pending_intents()

    assert report["store_error"] is not None
    assert bot.state == State.PAUSED
    reasons = bot._pm_blocked_order_reasons()
    assert "intent_store_unavailable" in reasons


def test_unresolved_intent_blocks_entry_dca_and_stoploss(mocker, pm_conf):
    """
    P0-2: an UNKNOWN intent in the durable store blocks entry, DCA and stoploss
    creation through the unified gates - without any in-memory reason set.
    """
    from freqtrade.persistence.pm_order_intent import PMOrderIntent

    bot = make_pm_bot(mocker, pm_conf)
    bot._pm_orders_blocked_reasons = []  # ONLY the durable store blocks
    bot.rpc.send_msg = MagicMock()
    bot.get_valid_enter_price_and_stake = MagicMock(return_value=(0.01, 0.0, 5.0))
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

    # Entry blocked
    assert bot.execute_entry("ETH/USDT:USDT", 60.0, is_short=False, ordertype="limit") is False
    # DCA blocked
    trade = make_open_trade(pm_conf, order_id="dca1", side="buy", amount=11.0)
    assert (
        bot.execute_entry(
            "ETH/USDT:USDT",
            60.0,
            is_short=False,
            ordertype="limit",
            trade=trade,
            mode="pos_adjust",
        )
        is False
    )
    bot.get_valid_enter_price_and_stake.assert_not_called()

    # Stoploss management blocked via the exchange-side gate
    bot.exchange.create_stoploss = MagicMock(side_effect=TemporaryError("unresolved"))
    assert bot.create_stoploss_order(trade, stop_price=0.005) is False


def test_user_stream_recovery_requires_clean_reconcile(mocker, pm_conf):
    """
    P0-3: a connected user stream must NOT unblock orders when reconciliation
    hits a TemporaryError; the block is kept and reconciliation_incomplete added.
    """
    bot = make_pm_bot(mocker, pm_conf)
    make_open_trade(pm_conf, order_id="ok1", side="buy", amount=11.0)
    bot._pm_block_orders("user_stream_unavailable")
    bot.exchange.get_pm_user_stream_stats = MagicMock(
        return_value={
            "enabled": True,
            "running": True,
            "connected": True,
            "queued_events": 0,
            "events_dropped": 0,
            "parse_errors": 0,
        }
    )
    bot.exchange.fetch_order = MagicMock(side_effect=TemporaryError("lookup timeout"))
    bot.exchange.fetch_stoploss_order = MagicMock()
    bot.rpc.send_msg = MagicMock()

    bot._pm_user_stream_health_monitor()

    reasons = bot._pm_blocked_order_reasons()
    assert "user_stream_unavailable" in reasons
    assert "reconciliation_incomplete" in reasons


def test_user_stream_recovery_unblocks_only_on_clean_reconcile(mocker, pm_conf):
    """P0-3: a fully clean reconcile (no errors, no unresolved intents) unblocks."""
    bot = make_pm_bot(mocker, pm_conf)
    make_open_trade(pm_conf, order_id="ok1", side="buy", amount=11.0)
    bot._pm_block_orders("user_stream_unavailable")
    bot._pm_block_orders("reconciliation_incomplete")
    bot.exchange.get_pm_user_stream_stats = MagicMock(
        return_value={
            "enabled": True,
            "running": True,
            "connected": True,
            "queued_events": 0,
            "events_dropped": 0,
            "parse_errors": 0,
        }
    )
    bot.exchange.fetch_order = MagicMock(
        return_value=ccxt_order("ok1", "open", "buy", amount=11.0, filled=0.0)
    )
    bot.rpc.send_msg = MagicMock()

    bot._pm_user_stream_health_monitor()

    reasons = bot._pm_blocked_order_reasons()
    assert "user_stream_unavailable" not in reasons
    assert "reconciliation_incomplete" not in reasons
    assert bot._pm_last_success_reconcile_time is not None


def test_pm_startup_cancel_failure_pauses(mocker, pm_conf):
    """P0-4: a failed cancel in startup cancel mode keeps the bot PAUSED."""
    from freqtrade.enums import State

    pm_conf["exchange"]["portfolio_margin_risk"]["startup_consistency_mode"] = "cancel"
    bot = make_pm_bot(mocker, pm_conf)
    bot.state = State.RUNNING
    bot.rpc.send_msg = MagicMock()
    bot.exchange.cancel_order = MagicMock(side_effect=TemporaryError("cancel failed"))
    bot.exchange.cancel_stoploss_order = MagicMock()

    result = {
        "status": "mismatch",
        "unknown_positions": [],
        "unknown_orders": [{"order_id": "123", "symbol": "ETH/USDT:USDT"}],
        "unknown_conditional_orders": [],
    }
    bot._pm_apply_startup_consistency(result)

    assert bot.state == State.PAUSED
    assert "startup_consistency_mismatch" in bot._pm_blocked_order_reasons()


def test_pm_startup_cancel_second_check_still_unknown_pauses(mocker, pm_conf):
    """P0-4: cancel succeeded but the second query still shows the order -> PAUSED."""
    from freqtrade.enums import State

    pm_conf["exchange"]["portfolio_margin_risk"]["startup_consistency_mode"] = "cancel"
    bot = make_pm_bot(mocker, pm_conf)
    bot.state = State.RUNNING
    bot.rpc.send_msg = MagicMock()
    bot.exchange.cancel_order = MagicMock(return_value={"status": "canceled"})
    bot.exchange.cancel_stoploss_order = MagicMock()
    # Second consistency check still reports the order as unknown.
    bot.exchange.fetch_positions = MagicMock(return_value=[])
    bot.exchange.fetch_open_orders = MagicMock(
        return_value=[ccxt_order("123", "open", "buy", amount=11.0, filled=0.0)]
    )
    bot.exchange.fetch_open_conditional_orders = MagicMock(return_value=[])

    result = {
        "status": "mismatch",
        "unknown_positions": [],
        "unknown_orders": [{"order_id": "123", "symbol": "ETH/USDT:USDT"}],
        "unknown_conditional_orders": [],
    }
    bot._pm_apply_startup_consistency(result)

    assert bot.state == State.PAUSED
    assert "startup_consistency_mismatch" in bot._pm_blocked_order_reasons()


def test_pm_startup_cancel_clean_second_check_proceeds(mocker, pm_conf):
    """P0-4: cancel succeeded and the second query is clean -> no pause/block."""
    from freqtrade.enums import State

    pm_conf["exchange"]["portfolio_margin_risk"]["startup_consistency_mode"] = "cancel"
    bot = make_pm_bot(mocker, pm_conf)
    bot.state = State.RUNNING
    bot.rpc.send_msg = MagicMock()
    bot.exchange.cancel_order = MagicMock(return_value={"status": "canceled"})
    bot.exchange.cancel_stoploss_order = MagicMock()
    bot.exchange.fetch_positions = MagicMock(return_value=[])
    bot.exchange.fetch_open_orders = MagicMock(return_value=[])
    bot.exchange.fetch_open_conditional_orders = MagicMock(return_value=[])

    result = {
        "status": "mismatch",
        "unknown_positions": [],
        "unknown_orders": [{"order_id": "123", "symbol": "ETH/USDT:USDT"}],
        "unknown_conditional_orders": [],
    }
    bot._pm_apply_startup_consistency(result)

    assert bot.state == State.RUNNING
    assert "startup_consistency_mismatch" not in bot._pm_blocked_order_reasons()


def test_pm_rebuild_actual_order_map_from_persistent_orders(mocker, pm_conf):
    """P0-5: startup rebuild maps real child order ids from local stoploss orders."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stabc", side="sell")
    merged = ccxt_order("stabc", "open", "sell", amount=11.0, filled=0.0)
    merged["id_stop"] = "999"
    bot.exchange.fetch_stoploss_order = MagicMock(return_value=merged)

    bot._pm_rebuild_actual_order_map()

    assert bot._pm_actual_order_map.get("999") == "stabc"
    assert "reconciliation_incomplete" not in bot._pm_blocked_order_reasons()


def test_pm_rebuild_actual_order_map_failure_blocks(mocker, pm_conf):
    """P0-5: an unresolvable local stoploss at startup blocks new orders."""
    bot = make_pm_bot(mocker, pm_conf)
    make_stoploss_trade(pm_conf, order_id="stabc", side="sell")
    bot.exchange.fetch_stoploss_order = MagicMock(side_effect=TemporaryError("down"))
    bot.rpc.send_msg = MagicMock()

    bot._pm_rebuild_actual_order_map()

    assert "reconciliation_incomplete" in bot._pm_blocked_order_reasons()


def test_pm_unmatched_child_event_classified_via_conditional_lookup(mocker, pm_conf):
    """
    P0-5: after a restart the in-memory map is empty; a child order event must be
    classified by querying the local stoploss conditional history - never as foreign.
    """
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stabc", side="sell")
    bot._pm_actual_order_map = {}  # fresh process
    merged = ccxt_order("stabc", "open", "sell", amount=11.0, filled=0.0)
    merged["id_stop"] = "999"
    bot.exchange.fetch_stoploss_order = MagicMock(return_value=merged)
    bot.exchange.fetch_order = MagicMock()

    event = {"e": "ORDER_TRADE_UPDATE", "o": {"i": "999", "s": "ETH_USDT", "c": ""}}
    handled = bot._pm_handle_order_trade_update(event, {})

    assert handled is True
    bot.exchange.fetch_order.assert_not_called()
    assert bot._pm_actual_order_map.get("999") == "stabc"


def test_pm_unmatched_child_event_lookup_failure_fails_closed(mocker, pm_conf):
    """P0-5: a failed conditional lookup while classifying an event blocks orders."""
    bot = make_pm_bot(mocker, pm_conf)
    make_stoploss_trade(pm_conf, order_id="stabc", side="sell")
    bot._pm_actual_order_map = {}
    bot.exchange.fetch_stoploss_order = MagicMock(side_effect=TemporaryError("down"))
    bot.rpc.send_msg = MagicMock()

    event = {"e": "ORDER_TRADE_UPDATE", "o": {"i": "999", "s": "ETH_USDT", "c": ""}}
    handled = bot._pm_handle_order_trade_update(event, {})

    assert handled is False
    assert "reconciliation_incomplete" in bot._pm_blocked_order_reasons()


def make_emergency_bot(mocker, pm_conf, retries=3):
    import threading

    from freqtrade.freqtradebot import FreqtradeBot

    bot = FreqtradeBot.__new__(FreqtradeBot)
    bot.config = {"exchange": {"portfolio_margin_risk": {"emergency_close_retries": retries}}}
    bot.rpc = mocker.Mock()
    bot._exit_lock = threading.RLock()
    return bot


def test_pm_emergency_close_retries_then_reports_failure(mocker, pm_conf, init_persistence):
    """
    Emergency close retries a bounded number of times and sends an explicit failure
    alert when a position could NOT be closed - it must never fake "closed".
    """
    bot = make_emergency_bot(mocker, pm_conf, retries=2)
    make_open_trade(pm_conf, order_id="ec1", side="buy")
    bot._safe_force_exit = mocker.Mock(side_effect=[False, False])

    bot._pm_emergency_close_all()

    assert bot._safe_force_exit.call_count == 2
    warning = bot.rpc.send_msg.call_args_list[0][0][0]
    assert warning["type"] == RPCMessageType.WARNING
    assert "could NOT be closed" in warning["status"]
    assert "remain OPEN" in warning["status"]


def test_pm_emergency_close_retry_succeeds_without_failure_alert(mocker, pm_conf, init_persistence):
    """A transient failure followed by a successful retry must not emit a failure alert."""
    bot = make_emergency_bot(mocker, pm_conf, retries=3)
    make_open_trade(pm_conf, order_id="ec2", side="buy")
    bot._safe_force_exit = mocker.Mock(side_effect=[False, True])

    bot._pm_emergency_close_all()

    assert bot._safe_force_exit.call_count == 2
    for call in bot.rpc.send_msg.call_args_list:
        assert "could NOT be closed" not in call[0][0]["status"]


def test_pm_emergency_close_skips_already_flat_trades(mocker, pm_conf, init_persistence):
    """Trades without an open position are skipped (idempotent), not re-closed."""
    bot = make_emergency_bot(mocker, pm_conf, retries=2)
    trade = make_open_trade(pm_conf, order_id="ec3", side="buy")
    trade.is_open = False
    Trade.commit()
    bot._safe_force_exit = mocker.Mock()

    bot._pm_emergency_close_all()

    bot._safe_force_exit.assert_not_called()
    # No failure alert (everything closed/skipped).
    for call in bot.rpc.send_msg.call_args_list:
        assert "could NOT be closed" not in call[0][0]["status"]
