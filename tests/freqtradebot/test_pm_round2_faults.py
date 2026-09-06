"""Round-2 fault injection: the hidden paths behind the round-1 ownership fix.

These tests attack duplicate/restart/race/transaction/gate-release behavior that
the round-1 regression suite does not exercise:

1. the real PAPI outbox payload carries a raw "symbol" (DASHUSDT), never "pair" -
   the client-id fallback must still own the terminal order (production data);
2. the instrument_identity_unknown gate must auto-release, never stay permanent;
3. the full-account position quantity invariant blocks and auto-releases;
4. protection replacement must create+verify BEFORE retiring the old conditional;
5. a redelivery storm (10k events) must stay bounded (DB / journal / logs);
6. gate release is generation-safe across multiple incidents;
7. stream-journal retention keeps the evidence table bounded.
"""

import json
import time
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from freqtrade.exceptions import OperationalException, TemporaryError
from freqtrade.persistence import Order, Trade
from freqtrade.persistence.pm_outbox import PMOutbox
from freqtrade.persistence.pm_stream_journal import PMStreamJournal
from tests.freqtradebot.test_pm_order_ownership import (
    CLIENT,
    ORDER_ID,
    event,
    filled_order,
)
from tests.freqtradebot.test_pm_recovery import (
    ccxt_order,
    make_open_trade,
    make_pm_bot,
    make_stoploss_trade,
)
from tests.freqtradebot.test_pm_recovery import pm_conf as _pm_conf


pm_conf = _pm_conf

DASH_CLIENT = "ft13ff22a52bad4a7fb82c42020657"
DASH_ORDER_ID = "10660398840"


def make_pair_trade(conf, pair, order_id, amount=723.589):
    """A trade on an arbitrary pair (the live DASH fixture)."""
    open_rate = 69.18864
    trade = Trade(
        pair=pair,
        stake_currency=conf["stake_currency"],
        open_rate=open_rate,
        amount=amount,
        fee_open=0.001,
        fee_close=0.001,
        stake_amount=amount * open_rate,
        open_date=datetime.now(UTC),
        exchange="binance",
        is_open=True,
        is_short=False,
        leverage=1.0,
    )
    trade.orders = [
        Order(
            ft_order_side="buy",
            ft_pair=pair,
            ft_is_open=True,
            ft_amount=amount,
            ft_price=open_rate,
            order_id=order_id,
            status="open",
            symbol=pair,
            order_type="market",
            side="buy",
            price=open_rate,
            average=open_rate,
            filled=0.0,
            remaining=amount,
            cost=0.0,
            order_date=trade.open_date,
        )
    ]
    Trade.session.add(trade)
    Trade.commit()
    return trade


def real_outbox_payload(symbol="DASHUSDT", client=DASH_CLIENT):
    """The actual request body persisted by _pm_enqueue on the live path:
    raw symbol id, no "pair" key."""
    return {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quantity": 723.589,
        "newClientOrderId": client,
    }


def record_outbox(client_id, payload, order_id, trade_id, state="RECONCILED"):
    PMOutbox.session.add(
        PMOutbox(
            client_id=client_id,
            operation="order",
            payload=json.dumps(payload),
            state=state,
            exchange_order_id=order_id,
            linked_order_id=order_id,
            linked_trade_id=trade_id,
        )
    )
    Trade.commit()


# ---------------------------------------------------------------------------
# 1. Production-shape outbox payload must never turn a known order into an
#    incident (round-1 read json payload "pair" which live data never writes).
# ---------------------------------------------------------------------------
def test_real_papi_outbox_payload_still_owns_terminal_dash_order(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.exchange.markets["DASH/USDT:USDT"] = {
        "id": "DASHUSDT",
        "settle": "USDT",
        "inverse": False,
    }
    # Production incident identity: Trade #2. A filler trade occupies id 1.
    make_open_trade(pm_conf, order_id="1")
    trade = make_pair_trade(pm_conf, "DASH/USDT:USDT", DASH_ORDER_ID)
    assert trade.id == 2
    order = filled_order(trade, filled=723.589)
    record_outbox(DASH_CLIENT, real_outbox_payload(), DASH_ORDER_ID, trade.id)

    bot.rpc.send_msg = MagicMock()
    dash_event = event(
        order_id=DASH_ORDER_ID, client=DASH_CLIENT, status="FILLED", filled="723.589"
    )
    dash_event["o"]["s"] = "DASHUSDT"

    assert bot._pm_handle_order_trade_update(dash_event, {}) is False
    bot.rpc.send_msg.assert_not_called()
    assert "unmatched_stream_order" not in bot._pm_blocked_order_reasons()
    assert PMStreamJournal.get_unresolved() == []
    # Ownership resolves to the exact production trade/order.
    owned = bot._pm_owned_order(trade.pair, DASH_ORDER_ID, DASH_CLIENT, {})
    assert owned[0].id == trade.id and owned[1].id == order.id


def test_legacy_pair_key_payload_still_owns(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade)
    record_outbox(CLIENT, {"pair": trade.pair}, ORDER_ID, trade.id)
    owned = bot._pm_owned_order(trade.pair, "", CLIENT, {})
    assert str(owned[1].order_id) == ORDER_ID


def test_conflicting_symbol_payload_is_rejected(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade)
    record_outbox(CLIENT, {"symbol": "BTCUSDT"}, ORDER_ID, trade.id)
    bot.exchange.markets["BTC/USDT:USDT"] = {
        "id": "BTCUSDT",
        "settle": "USDT",
        "inverse": False,
    }
    with pytest.raises(OperationalException, match="instrument conflict"):
        bot._pm_owned_order(trade.pair, "", CLIENT, {})


def test_unresolvable_symbol_payload_defers_to_linked_order(mocker, pm_conf):
    """A payload whose symbol cannot be resolved (delisted contract) must still
    be owned through the linked local Order rows - never fail closed."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade)
    record_outbox(CLIENT, {"symbol": "GONEUSDT"}, ORDER_ID, trade.id)
    owned = bot._pm_owned_order(trade.pair, "", CLIENT, {})
    assert str(owned[1].order_id) == ORDER_ID


# ---------------------------------------------------------------------------
# 2. instrument_identity_unknown is not a permanent gate.
# ---------------------------------------------------------------------------
def test_unknown_instrument_gate_stays_until_evidence_reprocessed(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.rpc.send_msg = MagicMock()
    bot._pm_order_recovery = MagicMock()
    mystery = event()
    mystery["o"]["s"] = "NEWLISTUSDT"
    assert bot._pm_handle_order_trade_update(mystery, {}) is False
    assert "instrument_identity_unknown" in bot._pm_blocked_order_reasons()
    assert "NEWLISTUSDT" in bot._pm_unresolved_instrument_ids

    # Still unresolvable: retry keeps the gate.
    assert bot._pm_retry_unresolved_instruments() == 1
    assert "instrument_identity_unknown" in bot._pm_blocked_order_reasons()

    # The market appears but the ORDER is still unexplained: the original event
    # is re-dispatched, creates a durable journal incident, and the identity
    # gate STAYS (canonicalization alone is never sufficient evidence).
    bot.exchange.markets["NEWLIST/USDT:USDT"] = {
        "id": "NEWLISTUSDT",
        "settle": "USDT",
        "inverse": False,
    }
    assert bot._pm_retry_unresolved_instruments() == 1
    assert "instrument_identity_unknown" in bot._pm_blocked_order_reasons()
    assert "unmatched_stream_order" in bot._pm_blocked_order_reasons()
    assert PMStreamJournal.get("NEWLIST/USDT:USDT", ORDER_ID).unresolved


def test_instrument_gate_releases_when_event_owned_and_account_clean(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.rpc.send_msg = MagicMock()
    bot._pm_order_recovery = MagicMock()
    mystery = event()
    mystery["o"]["s"] = "NEWLISTUSDT"
    assert bot._pm_handle_order_trade_update(mystery, {}) is False
    assert "instrument_identity_unknown" in bot._pm_blocked_order_reasons()

    # The market appears AND the order gains proven local ownership: the
    # re-dispatched event is a known terminal replay, no incident remains and
    # the account quantity invariant is clean -> the gate auto-releases.
    bot.exchange.markets["NEWLIST/USDT:USDT"] = {
        "id": "NEWLISTUSDT",
        "settle": "USDT",
        "inverse": False,
    }
    trade = make_pair_trade(pm_conf, "NEWLIST/USDT:USDT", ORDER_ID)
    filled_order(trade)
    assert bot._pm_retry_unresolved_instruments() == 0
    assert "instrument_identity_unknown" not in bot._pm_blocked_order_reasons()
    assert PMStreamJournal.get_unresolved() == []


# ---------------------------------------------------------------------------
# 3. Full-account position quantity invariant (P0-1).
# ---------------------------------------------------------------------------
def test_position_quantity_mismatch_blocks_and_auto_releases(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.rpc.send_msg = MagicMock()
    trade = make_open_trade(pm_conf, order_id=ORDER_ID, amount=11.0)
    filled_order(trade)
    bot.exchange.fetch_positions = MagicMock(
        return_value=[{"symbol": trade.pair, "side": "long", "contracts": 5.0}]
    )

    report = bot._pm_reconcile_open_orders()
    assert report["position_mismatches"], report
    assert report["success"] is False
    assert "position_quantity_mismatch" in bot._pm_blocked_order_reasons()
    bot.rpc.send_msg.assert_called_once()  # one SAFE_HOLD alert, not per run

    # A second run does not re-alert.
    bot._pm_reconcile_open_orders()
    bot.rpc.send_msg.assert_called_once()

    # Quantities reconcile -> the gate auto-releases.
    bot.exchange.fetch_positions = MagicMock(
        return_value=[{"symbol": trade.pair, "side": "long", "contracts": 11.0}]
    )
    report = bot._pm_reconcile_open_orders()
    assert report["position_mismatches"] == []
    assert "position_quantity_mismatch" not in bot._pm_blocked_order_reasons()


def test_position_mismatch_keeps_reduce_only_path_open(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID, amount=11.0)
    filled_order(trade)
    bot.exchange.fetch_positions = MagicMock(
        return_value=[{"symbol": trade.pair, "side": "long", "contracts": 3.0}]
    )
    bot._pm_reconcile_open_orders()
    assert "position_quantity_mismatch" in bot._pm_blocked_order_reasons()
    # Protective stoploss creation must not consult the entry gate.
    bot.exchange.create_stoploss = MagicMock(
        return_value=ccxt_order("stprotect", "open", "sell", filled=0)
    )
    bot._pm_link_intent_for_order = MagicMock()
    assert bot.create_stoploss_order(trade, 0.009)
    bot.exchange.create_stoploss.assert_called_once()


def test_in_flight_bounded_interval_allows_explainable_drift(mocker, pm_conf):
    """Open orders allow a BOUNDED interval, never a blanket exemption."""
    bot = make_pm_bot(mocker, pm_conf)
    # Entry order partially working: local 11, remaining 5 risk-increasing
    # -> exchange in [11, 16] is explainable; 999 is not.
    trade = make_open_trade(pm_conf, order_id=ORDER_ID, amount=11.0)
    order = trade.open_orders[0]
    order.amount = 11.0
    order.filled = 6.0
    order.remaining = 5.0
    Trade.commit()
    bot.exchange.fetch_positions = MagicMock(
        return_value=[{"symbol": trade.pair, "side": "long", "contracts": 13.0}]
    )
    report = bot._pm_reconcile_open_orders()
    assert report["position_mismatches"] == []
    assert "position_quantity_mismatch" not in bot._pm_blocked_order_reasons()

    bot.exchange.fetch_positions = MagicMock(
        return_value=[{"symbol": trade.pair, "side": "long", "contracts": 999.0}]
    )
    report = bot._pm_reconcile_open_orders()
    assert report["position_mismatches"]
    assert "position_quantity_mismatch" in bot._pm_blocked_order_reasons()

    # Exit order working: remaining 3 risk-reducing -> exchange in [8, 11].
    bot._pm_unblock_orders("position_quantity_mismatch")
    order.side = trade.exit_side
    order.filled = 8.0
    order.remaining = 3.0
    Trade.commit()
    bot.exchange.fetch_positions = MagicMock(
        return_value=[{"symbol": trade.pair, "side": "long", "contracts": 9.0}]
    )
    report = bot._pm_reconcile_open_orders()
    assert report["position_mismatches"] == []
    assert "position_quantity_mismatch" not in bot._pm_blocked_order_reasons()


def test_inflight_exemption_expires_after_deadline(mocker, pm_conf):
    """A permanently working order must never mask a real quantity mismatch."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID, amount=11.0)
    order = trade.open_orders[0]
    order.amount = 11.0
    order.filled = 6.0
    order.remaining = 5.0
    order.order_date = datetime.now(UTC) - timedelta(hours=2)
    Trade.commit()
    bot.exchange.fetch_positions = MagicMock(
        return_value=[{"symbol": trade.pair, "side": "long", "contracts": 13.0}]
    )
    report = bot._pm_reconcile_open_orders()
    assert report["position_mismatches"]
    assert "position_quantity_mismatch" in bot._pm_blocked_order_reasons()


# ---------------------------------------------------------------------------
# 4. Stop-protection invariant + PM-safe replacement switch (P0-2).
# ---------------------------------------------------------------------------
def test_stop_protection_missing_blocks_and_auto_heals(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.rpc.send_msg = MagicMock()
    bot.strategy.order_types["stoploss_on_exchange"] = True
    trade = make_open_trade(pm_conf, order_id=ORDER_ID, amount=11.0)
    filled_order(trade)
    bot.exchange.fetch_open_conditional_orders.return_value = []

    report = bot._pm_reconcile_open_orders()
    assert report["unprotected_positions"]
    assert "stop_protection_missing" in bot._pm_blocked_order_reasons()
    bot.rpc.send_msg.assert_called_once()

    # The regular loop recreates the protection: next reconcile auto-heals.
    trade.orders.append(
        Order(
            ft_order_side="stoploss",
            ft_pair=trade.pair,
            ft_is_open=True,
            ft_amount=trade.amount,
            ft_price=0.009,
            order_id="stguard",
            status="open",
            symbol=trade.pair,
            order_type="stoploss",
            side=trade.exit_side,
            price=0.009,
            average=0.009,
            filled=0.0,
            remaining=trade.amount,
            cost=0.0,
            order_date=trade.open_date,
        )
    )
    Trade.commit()
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=ccxt_order("stguard", "open", "sell", filled=0)
    )
    bot.exchange.fetch_open_conditional_orders.return_value = [
        {
            "id": "stguard",
            "clientAlgoId": "stguard",
            "symbol": trade.pair,
            "status": "open",
            "side": trade.exit_side,
            "amount": trade.amount,
            "info": {"reduceOnly": True},
        }
    ]
    report = bot._pm_reconcile_open_orders()
    assert report["unprotected_positions"] == []
    assert "stop_protection_missing" not in bot._pm_blocked_order_reasons()


def test_trailing_stop_switch_creates_and_verifies_before_cancel(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stold", side="sell")
    trade.open_sl_orders[0].order_date = datetime.now(UTC) - timedelta(seconds=120)
    assert trade.open_sl_orders[0].order_id == "stold"
    bot.exchange.stoploss_adjust = MagicMock(return_value=True)
    bot.exchange.create_stoploss = MagicMock(
        return_value=ccxt_order("stnew", "open", "sell", filled=0)
    )
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=ccxt_order("stnew", "open", "sell", filled=0)
    )
    bot.exchange.cancel_stoploss_order_with_result = MagicMock(
        return_value=ccxt_order("stold", "canceled", "sell", filled=0)
    )
    bot._pm_link_intent_for_order = MagicMock()

    def close_old(trade_, order_id, action_order, **kwargs):
        for sl in trade_.open_sl_orders:
            if str(sl.order_id) == str(order_id):
                sl.ft_is_open = False
                sl.status = "canceled"
        return True

    bot.update_trade_state = MagicMock(side_effect=close_old)

    old_ccxt = ccxt_order("stold", "open", "sell", filled=0)
    bot.handle_trailing_stoploss_on_exchange(trade, old_ccxt)

    # Create+verify happen before the old conditional is retired.
    bot.exchange.cancel_stoploss_order_with_result.assert_called_once_with(
        "stold", trade.pair, trade.amount
    )
    bot.exchange.fetch_stoploss_order.assert_called_once_with("stnew", trade.pair)
    bot.update_trade_state.assert_called_once()
    assert bot.update_trade_state.call_args.args[1] == "stold"
    assert {sl.order_id for sl in trade.open_sl_orders} == {"stnew"}


def test_trailing_stop_switch_keeps_old_protection_when_create_fails(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stold", side="sell")
    trade.open_sl_orders[0].order_date = datetime.now(UTC) - timedelta(seconds=120)
    bot.exchange.stoploss_adjust = MagicMock(return_value=True)
    bot.exchange.create_stoploss = MagicMock(side_effect=TemporaryError("rejected"))
    bot.exchange.cancel_stoploss_order_with_result = MagicMock()

    old_ccxt = ccxt_order("stold", "open", "sell", filled=0)
    bot.handle_trailing_stoploss_on_exchange(trade, old_ccxt)

    # The old protection stays: no cancel was ever issued.
    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()
    assert {sl.order_id for sl in trade.open_sl_orders} == {"stold"}


def test_trailing_stop_switch_keeps_old_when_new_not_verified(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stold", side="sell")
    trade.open_sl_orders[0].order_date = datetime.now(UTC) - timedelta(seconds=120)
    bot.exchange.stoploss_adjust = MagicMock(return_value=True)
    bot.exchange.create_stoploss = MagicMock(
        return_value=ccxt_order("stnew", "open", "sell", filled=0)
    )
    bot.exchange.fetch_stoploss_order = MagicMock(side_effect=TemporaryError("lookup down"))
    bot.exchange.cancel_stoploss_order_with_result = MagicMock()

    old_ccxt = ccxt_order("stold", "open", "sell", filled=0)
    bot.handle_trailing_stoploss_on_exchange(trade, old_ccxt)

    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()


# ---------------------------------------------------------------------------
# 5. Recovery is not amplified by an unknown-order burst.
# ---------------------------------------------------------------------------
def test_unknown_burst_does_not_amplify_full_recovery(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.rpc.send_msg = MagicMock()
    bot._pm_order_recovery = MagicMock()
    targeted = mocker.spy(bot, "_pm_recover_stream_incidents")

    for idx in range(5):
        ev = event(order_id=f"unknown{idx}", client=f"ftunknown{idx}", status="NEW", filled="0")
        assert bot._pm_handle_order_trade_update(ev, {}) is False

    # Targeted incident recovery runs per first-event; the FULL sweep is
    # rate-limited to one per cooldown window.
    assert targeted.call_count == 5
    assert bot._pm_order_recovery.call_count == 1
    assert len(PMStreamJournal.get_unresolved()) == 5


def test_auto_recovery_cooldown_config_controls_full_sweep(mocker, pm_conf):
    conf = deepcopy(pm_conf)
    conf["exchange"]["portfolio_margin_risk"]["user_stream_auto_recovery_cooldown_seconds"] = 0
    bot = make_pm_bot(mocker, conf)
    bot.rpc.send_msg = MagicMock()
    bot._pm_order_recovery = MagicMock()
    for idx in range(3):
        ev = event(order_id=f"u{idx}", client=f"ftu{idx}", status="NEW", filled="0")
        bot._pm_handle_order_trade_update(ev, {})
    assert bot._pm_order_recovery.call_count == 3


def test_ownership_cache_invalidated_on_new_incident(mocker, pm_conf):
    """A new incident for a previously clean terminal order must invalidate the
    per-batch ownership cache (keyed with the client id) - never serve stale."""
    bot = make_pm_bot(mocker, pm_conf)
    bot.config["exchange"]["portfolio_margin_risk"]["user_stream_recover_unmatched_orders"] = False
    bot.rpc.send_msg = MagicMock()
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade)
    owned = bot._pm_owned_order(trade.pair, ORDER_ID, CLIENT, {})
    assert owned is not None
    assert (trade.pair, ORDER_ID, CLIENT) in bot._pm_owned_resolve_cache

    bot._pm_note_stream_incident(trade.pair, ORDER_ID, event()["o"], "reopened")

    assert not any(
        key[0] == trade.pair and key[1] == ORDER_ID
        for key in bot._pm_owned_resolve_cache
    )
    assert (trade.pair, ORDER_ID) not in bot._pm_clean_terminal_ids


# ---------------------------------------------------------------------------
# 6. Generation-safe gate release across multiple incidents (P0-6 ABA).
# ---------------------------------------------------------------------------
def test_gate_release_is_scoped_to_all_incidents(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    # Incident A is owned (resolvable), incident B is genuinely unknown.
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade)
    PMStreamJournal.record_unresolved(trade.pair, ORDER_ID, CLIENT, event()["o"], "late")
    PMStreamJournal.record_unresolved(
        trade.pair, "unknownB", "", event(order_id="unknownB")["o"], "unknown"
    )
    Trade.commit()
    bot.exchange.fetch_order.return_value = ccxt_order(ORDER_ID, "closed", "buy")
    bot.update_trade_state = MagicMock()
    bot._pm_block_orders("unmatched_stream_order")

    report = bot._pm_recover_stream_incidents()
    # A resolved, B did not: the gate MUST stay up (B's evidence is not cleared).
    assert report["unresolved"] >= 1
    assert "unmatched_stream_order" in bot._pm_blocked_order_reasons()
    assert PMStreamJournal.get(trade.pair, "unknownB").unresolved

    # Now B becomes owned too: recovery resolves everything and releases.
    b_trade = make_open_trade(deepcopy(pm_conf), order_id="unknownB")
    filled_order(b_trade)
    bot.exchange.fetch_order.return_value = ccxt_order("unknownB", "closed", "buy")
    report = bot._pm_recover_stream_incidents()
    assert report["unresolved"] == 0
    assert "unmatched_stream_order" not in bot._pm_blocked_order_reasons()


def test_new_incident_after_release_reblocks(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.rpc.send_msg = MagicMock()
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade)
    PMStreamJournal.record_unresolved(trade.pair, ORDER_ID, CLIENT, event()["o"], "late")
    Trade.commit()
    bot.exchange.fetch_order.return_value = ccxt_order(ORDER_ID, "closed", "buy")
    bot.update_trade_state = MagicMock()
    bot._pm_block_orders("unmatched_stream_order")
    assert bot._pm_recover_stream_incidents()["unresolved"] == 0
    assert "unmatched_stream_order" not in bot._pm_blocked_order_reasons()

    # A genuinely new unknown event re-opens the gate.
    ev = event(order_id="brandnew", client="ftbrandnew", status="NEW", filled="0")
    assert bot._pm_handle_order_trade_update(ev, {}) is False
    assert "unmatched_stream_order" in bot._pm_blocked_order_reasons()


# ---------------------------------------------------------------------------
# 7. A 10,000-event redelivery storm stays bounded.
# ---------------------------------------------------------------------------
def test_ten_thousand_replay_storm_is_bounded(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade)
    record_outbox(
        CLIENT, real_outbox_payload(symbol="ETHUSDT", client=CLIENT), ORDER_ID, trade.id
    )
    bot.exchange.fetch_order.return_value = ccxt_order(ORDER_ID, "closed", "buy")
    bot._pm_risk_monitor = MagicMock()
    bot.rpc.send_msg = MagicMock()
    journal_get = mocker.spy(PMStreamJournal, "get")

    bot.exchange.pop_pm_user_stream_events = MagicMock(
        return_value=[event() for _ in range(10_000)]
    )
    bot._pm_consume_user_stream_events()

    bot.exchange.fetch_order.assert_not_called()
    bot.rpc.send_msg.assert_not_called()
    assert PMStreamJournal.get_unresolved() == []
    assert "unmatched_stream_order" not in bot._pm_blocked_order_reasons()
    # Per-batch caches collapse the storm: ownership + incident + finish lookups
    # run a small constant number of times (not 10k times).
    assert journal_get.call_count <= 3
    assert len(bot._pm_owned_resolve_cache) == 1
    assert (trade.pair, ORDER_ID) in bot._pm_clean_terminal_ids
    assert len(bot._pm_clean_terminal_ids) == 1


# ---------------------------------------------------------------------------
# 8. Stream-journal retention keeps the evidence table bounded.
# ---------------------------------------------------------------------------
def test_journal_purge_keeps_recent_and_unresolved(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    order = filled_order(make_open_trade(pm_conf, order_id=ORDER_ID))
    pair = "DASH/USDT:USDT"
    # Plain-evidence rows: their exchange ids match local Order rows, so they
    # are purgeable after retention. Aliases (ids absent from the orders table)
    # are never purged.
    for key in ("stale", "fresh"):
        filled_order(make_open_trade(deepcopy(pm_conf), order_id=key))
    stale_row, _ = PMStreamJournal.record_unresolved(
        pair, "stale", "", event(order_id="stale")["o"], "x"
    )
    PMStreamJournal.resolve(pair, "stale", order.id)
    PMStreamJournal.record_unresolved(pair, "fresh", "", event(order_id="fresh")["o"], "x")
    PMStreamJournal.resolve(pair, "fresh", order.id)
    PMStreamJournal.record_unresolved(pair, "pending", "", event(order_id="pending")["o"], "x")
    Trade.commit()
    stale_row.updated_at = datetime.now(UTC) - timedelta(days=60)
    Trade.commit()

    bot._pm_last_journal_purge_at = time.monotonic() - 3700
    purged = bot._pm_purge_resolved_journal_if_due()
    Trade.commit()

    assert purged == 1
    assert PMStreamJournal.get(pair, "stale") is None
    assert PMStreamJournal.get(pair, "fresh") is not None
    assert PMStreamJournal.get(pair, "pending").unresolved

    # The purge is rate-limited: no second run inside the hour window.
    assert bot._pm_purge_resolved_journal_if_due() == 0


def test_purge_preserves_durable_child_alias_across_restart(mocker, pm_conf):
    """A child/parent ownership alias must survive the incident purge and still
    resolve the child order after a process restart."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stparent", side="sell")
    order = filled_order(trade, closed_trade=True)
    PMStreamJournal.record_unresolved(
        trade.pair, "child999", "", {"i": "child999"}, "conditional child ownership alias"
    )
    PMStreamJournal.resolve(trade.pair, "child999", order.id)
    Trade.commit()
    alias_row = PMStreamJournal.get(trade.pair, "child999")
    alias_row.updated_at = datetime.now(UTC) - timedelta(days=60)
    Trade.commit()

    bot._pm_last_journal_purge_at = time.monotonic() - 3700
    assert bot._pm_purge_resolved_journal_if_due() == 0  # alias never purged
    Trade.commit()
    assert PMStreamJournal.get(trade.pair, "child999") is not None

    # Restart: fresh in-memory caches; the child event still resolves ownership.
    bot._pm_actual_order_map.clear()
    bot._pm_owned_resolve_cache.clear()
    bot._pm_clean_terminal_ids.clear()
    bot.update_trade_state = MagicMock()
    ev = event(order_id="child999", client="", status="FILLED", filled="11")
    assert bot._pm_handle_order_trade_update(ev, {}) is False
    bot.update_trade_state.assert_not_called()
    assert PMStreamJournal.get_unresolved() == []
