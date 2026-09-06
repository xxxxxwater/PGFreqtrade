"""Regression of the live DASH REST-fill -> delayed WS ownership incident."""

import json
import threading
from copy import deepcopy
from unittest.mock import MagicMock, PropertyMock

import pytest

from freqtrade.exceptions import OperationalException, TemporaryError
from freqtrade.persistence import Trade
from freqtrade.persistence.pm_outbox import PMOutbox
from freqtrade.persistence.pm_stream_journal import PMStreamJournal
from freqtrade.rpc import RPC
from tests.freqtradebot.test_pm_recovery import (
    ccxt_order,
    make_open_trade,
    make_pm_bot,
    make_stoploss_trade,
)
from tests.freqtradebot.test_pm_recovery import pm_conf as _pm_conf


pm_conf = _pm_conf


CLIENT = "ft13ff22a52bad4a7fb82c42020657"
ORDER_ID = "10660398840"


def event(order_id=ORDER_ID, client=CLIENT, status="FILLED", filled="11"):
    return {
        "e": "ORDER_TRADE_UPDATE",
        "fs": "UM",
        "o": {
            "s": "ETHUSDT",
            "i": order_id,
            "c": client,
            "X": status,
            "z": filled,
        },
    }


def filled_order(trade, *, closed_trade=False, filled=11.0, status="closed"):
    order = trade.orders[-1]
    order.ft_is_open = False
    order.status = status
    order.filled = filled
    order.remaining = 0
    trade.is_open = not closed_trade
    Trade.commit()
    return order


@pytest.mark.parametrize("closed_trade", [False, True])
@pytest.mark.parametrize(
    "status,filled", [("NEW", "0"), ("PARTIALLY_FILLED", "4"), ("FILLED", "11")]
)
def test_terminal_order_owned_after_rest_or_restart(mocker, pm_conf, closed_trade, status, filled):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade, closed_trade=closed_trade)
    bot.update_trade_state = MagicMock()
    bot.rpc.send_msg = MagicMock()
    # No transient index, no pending intent: the exact production incident.
    assert bot._pm_handle_order_trade_update(event(status=status, filled=filled), {}) is False
    bot.exchange.fetch_order.assert_not_called()
    bot.update_trade_state.assert_not_called()
    bot.rpc.send_msg.assert_not_called()
    assert "unmatched_stream_order" not in bot._pm_blocked_order_reasons()
    assert PMStreamJournal.get_unresolved() == []


def test_spot_first_does_not_shadow_contract(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    markets = bot.exchange.markets
    # Insertion order was the production bug: spot and UM share the raw id.
    spot = {"ETH/USDT": {"id": "ETHUSDT", "spot": True}}
    mocker.patch.object(
        type(bot.exchange), "markets", PropertyMock(return_value={**spot, **markets})
    )
    assert bot._pm_pair_from_exchange_symbol("ETHUSDT") == "ETH/USDT:USDT"


def test_order_id_collision_is_scoped_by_instrument(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    eth = make_open_trade(pm_conf, order_id=ORDER_ID)
    bot.exchange.markets["BTC/USDT:USDT"] = {
        "id": "BTCUSDT",
        "settle": "USDT",
        "inverse": False,
    }
    other_conf = deepcopy(pm_conf)
    other_conf["exchange"]["pair_whitelist"] = ["BTC/USDT:USDT"]
    btc = make_open_trade(other_conf, order_id=ORDER_ID)
    filled_order(eth)
    filled_order(btc)
    owned = bot._pm_owned_order(
        eth.pair, ORDER_ID, "", {(btc.pair, ORDER_ID): (btc, btc.orders[-1])}
    )
    assert owned[0].id == eth.id


def test_client_fallback_uses_retained_outbox(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    order = filled_order(trade)
    PMOutbox.session.add(
        PMOutbox(
            client_id=CLIENT,
            operation="order",
            payload=json.dumps({"pair": trade.pair}),
            state="RECONCILED",
            exchange_order_id=ORDER_ID,
            linked_order_id=ORDER_ID,
            linked_trade_id=trade.id,
        )
    )
    Trade.commit()
    owned = bot._pm_owned_order(trade.pair, "", CLIENT, {})
    assert owned[1].id == order.id
    with pytest.raises(OperationalException, match="order-id conflict"):
        bot._pm_owned_order(trade.pair, "some-other-id", CLIENT, {})


def test_unknown_owned_event_is_durable_and_alerted_once(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.rpc.send_msg = MagicMock()
    bot._pm_order_recovery = MagicMock()
    for _ in range(174):
        assert bot._pm_handle_order_trade_update(event(), {}) is False
    assert len(PMStreamJournal.get_unresolved()) == 1
    bot.rpc.send_msg.assert_called_once()
    bot._pm_order_recovery.assert_called_once()
    # Losing memory (process restart) must not release the admission gate.
    bot._pm_unmatched_stream_order_ids.clear()
    bot._pm_orders_blocked_reasons.clear()
    assert "unmatched_stream_order" in bot._pm_blocked_order_reasons()


def test_recover_cannot_clear_unknown_order_on_empty_open_scan(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot._pm_note_stream_incident("ETH/USDT:USDT", ORDER_ID, event()["o"], "unknown owner")
    report = RPC(bot)._rpc_pm_recover()
    assert report["stream_ownership"]["unresolved"] == 1
    assert "unmatched_stream_order" in report["orders_blocked_reasons"]


def test_scheduled_recovery_resolves_exact_terminal_order_without_manual_command(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    PMStreamJournal.record_unresolved(
        "ETH/USDT:USDT", ORDER_ID, CLIENT, event()["o"], "before link"
    )
    Trade.commit()
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade)
    bot.exchange.fetch_order.return_value = ccxt_order(ORDER_ID, "closed", "buy")
    bot.update_trade_state = MagicMock()
    bot._pm_block_orders("unmatched_stream_order")
    bot._pm_order_recovery()
    assert PMStreamJournal.get_unresolved() == []
    assert "unmatched_stream_order" not in bot._pm_blocked_order_reasons()
    bot.update_trade_state.assert_not_called()  # no duplicate fill/fee/notification


@pytest.mark.parametrize("response", [None, "wrong_instrument", "regressed_fill"])
def test_recovery_unverifiable_or_conflicting_rest_stays_blocked(mocker, pm_conf, response):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade)
    PMStreamJournal.record_unresolved(trade.pair, ORDER_ID, CLIENT, event()["o"], "uncertain")
    Trade.commit()
    if response is None:
        bot.exchange.fetch_order.side_effect = TemporaryError("read unavailable")
    else:
        result = ccxt_order(ORDER_ID, "closed", "buy")
        if response == "wrong_instrument":
            result["symbol"] = "BTC/USDT:USDT"
        else:
            result["filled"] = 1
        bot.exchange.fetch_order.return_value = result
    report = bot._pm_recover_stream_incidents()
    assert report["errors"]
    assert len(PMStreamJournal.get_unresolved()) == 1
    assert "unmatched_stream_order" in bot._pm_blocked_order_reasons()


def test_canceled_partial_order_new_fill_is_not_discarded(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade, filled=4, status="canceled")
    bot.exchange.fetch_order.return_value = ccxt_order(ORDER_ID, "closed", "buy")
    bot.update_trade_state = MagicMock()
    assert bot._pm_handle_order_trade_update(event(), {}) is True
    bot.exchange.fetch_order.assert_called_once_with(ORDER_ID, trade.pair)
    bot.update_trade_state.assert_called_once()


def test_rest_fills_before_queued_174_ws_events(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    bot.exchange.fetch_order.return_value = ccxt_order(ORDER_ID, "closed", "buy")
    report = bot._pm_reconcile_open_orders()
    assert report["errors"] == []
    assert trade.orders[-1].ft_is_open is False
    bot.exchange.fetch_order.reset_mock()
    bot.exchange.pop_pm_user_stream_events = MagicMock(return_value=[event() for _ in range(174)])
    bot._pm_risk_monitor = MagicMock()
    bot.rpc.send_msg = MagicMock()
    bot._pm_consume_user_stream_events()
    assert trade.is_open is True
    assert trade.amount == 11
    bot.exchange.fetch_order.assert_not_called()
    bot.rpc.send_msg.assert_not_called()
    assert "unmatched_stream_order" not in bot._pm_blocked_order_reasons()


def test_unknown_event_highwater_survives_out_of_order_new(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    for state, quantity in [("NEW", "0"), ("FILLED", "11"), ("NEW", "0")]:
        PMStreamJournal.record_unresolved(
            trade.pair, ORDER_ID, CLIENT, event(status=state, filled=quantity)["o"], "unknown"
        )
    Trade.commit()
    bot.exchange.fetch_order.return_value = ccxt_order(ORDER_ID, "open", "buy", filled=0)
    report = bot._pm_recover_stream_incidents()
    assert report["unresolved"] == 1
    assert "not caught up" in report["errors"][0]
    row = PMStreamJournal.get(trade.pair, ORDER_ID)
    assert row.max_cumulative_filled == 11
    assert row.saw_filled
    assert json.loads(row.order_data)["X"] == "NEW"  # first evidence retained


def test_update_failure_rolls_back_before_journaling(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.config["exchange"]["portfolio_margin_risk"]["user_stream_recover_unmatched_orders"] = False
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    bot.exchange.fetch_order.return_value = ccxt_order(ORDER_ID, "closed", "buy")

    def fail_after_partial_mutation(*args, **kwargs):
        trade.orders[-1].status = "closed"
        trade.orders[-1].ft_is_open = False
        trade.amount = 999
        raise RuntimeError("injected crash before local commit")

    bot.update_trade_state = MagicMock(side_effect=fail_after_partial_mutation)
    bot.exchange.pop_pm_user_stream_events = MagicMock(return_value=[event()])
    bot.rpc.send_msg = MagicMock()
    bot._pm_consume_user_stream_events()
    Trade.session.refresh(trade)
    assert trade.amount == 11
    assert trade.orders[-1].status == "open"
    assert len(PMStreamJournal.get_unresolved()) == 1


def test_child_alias_survives_closed_trade_and_restart(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stchild", side="sell")
    order = filled_order(trade, closed_trade=True)
    bot._pm_record_actual_order({"symbol": trade.pair, "id_stop": "999"}, "stchild", trade.pair)
    Trade.commit()
    bot._pm_actual_order_map.clear()
    bot.update_trade_state = MagicMock()
    assert bot._pm_handle_order_trade_update(event(order_id="999", client=""), {}) is False
    bot.update_trade_state.assert_not_called()
    assert PMStreamJournal.get(trade.pair, "999").linked_order_pk == order.id
    assert PMStreamJournal.get_unresolved() == []


def test_lifecycle_serialization_uses_same_reentrant_lock():
    from freqtrade.pm_order_ownership import pm_order_locked

    class Lifecycle:
        _exit_lock = threading.RLock()

        @pm_order_locked
        def inner(self):
            entered.set()

        @pm_order_locked
        def outer(self):
            self.inner()

    entered = threading.Event()
    lifecycle = Lifecycle()
    with lifecycle._exit_lock:
        worker = threading.Thread(target=lifecycle.outer)
        worker.start()
        assert not entered.wait(0.05)
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert entered.is_set()


def test_protection_creation_does_not_consult_entry_gate(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id=ORDER_ID)
    filled_order(trade)
    bot._pm_blocked_order_reasons = MagicMock(side_effect=AssertionError("entry-only gate"))
    bot.exchange.create_stoploss = MagicMock(
        return_value=ccxt_order("stprotect", "open", "sell", filled=0)
    )
    bot._pm_link_intent_for_order = MagicMock()
    assert bot.create_stoploss_order(trade, 0.009)
    bot.exchange.create_stoploss.assert_called_once()
