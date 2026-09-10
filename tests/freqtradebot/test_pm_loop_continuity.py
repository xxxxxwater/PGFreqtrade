"""Recovery gates and cooperative market scheduling, without changing strategy rules."""
from unittest.mock import MagicMock
from threading import RLock

import pytest

from freqtrade.enums import State
from freqtrade.exceptions import TemporaryError
from freqtrade.persistence import PMOrderIntent, Trade
from freqtrade.persistence.pm_stream_journal import PMStreamJournal
from tests.freqtradebot.test_pm_unattended_gap_closure import pm_conf, pm_db
from tests.freqtradebot.test_pm_recovery import make_pm_bot, make_stoploss_trade


@pytest.mark.parametrize("unresolved", [False, True])
def test_store_failure_then_success_releases_only_infrastructure(mocker, pm_conf, unresolved):
    bot = make_pm_bot(mocker, pm_conf)
    bot.state = State.PAUSED
    bot._pm_block_orders("stop_protection_missing")
    mocker.patch.object(PMStreamJournal, "get_unresolved", return_value=[])
    query = mocker.patch.object(PMOrderIntent, "has_unresolved", side_effect=RuntimeError("db down"))
    bot._pm_block_orders("intent_store_unavailable")
    assert "intent_store_unavailable" in bot._pm_blocked_order_reasons()
    query.side_effect = None
    query.return_value = unresolved
    reasons = bot._pm_blocked_order_reasons()
    assert "intent_store_unavailable" not in reasons
    assert "intent_store_unavailable" not in bot._pm_orders_blocked_reasons
    assert ("unresolved_intent" in reasons) == unresolved
    assert "stop_protection_missing" in reasons
    assert bot.state == State.PAUSED


def test_journal_failure_does_not_clear_store_gate(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot._pm_block_orders("intent_store_unavailable")
    mocker.patch.object(PMStreamJournal, "get_unresolved", side_effect=RuntimeError("db down"))
    assert "intent_store_unavailable" in bot._pm_blocked_order_reasons()


def market_bot(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.exchange.reload_markets = MagicMock()
    bot.update_trades_without_assigned_fees = MagicMock()
    bot._refresh_active_whitelist = MagicMock(return_value=["ETH/USDT:USDT", "BTC/USDT:USDT"])
    bot.dataprovider.refresh = MagicMock()
    bot.strategy.bot_loop_start = MagicMock()
    bot.strategy.analyze = MagicMock()
    bot._measure_execution = RLock()
    mocker.patch.object(Trade, "get_open_trades", return_value=[])
    return bot


@pytest.mark.parametrize("stage,ticks", [
    ("markets", [0, 13]),
    ("whitelist", [0, 1, 2, 13]),
    ("candles", [0, 1, 2, 3, 13]),
    ("analyze", [0, 1, 2, 3, 4, 5, 13]),
])
def test_budget_yields_before_optional_work(mocker, pm_conf, stage, ticks):
    bot = market_bot(mocker, pm_conf)
    mocker.patch("freqtrade.freqtradebot._time.monotonic", side_effect=ticks)
    bot._pm_refresh_market_with_budget(12)
    assert f"stage={stage}" in bot._pm_cycle_exposure_block_reason
    if stage != "analyze":
        bot.strategy.analyze.assert_not_called()
    else:
        assert bot.strategy.analyze.call_count == 1
    if stage in {"markets", "whitelist"}:
        bot.dataprovider.refresh.assert_not_called()


def test_refresh_failure_never_leaves_exposure_ready(mocker, pm_conf):
    bot = market_bot(mocker, pm_conf)
    bot.dataprovider.refresh.side_effect = RuntimeError("network error")
    with pytest.raises(RuntimeError):
        bot._pm_refresh_market_with_budget(12)
    assert bot._pm_cycle_exposure_block_reason == "market_refresh_failed"


def test_refresh_blocks_inflight_then_releases_on_complete(mocker, pm_conf):
    bot = market_bot(mocker, pm_conf)
    observed = []
    bot.strategy.analyze.side_effect = lambda pairs: observed.append(
        bot._pm_cycle_exposure_block_reason
    )
    bot._pm_refresh_market_with_budget(12)
    assert observed == ["market_refresh_in_progress"] * 2
    assert bot._pm_cycle_exposure_block_reason is None


def test_protection_checkpoint_services_only_bot_trades(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="owned", side="sell")
    bot.strategy.order_types["stoploss_on_exchange"] = True
    bot.manage_open_orders = MagicMock()
    bot.handle_stoploss_on_exchange = MagicMock()
    mocker.patch.object(Trade, "get_open_trades", return_value=[trade])
    bot._pm_service_protection_before_market()
    bot.manage_open_orders.assert_called_once()
    bot.handle_stoploss_on_exchange.assert_called_once_with(trade)


def test_process_services_protection_before_slow_market_and_still_exits(mocker, pm_conf):
    bot = market_bot(mocker, pm_conf)
    bot.config["exchange"]["portfolio_margin_risk"]["market_analysis_budget_seconds"] = 12
    events = []
    bot._pm_consume_user_stream_events = MagicMock()
    bot._pm_maintenance_schedule.run_pending = MagicMock()
    bot._schedule.run_pending = MagicMock()
    bot._pm_service_protection_before_market = MagicMock(
        side_effect=lambda: events.append("protection")
    )
    def slow_market(budget):
        events.append("market")
        bot._pm_cycle_exposure_block_reason = "market_analysis_budget_exceeded"
    bot._pm_refresh_market_with_budget = MagicMock(side_effect=slow_market)
    bot.manage_open_orders = MagicMock()
    bot.exit_positions = MagicMock(side_effect=lambda trades: events.append("exit"))
    bot.process_open_trade_positions = MagicMock()
    bot.get_free_open_trades = MagicMock(return_value=False)
    bot._pm_record_entry_block_for_pairs = MagicMock()
    bot.rpc.process_msg_queue = MagicMock()
    bot.process()
    assert events == ["protection", "market", "exit"]
    assert bot._pm_cycle_exposure_block_reason == "market_analysis_budget_exceeded"


def test_process_transient_market_failure_still_runs_exits_and_skips_new_entry_scan(mocker, pm_conf):
    bot = market_bot(mocker, pm_conf)
    bot.config["exchange"]["portfolio_margin_risk"]["market_analysis_budget_seconds"] = 12
    bot.strategy.position_adjustment_enable = True
    events = []
    bot._pm_consume_user_stream_events = MagicMock()
    bot._pm_maintenance_schedule.run_pending = MagicMock()
    bot._schedule.run_pending = MagicMock()
    bot._pm_service_protection_before_market = MagicMock(
        side_effect=lambda: events.append("protection")
    )
    bot._refresh_active_whitelist.side_effect = TemporaryError("symbolConfig network disconnect")
    bot.manage_open_orders = MagicMock()
    bot.exit_positions = MagicMock(side_effect=lambda trades: events.append("exit"))
    bot.process_open_trade_positions = MagicMock(side_effect=lambda: events.append("dca"))
    bot.enter_positions = MagicMock(side_effect=lambda: events.append("entry"))
    bot._pm_record_entry_block_for_pairs = MagicMock()
    bot.rpc.process_msg_queue = MagicMock()

    bot.process()

    assert events == ["protection", "protection", "exit", "dca"]
    assert bot._pm_cycle_exposure_block_reason == "market_refresh_failed"
    bot.enter_positions.assert_not_called()
    bot._pm_record_entry_block_for_pairs.assert_called_once_with(
        bot.active_pair_whitelist, "market_refresh_failed"
    )
