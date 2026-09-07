from unittest.mock import MagicMock

from freqtrade.enums import State
from tests.freqtradebot.test_pm_recovery import make_pm_bot


def _conf(default_conf_usdt):
    conf = default_conf_usdt.copy()
    conf["dry_run"] = False
    conf["trading_mode"] = "futures"
    conf["margin_mode"] = "cross"
    conf["stake_currency"] = "USDT"
    conf["cancel_open_orders_on_exit"] = False
    conf["exchange"] = conf["exchange"].copy()
    conf["exchange"]["name"] = "binance"
    conf["exchange"]["key"] = "dummy_key"
    conf["exchange"]["secret"] = "dummy_secret"
    conf["exchange"]["pair_whitelist"] = ["ETH/USDT:USDT"]
    conf["exchange"]["portfolio_margin"] = True
    conf["exchange"]["portfolio_margin_risk"] = {"user_stream_enabled": False}
    return conf


def test_process_stopped_runs_pm_control_plane_without_trading(mocker, default_conf_usdt):
    bot = make_pm_bot(mocker, _conf(default_conf_usdt))
    bot.state = State.STOPPED
    bot._pm_cold_start_reconcile = MagicMock()
    bot._pm_consume_user_stream_events = MagicMock()
    bot._pm_maintenance_schedule.run_pending = MagicMock()
    bot._schedule.run_pending = MagicMock()

    bot.process_stopped()
    bot.process_stopped()

    bot._pm_cold_start_reconcile.assert_called_once_with()
    assert bot._pm_consume_user_stream_events.call_count == 2
    bot._pm_consume_user_stream_events.assert_called_with(allow_account_risk_actions=False)
    assert bot._pm_maintenance_schedule.run_pending.call_count == 2
    bot._schedule.run_pending.assert_not_called()
    assert bot.state == State.STOPPED


def test_stopped_stream_drain_suppresses_account_risk_actions(mocker, default_conf_usdt):
    bot = make_pm_bot(mocker, _conf(default_conf_usdt))
    bot.exchange.pop_pm_user_stream_events = MagicMock(return_value=[{"e": "ACCOUNT_UPDATE"}])
    bot._pm_process_user_stream_event = MagicMock(return_value=(False, False, True))
    bot._pm_risk_monitor = MagicMock()

    bot._pm_consume_user_stream_events(allow_account_risk_actions=False)

    bot._pm_risk_monitor.assert_not_called()
