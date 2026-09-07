from unittest.mock import MagicMock

from freqtrade.enums import State
from tests.freqtradebot.test_pm_recovery import make_pm_bot


def _conf(default_conf_usdt, *, consistency_mode="report"):
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
    conf["exchange"]["portfolio_margin_risk"] = {
        "user_stream_enabled": False,
        "startup_consistency_mode": consistency_mode,
    }
    return conf


def test_process_stopped_uses_explicit_stopped_recovery_scope(mocker, default_conf_usdt):
    bot = make_pm_bot(mocker, _conf(default_conf_usdt))
    bot.state = State.STOPPED
    bot._pm_cold_start_reconcile = MagicMock()
    bot._pm_consume_user_stream_events = MagicMock()
    bot._pm_maintenance_schedule.run_pending = MagicMock()
    bot._schedule.run_pending = MagicMock()

    bot.process_stopped()
    bot.process_stopped()

    bot._pm_cold_start_reconcile.assert_called_once_with(stopped_mode=True)
    bot._pm_consume_user_stream_events.assert_called_with(allow_account_risk_actions=False)
    bot._schedule.run_pending.assert_not_called()
    assert bot.state == State.STOPPED


def test_stopped_intent_store_failure_never_changes_operator_stopped(mocker, default_conf_usdt):
    bot = make_pm_bot(mocker, _conf(default_conf_usdt))
    bot.state = State.STOPPED
    bot.exchange.pm_drain_outbox = MagicMock(return_value={"errors": []})
    bot.exchange.list_pm_pending_intents = MagicMock(side_effect=RuntimeError("db down"))

    report = bot._pm_recover_pending_intents(
        allow_exposure_increasing_relay=False,
        preserve_operator_stop=True,
    )

    assert report["store_error"] is not None
    assert bot.state == State.STOPPED
    bot.exchange.pm_drain_outbox.assert_called_once_with(allow_exposure_increasing=False)


def test_stopped_cancel_consistency_mode_is_report_only_no_exchange_mutation(
    mocker, default_conf_usdt
):
    bot = make_pm_bot(mocker, _conf(default_conf_usdt, consistency_mode="cancel"))
    bot.state = State.STOPPED
    bot.exchange.cancel_order = MagicMock()
    bot.exchange.cancel_stoploss_order = MagicMock()
    result = {
        "status": "mismatch",
        "unknown_positions": [],
        "unknown_orders": [{"order_id": "manual-normal", "symbol": "ETH/USDT:USDT"}],
        "unknown_conditional_orders": [
            {"order_id": "manual-conditional", "symbol": "ETH/USDT:USDT"}
        ],
        "local_open_trades_flat_on_exchange": [],
        "local_open_orders_missing_on_exchange": [],
        "recent_closed_order_mismatches": [],
    }

    bot._pm_apply_startup_consistency(
        result,
        preserve_operator_stop=True,
        allow_exchange_mutations=False,
    )

    bot.exchange.cancel_order.assert_not_called()
    bot.exchange.cancel_stoploss_order.assert_not_called()
    assert bot.state == State.STOPPED
    assert "startup_consistency_mismatch" in bot._pm_blocked_order_reasons()


def test_stopped_stream_drain_suppresses_account_risk_actions(mocker, default_conf_usdt):
    bot = make_pm_bot(mocker, _conf(default_conf_usdt))
    bot.exchange.pop_pm_user_stream_events = MagicMock(return_value=[{"e": "ACCOUNT_UPDATE"}])
    bot._pm_process_user_stream_event = MagicMock(return_value=(False, False, True))
    bot._pm_risk_monitor = MagicMock()

    bot._pm_consume_user_stream_events(allow_account_risk_actions=False)

    bot._pm_risk_monitor.assert_not_called()


def test_stopped_cold_reconcile_scope_blocks_nested_pause_transition(mocker, default_conf_usdt):
    bot = make_pm_bot(mocker, _conf(default_conf_usdt))
    bot.state = State.STOPPED

    def nested_recovery(**kwargs):
        bot.set_state(State.PAUSED)
        return {"errors": []}

    bot._pm_recover_pending_intents = MagicMock(side_effect=nested_recovery)
    bot._pm_rebuild_actual_order_map = MagicMock()
    bot._pm_startup_consistency_check = MagicMock(return_value={"status": "consistent"})
    bot._pm_apply_startup_consistency = MagicMock()
    bot._pm_order_recovery = MagicMock()

    bot._pm_cold_start_reconcile(stopped_mode=True)

    assert bot.state == State.STOPPED
    bot._pm_recover_pending_intents.assert_called_once_with(
        allow_exposure_increasing_relay=False, preserve_operator_stop=True
    )
