from datetime import UTC, datetime, timedelta

from freqtrade.enums import RPCMessageType
from freqtrade.freqtradebot import FreqtradeBot


class PMExchangeStub:
    def __init__(self, stats):
        self._stats = stats
        self.started = False
        self.stopped = False

    def _is_portfolio_margin(self):
        return True

    def get_pm_user_stream_stats(self):
        return self._stats

    def stop_pm_user_stream(self):
        self.stopped = True

    def start_pm_user_stream(self, listen_key):
        self.started = listen_key


def make_pm_bot(mocker, stats):
    bot = FreqtradeBot.__new__(FreqtradeBot)
    bot.config = {"dry_run": False, "exchange": {"portfolio_margin_risk": {}}}
    bot.exchange = PMExchangeStub(stats)
    bot.rpc = mocker.Mock()
    bot._pm_listen_key = "listen-key"
    bot._pm_order_recovery = mocker.Mock()
    bot._pm_create_listen_key = mocker.Mock()
    bot._pm_init_user_stream_state()
    return bot


def test_pm_user_stream_health_recovers_after_dropped_events(mocker):
    bot = make_pm_bot(
        mocker,
        {
            "enabled": True,
            "running": True,
            "connected": True,
            "queued_events": 0,
            "events_dropped": 2,
            "parse_errors": 0,
        },
    )
    # Round-4: unblocking requires a clean reconcile; stub a successful one.
    bot._pm_reconcile_open_orders = mocker.Mock(
        return_value={"errors": [], "unresolved_intents": 0}
    )
    bot._pm_has_unresolved_intents = mocker.Mock(return_value=False)

    bot._pm_user_stream_health_monitor()

    bot.rpc.send_msg.assert_called_once()
    assert bot.rpc.send_msg.call_args.args[0]["type"] == RPCMessageType.WARNING
    assert "dropped events" in bot.rpc.send_msg.call_args.args[0]["status"]
    bot._pm_order_recovery.assert_called_once()


def test_pm_user_stream_health_restarts_after_long_disconnect(mocker):
    bot = make_pm_bot(
        mocker,
        {
            "enabled": True,
            "running": True,
            "connected": False,
            "queued_events": 0,
            "events_dropped": 0,
            "parse_errors": 0,
            "last_disconnected_at": (datetime.now(UTC) - timedelta(seconds=180)).isoformat(),
            "last_error": "ConnectionClosedError",
        },
    )

    bot._pm_user_stream_health_monitor()

    assert bot.exchange.stopped is True
    assert bot.exchange.started == "listen-key"
    bot._pm_order_recovery.assert_called_once()
    assert bot.rpc.send_msg.call_args.args[0]["type"] == RPCMessageType.WARNING


def test_pm_unmatched_order_event_warns_and_recovers_once(mocker):
    bot = FreqtradeBot.__new__(FreqtradeBot)
    bot.config = {"exchange": {"portfolio_margin_risk": {}}}
    bot.rpc = mocker.Mock()
    bot._pm_order_recovery = mocker.Mock()
    bot.exchange = mocker.Mock()
    bot.exchange.markets = {"BTC/USDT:USDT": {"id": "BTCUSDT"}}
    bot._pm_init_user_stream_state()

    event = {"e": "ORDER_TRADE_UPDATE", "o": {"i": 12345, "s": "BTCUSDT"}}

    assert bot._pm_handle_order_trade_update(event, {}) is False
    assert bot._pm_handle_order_trade_update(event, {}) is False

    # Foreign order (no client id): warn only, never alert, never recover.
    bot.rpc.send_msg.assert_not_called()
    bot._pm_order_recovery.assert_not_called()


def test_pm_unmatched_own_order_event_blocks_and_alerts(mocker):
    """An order carrying our client id that has no local match must fail closed."""
    bot = FreqtradeBot.__new__(FreqtradeBot)
    bot.config = {"exchange": {"portfolio_margin_risk": {}}}
    bot.rpc = mocker.Mock()
    bot._pm_order_recovery = mocker.Mock()
    bot.exchange = mocker.Mock()
    bot.exchange.markets = {"BTC/USDT:USDT": {"id": "BTCUSDT"}}
    bot._pm_init_user_stream_state()

    event = {"e": "ORDER_TRADE_UPDATE", "o": {"i": 12345, "s": "BTCUSDT", "c": "ftabcd"}}

    assert bot._pm_handle_order_trade_update(event, {}) is False

    assert "unmatched_stream_order" in bot._pm_blocked_order_reasons()
    assert bot.rpc.send_msg.call_count == 1
    assert "FAIL-CLOSED" in bot.rpc.send_msg.call_args.args[0]["status"]
    bot._pm_order_recovery.assert_called_once()
