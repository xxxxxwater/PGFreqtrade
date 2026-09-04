import json
from threading import RLock

from freqtrade.exchange.binance import Binance
from freqtrade.exchange.binance_pm_user_stream import BinancePMUserStream


def test_pm_user_stream_queues_events_and_updates_stats():
    stream = BinancePMUserStream("listen-key", max_queue_size=3)

    stream._handle_message(json.dumps({"e": "ORDER_TRADE_UPDATE", "E": 123, "o": {"i": 42}}))

    assert stream.pop_events() == [{"e": "ORDER_TRADE_UPDATE", "E": 123, "o": {"i": 42}}]
    stats = stream.stats()
    assert stats["events_received"] == 1
    assert stats["events_dropped"] == 0
    assert stats["last_event_type"] == "ORDER_TRADE_UPDATE"
    assert stats["last_event_time"] == "123"


def test_pm_user_stream_caps_queue_and_counts_dropped_events():
    stream = BinancePMUserStream("listen-key", max_queue_size=2)

    stream._handle_message(json.dumps({"e": "ACCOUNT_UPDATE", "E": 1}))
    stream._handle_message(json.dumps({"e": "ACCOUNT_UPDATE", "E": 2}))
    stream._handle_message(json.dumps({"e": "ACCOUNT_UPDATE", "E": 3}))

    assert [event["E"] for event in stream.pop_events()] == [2, 3]
    stats = stream.stats()
    assert stats["events_received"] == 3
    assert stats["events_dropped"] == 1


def test_pm_user_stream_invalid_json_is_not_queued():
    stream = BinancePMUserStream("listen-key")

    stream._handle_message("{invalid")

    assert stream.pop_events() == []
    stats = stream.stats()
    assert stats["parse_errors"] == 1
    assert "JSONDecodeError" in stats["last_error"]


def test_pm_user_stream_reconnect_delay_backs_off_and_resets():
    stream = BinancePMUserStream("listen-key", reconnect_delay=1, max_reconnect_delay=4)

    assert stream._record_reconnect() == 1
    assert stream._record_reconnect() == 2
    assert stream._record_reconnect() == 4
    assert stream._record_reconnect() == 4
    assert stream.stats()["next_reconnect_delay"] == 4

    stream._record_connect()

    assert stream.stats()["connected"] is True
    assert stream.stats()["next_reconnect_delay"] == 1


class DummyPMUserStream:
    instances = []

    def __init__(self, listen_key):
        self.listen_key = listen_key
        self.started = False
        self.stopped = False
        self.events = [{"e": "ACCOUNT_UPDATE"}]
        DummyPMUserStream.instances.append(self)

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def pop_events(self, max_events=1000):
        events = self.events[:max_events]
        self.events = self.events[max_events:]
        return events

    def stats(self):
        return {
            "enabled": True,
            "running": self.started and not self.stopped,
            "connected": self.started and not self.stopped,
        }


def _set_minimal_exchange_cleanup_attrs(exchange):
    exchange._exchange_ws = None
    exchange._api_async = None
    exchange._ws_async = None
    exchange.loop = None


def test_pm_exchange_user_stream_switching_is_locked(monkeypatch):
    DummyPMUserStream.instances = []
    monkeypatch.setattr("freqtrade.exchange.binance.BinancePMUserStream", DummyPMUserStream)

    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._config = {
        "dry_run": False,
        "exchange": {"portfolio_margin_risk": {"user_stream_enabled": True}},
    }
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    _set_minimal_exchange_cleanup_attrs(exchange)

    exchange.start_pm_user_stream("listen-key-1")
    first = exchange._pm_user_stream
    exchange.start_pm_user_stream("listen-key-1")
    exchange.start_pm_user_stream("listen-key-2")
    second = exchange._pm_user_stream

    assert len(DummyPMUserStream.instances) == 2
    assert first.stopped is True
    assert second.listen_key == "listen-key-2"
    assert second.started is True
    assert exchange.pop_pm_user_stream_events() == [{"e": "ACCOUNT_UPDATE"}]
    assert exchange.get_pm_user_stream_stats()["running"] is True

    exchange.stop_pm_user_stream()

    assert second.stopped is True
    assert exchange.get_pm_user_stream_stats()["running"] is False
