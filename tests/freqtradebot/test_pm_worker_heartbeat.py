from types import SimpleNamespace

from freqtrade.util import PeriodicCache
from freqtrade.worker import Worker


class PMHeartbeatExchangeStub:
    def __init__(self):
        self.risk_calls = 0

    def _is_portfolio_margin(self):
        return True

    def get_pm_risk_summary(self):
        self.risk_calls += 1
        return {
            "enabled": True,
            "uni_mmr": 10,
            "account_equity": 100,
            "account_status": "NORMAL",
        }

    def get_pm_user_stream_stats(self):
        return {"connected": True, "queued_events": 0, "events_dropped": 0}


def test_pm_heartbeat_risk_summary_is_cached():
    exchange = PMHeartbeatExchangeStub()
    worker = Worker.__new__(Worker)
    worker.freqtrade = SimpleNamespace(
        exchange=exchange,
        config={"dry_run": False},
    )
    worker._pm_heartbeat_risk_cache = PeriodicCache(maxsize=1, ttl=300)

    first = worker._pm_heartbeat_suffix()
    second = worker._pm_heartbeat_suffix()

    assert "uniMMR=10" in first
    assert "stream_connected=True" in second
    assert exchange.risk_calls == 1
