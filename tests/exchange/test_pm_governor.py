"""
Tests for the Binance PAPI request governor: endpoint weights, 429 backoff,
418 circuit breaker, critical-priority bypass and single-flight snapshots.
"""

import threading
import time
from unittest.mock import MagicMock

import pytest

from freqtrade.exchange.pm_governor import PapiGovernor, endpoint_weight


def test_endpoint_weights_best_match():
    assert endpoint_weight("account") == 20
    assert endpoint_weight("papi/v1/account") == 20
    assert endpoint_weight("um/order") == 1
    assert endpoint_weight("um/algo/order") == 1
    assert endpoint_weight("um/income") == 30
    assert endpoint_weight("listenKey") == 1
    assert endpoint_weight("um/unknownThing") == 1  # never 0


def test_before_request_accounts_weight_and_resets_window():
    gov = PapiGovernor(weight_cap=100)
    gov.before_request(20)
    assert gov.stats()["window_used_weight"] == 20
    gov.before_request(20)
    gov.before_request(20)
    gov.before_request(20)
    gov.before_request(20)
    assert gov.stats()["window_used_weight"] == 100


def test_response_headers_are_authoritative():
    gov = PapiGovernor()
    gov.before_request(1)
    gov.on_response_headers({"X-MBX-USED-WEIGHT-1M": "777"})
    assert gov.stats()["window_used_weight"] == 777
    # Lower-case variants also accepted.
    gov.on_response_headers({"x-mbx-used-weight-1m": "5"})
    assert gov.stats()["window_used_weight"] == 777  # never decreased


def test_429_backoff_deferred_normal_requests():
    gov = PapiGovernor()
    gov.on_http_error(429)
    assert gov.stats()["backoff_active"] is True
    gov.before_request(1, priority="normal")  # must wait out the backoff
    assert gov.stats()["backed_off"] == 1


def test_429_backoff_not_deferred_for_critical():
    gov = PapiGovernor()
    gov.on_http_error(429)
    gov.before_request(1, priority="critical")  # never deferred locally
    assert gov.stats()["backed_off"] == 0


def test_retry_after_header_honored():
    gov = PapiGovernor()
    gov.on_http_error(429, {"Retry-After": "0.1"})
    before = time.monotonic()
    gov.before_request(1, priority="normal")
    assert time.monotonic() - before >= 0.05


def test_418_circuit_breaker_blocks_and_probes():
    gov = PapiGovernor()
    gov.on_http_error(418)
    assert gov.stats()["circuit_open"] is True
    with pytest.raises(Exception, match="circuit breaker OPEN"):
        gov.before_request(1, priority="critical")
    # A success proves the circuit may close.
    gov.circuit_closed_on_success()
    assert gov.stats()["circuit_open"] is False
    gov.before_request(1)  # no raise


def test_snapshot_single_flight_cache():
    gov = PapiGovernor(snapshot_ttl_s=0.2)
    assert gov.snapshot_get("account") is None
    gov.snapshot_put("account", {"uniMMR": "10"})
    assert gov.snapshot_get("account") == {"uniMMR": "10"}
    time.sleep(0.25)
    assert gov.snapshot_get("account") is None


def test_snapshot_get_miss_is_not_error():
    gov = PapiGovernor()
    gov.snapshot_put("positions", [])
    assert gov.snapshot_get("positions") == []
    assert gov.snapshot_get("missing") is None


def test_snapshot_get_or_fetch_merges_concurrent_callers():
    """A burst of concurrent callers shares ONE fetch (true single-flight)."""
    gov = PapiGovernor(snapshot_ttl_s=60)
    fetch_calls = []
    leader_started = threading.Event()

    def fetch():
        fetch_calls.append(1)
        leader_started.set()
        time.sleep(0.3)  # keep the flight open while the followers arrive
        return {"fetched": True}

    results: list[dict] = []
    errors: list[Exception] = []

    def worker():
        try:
            results.append(gov.snapshot_get_or_fetch("account", fetch))
        except Exception as e:  # pragma: no cover - fail loudly
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    assert leader_started.wait(timeout=5)
    for t in threads:
        t.join(timeout=10)
    assert not errors
    assert len(fetch_calls) == 1
    assert results == [{"fetched": True}] * 6
    # The published result is cached for the TTL.
    assert gov.snapshot_get("account") == {"fetched": True}


def test_snapshot_get_or_fetch_uses_fresh_cache():
    gov = PapiGovernor(snapshot_ttl_s=60)
    gov.snapshot_put("account", {"cached": 1})
    calls = []

    def fetch():
        calls.append(1)
        return {"fetched": True}

    assert gov.snapshot_get_or_fetch("account", fetch) == {"cached": 1}
    assert calls == []


def test_snapshot_get_or_fetch_leader_error_propagates_to_followers():
    """Followers must never silently invent data when the leader fails."""
    gov = PapiGovernor(snapshot_ttl_s=60)
    leader_started = threading.Event()
    errors: list[Exception] = []

    def fetch():
        leader_started.set()
        time.sleep(0.3)
        raise RuntimeError("boom")

    def follower():
        try:
            gov.snapshot_get_or_fetch("positions", fetch)
        except Exception as e:
            errors.append(e)

    def leader():
        try:
            gov.snapshot_get_or_fetch("positions", fetch)
        except Exception as e:
            errors.append(e)

    leader_thread = threading.Thread(target=leader)
    follower_thread = threading.Thread(target=follower)
    leader_thread.start()
    assert leader_started.wait(timeout=5)
    follower_thread.start()
    leader_thread.join(timeout=10)
    follower_thread.join(timeout=10)
    assert errors and "boom" in str(errors[0])
    assert len(errors) == 2  # leader + follower both observe the same failure
    # No poisoned cache entry may survive the failure.
    assert gov.snapshot_get("positions") is None
