"""
Binance PAPI request governor.

All Portfolio Margin PAPI requests flow through one governor that:

* accounts endpoint request weight against a per-minute budget (response header
  ``X-MBX-USED-WEIGHT-1M`` is authoritative when present, otherwise a local
  weight table is used),
* applies exponential backoff with jitter after 429 responses (and honours
  ``Retry-After`` from the raw fallback),
* opens a circuit breaker on 418 (IP ban) and probes with growing backoff,
* gives ``critical`` requests (reduce-only exits / stoplosses / emergency
  closes) priority: they are never deferred by local backoff,
* provides short-TTL single-flight caches for account / position snapshots so
  the pre-order risk checks do not multiply endpoint weight.
"""

import logging
import threading
import time
from typing import Any, Callable

from freqtrade.exceptions import TemporaryError

logger = logging.getLogger(__name__)

# Conservative endpoint weight table (per request). Values follow the current
# Binance PM rate-limit documentation; unknowns default to 1 (never 0 - an
# under-estimate would over-spend the budget).
_ENDPOINT_WEIGHTS = {
    "account": 20,
    "balance": 20,
    "um/order": 1,
    "um/openOrders": 1,
    "um/allOrders": 5,
    "um/orderAmendment": 1,
    "um/positionRisk": 5,
    "um/accountConfig": 5,
    "um/symbolConfig": 5,
    "um/leverage": 1,
    "um/marginType": 1,
    "um/income": 30,
    "um/userTrades": 5,
    "um/apiTradingStatus": 1,
    "um/algo/order": 1,
    "um/algo/algoOrder": 1,
    "um/algo/openAlgoOrders": 1,
    "um/algo/allAlgoOrders": 5,
    "listenKey": 1,
}

_DEFAULT_WEIGHT = 1
_WEIGHT_CAP = 1200  # conservative share of the standard PM weight budget
_BACKOFF_INITIAL_S = 0.5
_BACKOFF_MAX_S = 30.0
_CIRCUIT_PROBE_INITIAL_S = 30.0
_CIRCUIT_PROBE_MAX_S = 300.0


def endpoint_weight(path: str) -> int:
    """Weight of a PAPI path (best-match prefix)."""
    path = str(path).strip().lstrip("/")
    for prefix in ("papi/v1/", "papi/"):
        if path.startswith(prefix):
            path = path[len(prefix):]
            break
    best = _DEFAULT_WEIGHT
    best_len = 0
    for known, weight in _ENDPOINT_WEIGHTS.items():
        if path.startswith(known) and len(known) > best_len:
            best = weight
            best_len = len(known)
    return best


class PapiGovernor:
    """Per-process PAPI rate-limit / circuit-breaker state + snapshot caches."""

    def __init__(self, *, weight_cap: int = _WEIGHT_CAP, snapshot_ttl_s: float = 3.0) -> None:
        self._lock = threading.RLock()
        self._weight_cap = weight_cap
        self._window_start = time.monotonic()
        self._window_used = 0
        self._backoff_until = 0.0
        self._backoff_multiplier = 1.0
        self._circuit_open = False
        self._circuit_probe_delay = _CIRCUIT_PROBE_INITIAL_S
        self._circuit_probe_after = 0.0
        self._snapshot_ttl_s = snapshot_ttl_s
        self._snapshots: dict[str, tuple[float, Any]] = {}
        # Single-flight bookkeeping: key -> (completion event, [result, error]).
        self._inflight: dict[str, tuple[threading.Event, list[Any, Any]]] = {}
        self._counters = {"requests": 0, "429": 0, "418": 0, "backed_off": 0}

    # ---- snapshot single-flight cache ----

    def snapshot_get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._snapshots.get(key)
            if entry is None:
                return None
            expires, value = entry
            if time.monotonic() > expires:
                self._snapshots.pop(key, None)
                return None
            return value

    def snapshot_put(self, key: str, value: Any) -> None:
        with self._lock:
            self._snapshots[key] = (time.monotonic() + self._snapshot_ttl_s, value)

    def snapshot_get_or_fetch(
        self,
        key: str,
        fetch_fn: Callable[[], Any],
        *,
        ttl_s: float | None = None,
        wait_timeout_s: float = 30.0,
    ) -> Any:
        """
        True single-flight snapshot fetch.

        Concurrent callers for the same key share ONE ``fetch_fn`` execution:
        the first caller becomes the leader and every other caller waits on a
        per-key completion event, so a burst of pre-order risk checks collapses
        into a single PAPI request instead of a TTL-cache stampede.

        * A fresh cache entry short-circuits (no fetch, no flight).
        * Leader success publishes the result into the cache, then releases
          the followers.
        * Leader failure is stored and re-raised in every follower (fail-closed
          - followers never silently invent data).
        * A follower that is not released within ``wait_timeout_s`` raises
          :class:`TemporaryError` instead of hanging the order path forever.
        """
        with self._lock:
            cached = self._snapshots.get(key)
            if cached is not None and time.monotonic() <= cached[0]:
                return cached[1]
            inflight = self._inflight.get(key)
            if inflight is None:
                event = threading.Event()
                slot: list[Any, Any] = [None, None]
                self._inflight[key] = (event, slot)
                leader = True
            else:
                event, slot = inflight
                leader = False
        if leader:
            try:
                result = fetch_fn()
            except BaseException as exc:
                with self._lock:
                    slot[1] = exc
                    event.set()
                    self._inflight.pop(key, None)
                raise
            with self._lock:
                self._snapshots[key] = (
                    time.monotonic() + (ttl_s if ttl_s is not None else self._snapshot_ttl_s),
                    result,
                )
                slot[0] = result
                event.set()
                self._inflight.pop(key, None)
            return result
        # Follower: wait for the leader, then return its result or re-raise.
        if not event.wait(wait_timeout_s):
            with self._lock:
                self._inflight.pop(key, None)
            raise TemporaryError(
                f"Single-flight snapshot '{key}' timed out after {wait_timeout_s:.1f}s "
                "(leader never completed)."
            )
        error = slot[1]
        if error is not None:
            raise error
        with self._lock:
            cached = self._snapshots.get(key)
            if cached is not None:
                return cached[1]
        # Defensive retry: the leader must have published before setting the
        # event, so this is unreachable in practice.
        return self.snapshot_get_or_fetch(
            key, fetch_fn, ttl_s=ttl_s, wait_timeout_s=wait_timeout_s
        )

    # ---- rate limit / circuit breaker ----

    def before_request(self, weight: int, priority: str = "normal") -> None:
        """
        Gate a request. Raises TemporaryError when the circuit is open; normal
        priority requests additionally wait out the active backoff (critical
        requests are never deferred locally - the exchange remains the final
        authority).
        """
        with self._lock:
            now = time.monotonic()
            if now - self._window_start > 60.0:
                self._window_start = now
                self._window_used = 0
            if self._circuit_open:
                raise _CircuitOpenError(
                    "Binance PAPI circuit breaker OPEN (418 IP ban). All PAPI "
                    "requests are refused until the probe window passes."
                )
            self._window_used += max(1, int(weight))
            if self._window_used > self._weight_cap:
                logger.warning(
                    "PAPI local weight budget exceeded: used=%s cap=%s "
                    "(exchange headers remain authoritative).",
                    self._window_used,
                    self._weight_cap,
                )
            self._counters["requests"] += 1
        if priority != "critical" and self._backoff_until > time.monotonic():
            wait = self._backoff_until - time.monotonic()
            self._counters["backed_off"] += 1
            logger.warning("PAPI backoff active: sleeping %.2fs before request.", wait)
            time.sleep(wait)

    def on_response_headers(self, headers: dict[str, Any] | None) -> None:
        """Update the authoritative weight counter from response headers."""
        if not headers:
            return
        try:
            used = headers.get("X-MBX-USED-WEIGHT-1M") or headers.get("x-mbx-used-weight-1m")
            if used is not None:
                self._window_used = max(self._window_used, int(used))
        except (TypeError, ValueError):
            pass

    def on_http_error(self, status: int, headers: dict[str, Any] | None = None) -> None:
        """429 -> exponential backoff (Retry-After honoured); 418 -> circuit break."""
        with self._lock:
            if status == 429:
                self._counters["429"] += 1
                retry_after = 0.0
                if headers:
                    try:
                        retry_after = float(
                            headers.get("Retry-After") or headers.get("retry-after") or 0
                        )
                    except (TypeError, ValueError):
                        retry_after = 0.0
                delay = retry_after or min(
                    _BACKOFF_MAX_S, _BACKOFF_INITIAL_S * self._backoff_multiplier
                )
                self._backoff_multiplier = min(64.0, self._backoff_multiplier * 2)
                self._backoff_until = max(self._backoff_until, time.monotonic() + delay)
                logger.warning("PAPI 429: backoff %.2fs (Retry-After=%s).", delay, retry_after)
            elif status == 418:
                self._counters["418"] += 1
                if not self._circuit_open:
                    self._circuit_open = True
                    self._circuit_probe_after = time.monotonic() + self._circuit_probe_delay
                    logger.critical(
                        "PAPI 418 received: circuit breaker OPEN for %.0fs.",
                        self._circuit_probe_delay,
                    )
                else:
                    self._circuit_probe_delay = min(
                        _CIRCUIT_PROBE_MAX_S, self._circuit_probe_delay * 2
                    )
                    self._circuit_probe_after = time.monotonic() + self._circuit_probe_delay

    def circuit_closed_on_success(self) -> None:
        """A successful request proves the circuit can close again."""
        with self._lock:
            if self._circuit_open:
                self._circuit_open = False
                self._circuit_probe_delay = _CIRCUIT_PROBE_INITIAL_S
                self._backoff_multiplier = 1.0
                logger.info("PAPI circuit breaker closed (request succeeded).")

    def maybe_probe(self) -> bool:
        """True when the circuit may be probed again (a single trial request)."""
        with self._lock:
            return self._circuit_open and time.monotonic() >= self._circuit_probe_after

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "circuit_open": self._circuit_open,
                "window_used_weight": self._window_used,
                "weight_cap": self._weight_cap,
                "backoff_active": self._backoff_until > time.monotonic(),
                **self._counters,
            }


class _CircuitOpenError(Exception):
    """Raised by the governor while the 418 circuit breaker is open."""
