"""
PostgreSQL advisory-lock helpers for the PM order path.

The PM order pipeline (intent/outbox enqueue, relay dispatch, startup recovery,
reconciliation) must never run concurrently from two processes or two threads.
On PostgreSQL this is guaranteed with a session-scoped advisory lock; on other
backends (SQLite in tests / dry-run) a process-wide reentrant lock is used,
because SQLite has a single writer anyway.

NOTE: this module lives under freqtrade.exchange (NOT freqtrade.persistence) on
purpose - importing it must never execute freqtrade/persistence/__init__.py,
which would create a circular import (exchange -> persistence -> trade_model ->
exchange).
"""

import logging
import threading
import time
from contextlib import contextmanager

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Single well-known lock key for the whole PM order pipeline.
PM_PIPELINE_LOCK_KEY = 0x504D_5049_5045_4C49  # "PMPIPELI"

# SQLite / fallback in-process lock (per process).
_process_lock = threading.RLock()


def is_postgresql(session: Session) -> bool:
    try:
        return session.get_bind().dialect.name == "postgresql"
    except Exception:
        return False


@contextmanager
def pm_pipeline_lock(session: Session, timeout_seconds: float = 10.0):
    """
    Serialize PM order-pipeline sections.

    PostgreSQL: uses pg_try_advisory_lock in a polling loop, released with
    pg_advisory_unlock (session-scoped: automatically released if the process
    dies - never a stale lock). Timeout raises RuntimeError (fail-closed).

    Other backends: acquires a process-wide reentrant lock (SQLite has a single
    writer and tests are single-process).
    """
    if is_postgresql(session):
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        while True:
            acquired = session.execute(
                text("SELECT pg_try_advisory_lock(:key)"), {"key": PM_PIPELINE_LOCK_KEY}
            ).scalar()
            if acquired:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Could not acquire the PM pipeline advisory lock within "
                    f"{timeout_seconds}s (another PM instance may be running)."
                )
            time.sleep(0.05)
        try:
            yield
        finally:
            try:
                session.execute(
                    text("SELECT pg_advisory_unlock(:key)"), {"key": PM_PIPELINE_LOCK_KEY}
                )
            except Exception as e:  # pragma: no cover - defensive
                logger.warning(f"Could not release PM pipeline advisory lock: {e}")
    else:
        with _process_lock:
            yield
