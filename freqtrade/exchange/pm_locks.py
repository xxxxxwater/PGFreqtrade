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
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Single well-known lock key for the whole PM order pipeline.
PM_PIPELINE_LOCK_KEY = 0x504D_5049_5045_4C49  # "PMPIPELI"

# SQLite / fallback in-process lock (per process).
_process_lock = threading.RLock()
_pg_thread_state = threading.local()


def is_postgresql(session: Session) -> bool:
    try:
        return session.get_bind().dialect.name == "postgresql"
    except Exception:
        return False


@contextmanager
def pm_pipeline_lock(session: Session, timeout_seconds: float = 10.0):
    """Hold a session advisory lock on a pinned connection, independent of ORM commits."""
    if not is_postgresql(session):
        with _process_lock:
            yield
        return

    bind = session.get_bind()
    engine: Engine = bind.engine if isinstance(bind, Connection) else bind
    states = getattr(_pg_thread_state, "pipelines", None)
    if states is None:
        states = {}
        _pg_thread_state.pipelines = states
    key = id(engine)
    if key in states:
        # Reentrancy is scoped to this engine, never a different database.
        yield
        return

    lock_conn = engine.connect()
    acquired = False
    released = False
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    try:
        while True:
            acquired = bool(lock_conn.execute(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": PM_PIPELINE_LOCK_KEY},
            ).scalar())
            lock_conn.commit()
            if acquired:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Could not acquire the PM pipeline advisory lock within {timeout_seconds}s."
                )
            time.sleep(0.05)
        states[key] = lock_conn
        try:
            yield
        finally:
            states.pop(key, None)
            released = bool(lock_conn.execute(
                text("SELECT pg_advisory_unlock(:key)"),
                {"key": PM_PIPELINE_LOCK_KEY},
            ).scalar())
            lock_conn.commit()
            if not released:
                raise RuntimeError("PM pipeline advisory lock ownership lost at release.")
    except BaseException:
        # Connection.close() returns pooled connections; it does NOT necessarily
        # close the PostgreSQL session. Never pool a connection with uncertain locks.
        lock_conn.invalidate()
        raise
    finally:
        states.pop(key, None)
        if acquired and not released and not lock_conn.invalidated:
            lock_conn.invalidate()
        lock_conn.close()
