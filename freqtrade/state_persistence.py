"""
Bot-state persistence across restarts (production fail-safe).

A bot that paused or stopped itself (risk fail-closed, operator /pause or
/stop) must not silently re-arm as RUNNING after a container restart or
server reboot.  This module persists every RUNNING/PAUSED/STOPPED state
change to a small JSON file under the user-data directory (which survives
container recreation) and restores the recorded state on startup.

Design rules:
  - Live trading only: disabled entirely for dry-run runs.
  - Can be switched off with ``internals.persist_state: false``.
  - RUNNING, PAUSED and STOPPED are all persisted.  An intentional /stop is
    therefore also restored: the bot never re-arms itself automatically.
  - A MISSING file (first boot) is explicitly distinguished from a CORRUPT
    file: corrupt state blocks auto-trading (the bot starts PAUSED) instead
    of silently falling back to the configured running state.
  - A valid persisted PAUSED/STOPPED overrides the configured default; a
    persisted RUNNING never overrides an explicit configuration.
  - ``persist_state`` RETURNS a success flag: the caller must know when the
    write failed, because a pause/stop that never reached disk can be lost
    on the next restart.  The write is made power-loss-durable with fsync
    (file before rename, directory after rename), not just atomic.
"""

import errno
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from freqtrade.enums import State


logger = logging.getLogger(__name__)


@dataclass
class PersistedState:
    """Outcome of reading the persisted state file.

    state:   the valid persisted state, or None when no record exists yet.
    corrupt: True when a record exists but cannot be trusted.  Callers must
             treat this differently from "no record": the fail-safe answer
             for corrupt state is to BLOCK auto-trading, not to fall back to
             the configured default.
    """

    state: State | None = None
    corrupt: bool = False


def state_file_path(config: dict) -> Path:
    """File that survives container recreation: <user_data>/.freqtrade/state."""
    return Path(config.get("user_data_dir", "") or "") / ".freqtrade" / "state"


def persistence_enabled(config: dict) -> bool:
    if config.get("dry_run", False):
        return False
    return bool(config.get("internals", {}).get("persist_state", True))


def _directory_fsync_error_ignorable(exception: OSError) -> bool:
    """Only "not supported" outcomes may be ignored for directory fsync.

    A REAL I/O error on the directory fsync means the rename itself may not
    survive power loss - that is a durability failure and must surface as a
    persist failure, never be swallowed as success.
    """
    if exception.errno in (errno.EINVAL, errno.ENOTSUP):
        return True
    # Windows cannot open directories with os.open(); that is a platform
    # limitation, not an I/O failure.
    if os.name == "nt" and exception.errno == errno.EACCES:
        return True
    return False


def persist_state(config: dict, state: State) -> bool:
    """Durably write the current RUNNING/PAUSED/STOPPED state to disk.

    Called synchronously at the moment of the state change (before any
    notification), so a crash right after a pause/stop decision can never
    lose it.  Returns False when the state could NOT be persisted - the
    caller must alert loudly, because a restart would restore the previous
    (stale) state.  Non-trading states (RELOAD_CONFIG) and disabled
    persistence are no-ops that return True (nothing to persist).
    """
    if not persistence_enabled(config):
        return True
    if state not in (State.RUNNING, State.PAUSED, State.STOPPED):
        return True
    path = state_file_path(config)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        payload = json.dumps(
            {
                "state": state.name,
                "updated_at": datetime.now(UTC).isoformat(),
            }
        )
        # Power-loss durability: fsync the FILE before the atomic rename and
        # fsync the DIRECTORY afterwards so the rename itself survives.
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        try:
            dir_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError as exception:
            if not _directory_fsync_error_ignorable(exception):
                raise
        return True
    except Exception as exception:
        logger.critical(
            f"Could not persist bot state {state.name} to {path}: {exception}. "
            "A restart before the next successful write may restore the previous state."
        )
        return False


def invalidate_state_file(config: dict) -> bool:
    """Best-effort fail-safe after a failed persist.

    Overwrite the state record with a CORRUPT marker so a restart can never
    resurrect the previous (stale) state: ``read_persisted_state`` reports
    corrupt and the bot boots PAUSED (auto-trading blocked).  Returns False
    when invalidation itself failed - in that case nothing more can be done
    locally and only the CRITICAL log + operator alert remain.
    """
    if not persistence_enabled(config):
        return True
    path = state_file_path(config)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("corrupt - previous persist failed\n", encoding="utf-8")
        try:
            with open(path, "rb") as handle:
                os.fsync(handle.fileno())
        except OSError:
            pass  # tombstone fsync is best-effort; content already replaced
        return True
    except Exception as exception:
        logger.critical(f"Could not invalidate stale bot state at {path}: {exception}")
        return False


def read_persisted_state(config: dict) -> PersistedState:
    """Read the persisted state, distinguishing "no record" from "corrupt".

    Any file that parses to something that is not a well-formed record
    (including valid JSON like ``[]``) is CORRUPT, never silently ignored.
    """
    if not persistence_enabled(config):
        return PersistedState()
    path = state_file_path(config)
    try:
        if not path.is_file():
            return PersistedState()
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            logger.warning(f"Persisted bot state in {path} is not a JSON object")
            return PersistedState(corrupt=True)
        name = str(data.get("state", "")).upper()
        if name in ("RUNNING", "PAUSED", "STOPPED"):
            return PersistedState(state=State[name])
        logger.warning(f"Persisted bot state in {path} has unknown value {name!r}")
        return PersistedState(corrupt=True)
    except FileNotFoundError:
        return PersistedState()
    except Exception as exception:
        logger.warning(f"Persisted bot state in {path} is corrupt: {exception}")
        return PersistedState(corrupt=True)
