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
  - Every filesystem failure degrades to a warning; persistence must never
    crash the trading loop.
"""

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


def persist_state(config: dict, state: State) -> None:
    """Atomically write the current RUNNING/PAUSED/STOPPED state to disk.

    Called synchronously at the moment of the state change (before any
    notification), so a crash right after a pause/stop decision can never
    lose it.  Non-trading states (RELOAD_CONFIG) are ignored.
    """
    if not persistence_enabled(config):
        return
    if state not in (State.RUNNING, State.PAUSED, State.STOPPED):
        return
    path = state_file_path(config)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        payload = {
            "state": state.name,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, path)
    except Exception as exception:
        logger.warning(f"Could not persist bot state: {exception}")


def read_persisted_state(config: dict) -> PersistedState:
    """Read the persisted state, distinguishing "no record" from "corrupt"."""
    if not persistence_enabled(config):
        return PersistedState()
    path = state_file_path(config)
    try:
        if not path.is_file():
            return PersistedState()
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return PersistedState()
    except Exception as exception:
        logger.warning(f"Persisted bot state in {path} is corrupt: {exception}")
        return PersistedState(corrupt=True)

    name = str(data.get("state", "")).upper()
    if name in ("RUNNING", "PAUSED", "STOPPED"):
        return PersistedState(state=State[name])
    logger.warning(f"Persisted bot state in {path} has unknown value {name!r}")
    return PersistedState(corrupt=True)
