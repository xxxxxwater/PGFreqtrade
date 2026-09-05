"""
Bot-state persistence across restarts (production fail-safe).

A bot that paused itself (risk fail-closed, operator /pause) must not silently
re-arm as RUNNING after a container restart or server reboot.  This module
persists RUNNING/PAUSED transitions to a small JSON file under the
user-data directory (which survives container recreation) and restores a
persisted PAUSED state on startup.

Design rules:
  - Live trading only: disabled entirely for dry-run runs.
  - Can be switched off with ``internals.persist_state: false``.
  - Only RUNNING/PAUSED are persisted; STOPPED clears the file, so an
    intentional stop is not resurrected.
  - On startup only PAUSED is restored (the fail-safe direction).  A
    persisted RUNNING is never used to override an explicit configuration.
  - Every filesystem failure degrades to a warning; persistence must never
    crash the trading loop.
"""

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

from freqtrade.enums import State


logger = logging.getLogger(__name__)


def state_file_path(config: dict) -> Path:
    """File that survives container recreation: <user_data>/.freqtrade/state."""
    return Path(config.get("user_data_dir", "") or "") / ".freqtrade" / "state"


def persistence_enabled(config: dict) -> bool:
    if config.get("dry_run", False):
        return False
    return bool(config.get("internals", {}).get("persist_state", True))


def persist_state(config: dict, state: State) -> None:
    """Atomically write the current RUNNING/PAUSED state to disk."""
    if not persistence_enabled(config):
        return
    if state not in (State.RUNNING, State.PAUSED):
        # STOPPED is handled by clear_persisted_state(); RELOAD_CONFIG must
        # never overwrite a real state.
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


def clear_persisted_state(config: dict) -> None:
    """Remove the persisted state (intentional stop or explicit reset)."""
    path = state_file_path(config)
    try:
        path.unlink(missing_ok=True)
    except Exception as exception:
        logger.debug(f"Could not clear persisted bot state: {exception}")


def read_persisted_state(config: dict) -> State | None:
    """Return the persisted state, or None when absent/unreadable/disabled."""
    if not persistence_enabled(config):
        return None
    path = state_file_path(config)
    try:
        if not path.is_file():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        name = str(data.get("state", "")).upper()
        if name in ("RUNNING", "PAUSED"):
            return State[name]
        logger.warning(f"Ignoring unknown persisted bot state {name!r} in {path}")
    except Exception as exception:
        logger.warning(f"Could not read persisted bot state from {path}: {exception}")
    return None
