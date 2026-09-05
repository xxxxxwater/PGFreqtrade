"""
Tests for production bot-state persistence across restarts.

A bot that paused itself (risk fail-closed or operator /pause) must stay
PAUSED after a container restart / server reboot instead of silently
re-arming as RUNNING.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from freqtrade.enums import State
from freqtrade.state_persistence import (
    clear_persisted_state,
    persistence_enabled,
    persist_state,
    read_persisted_state,
    state_file_path,
)


def live_conf(tmp_path: Path) -> dict:
    return {
        "dry_run": False,
        "user_data_dir": str(tmp_path),
        "internals": {},
    }


def test_persist_and_read_roundtrip(tmp_path):
    conf = live_conf(tmp_path)
    assert read_persisted_state(conf) is None
    persist_state(conf, State.PAUSED)
    assert read_persisted_state(conf) == State.PAUSED
    persist_state(conf, State.RUNNING)
    assert read_persisted_state(conf) == State.RUNNING
    # File lives under user_data/.freqtrade/state (the bind-mounted volume).
    assert (tmp_path / ".freqtrade" / "state").is_file()


def test_dry_run_never_persists_or_reads(tmp_path):
    conf = live_conf(tmp_path)
    conf["dry_run"] = True
    assert not persistence_enabled(conf)
    persist_state(conf, State.PAUSED)
    assert not (tmp_path / ".freqtrade" / "state").exists()
    assert read_persisted_state(conf) is None


def test_internals_flag_disables(tmp_path):
    conf = live_conf(tmp_path)
    conf["internals"]["persist_state"] = False
    assert not persistence_enabled(conf)
    persist_state(conf, State.PAUSED)
    assert read_persisted_state(conf) is None


def test_non_trading_states_never_persisted(tmp_path):
    conf = live_conf(tmp_path)
    # An intentional stop must not be resurrected by an old value.
    persist_state(conf, State.STOPPED)
    persist_state(conf, State.RELOAD_CONFIG)
    assert read_persisted_state(conf) is None


def test_clear_removes_file(tmp_path):
    conf = live_conf(tmp_path)
    persist_state(conf, State.PAUSED)
    clear_persisted_state(conf)
    assert read_persisted_state(conf) is None


def test_corrupt_file_tolerated(tmp_path, caplog):
    conf = live_conf(tmp_path)
    state_file_path(conf).parent.mkdir(parents=True)
    state_file_path(conf).write_text("{not json", encoding="utf-8")
    assert read_persisted_state(conf) is None
    assert "Could not read persisted bot state" in caplog.text


def test_unknown_state_name_ignored(tmp_path, caplog):
    conf = live_conf(tmp_path)
    state_file_path(conf).parent.mkdir(parents=True)
    state_file_path(conf).write_text(json.dumps({"state": "PAUSEDX"}), encoding="utf-8")
    assert read_persisted_state(conf) is None
    assert "unknown persisted bot state" in caplog.text


def test_worker_transition_persists_and_clears(tmp_path):
    """The worker's state-transition hook writes RUNNING/PAUSED, clears on STOPPED."""
    from freqtrade.freqtradebot import FreqtradeBot
    from freqtrade.worker import Worker

    conf = {
        "dry_run": False,
        "user_data_dir": str(tmp_path),
        "internals": {},
        "timeframe": "5m",
    }
    worker = Worker.__new__(Worker)
    worker._config = conf
    worker._heartbeat_msg = 0
    worker._heartbeat_interval = 0
    worker._throttle_secs = 5
    worker._throttle = MagicMock()
    worker._notify = MagicMock()
    bot = MagicMock(spec=FreqtradeBot)
    bot.state = State.PAUSED
    bot.notify_status = MagicMock()
    bot.startup = MagicMock()
    bot.check_for_open_trades = MagicMock()
    worker.freqtrade = bot

    worker._worker(old_state=State.RUNNING)
    assert read_persisted_state(conf) == State.PAUSED

    bot.state = State.STOPPED
    worker._worker(old_state=State.PAUSED)
    assert read_persisted_state(conf) is None


def _live_pm_conf(default_conf_usdt, tmp_path: Path) -> dict:
    conf = default_conf_usdt.copy()
    conf["dry_run"] = False
    conf["user_data_dir"] = tmp_path
    conf["trading_mode"] = "futures"
    conf["margin_mode"] = "cross"
    conf["stake_currency"] = "USDT"
    conf["exchange"] = conf["exchange"].copy()
    conf["exchange"]["name"] = "binance"
    conf["exchange"]["key"] = "dummy_key"
    conf["exchange"]["secret"] = "dummy_secret"
    conf["exchange"]["pair_whitelist"] = ["ETH/USDT:USDT"]
    conf["exchange"]["portfolio_margin"] = True
    conf["exchange"]["portfolio_margin_risk"] = {
        "min_uni_mmr": 1.5,
        "user_stream_enabled": False,
    }
    return conf


def test_bot_restores_persisted_paused_on_init(mocker, default_conf_usdt, tmp_path):
    from tests.conftest import get_patched_freqtradebot

    conf = _live_pm_conf(default_conf_usdt, tmp_path)
    conf["initial_state"] = "running"
    # Simulate a crash/reboot that happened while the bot was PAUSED.
    persist_state(conf, State.PAUSED)
    mocker.patch("freqtrade.exchange.binance.Binance.validate_config", MagicMock())
    bot = get_patched_freqtradebot(mocker, conf)
    assert bot.state == State.PAUSED


def test_bot_starts_from_config_when_no_state_file(mocker, default_conf_usdt, tmp_path):
    from tests.conftest import get_patched_freqtradebot

    conf = _live_pm_conf(default_conf_usdt, tmp_path)
    conf["initial_state"] = "running"
    mocker.patch("freqtrade.exchange.binance.Binance.validate_config", MagicMock())
    bot = get_patched_freqtradebot(mocker, conf)
    assert bot.state == State.RUNNING


def test_persisted_running_never_overrides_explicit_config(mocker, default_conf_usdt, tmp_path):
    from tests.conftest import get_patched_freqtradebot

    conf = _live_pm_conf(default_conf_usdt, tmp_path)
    conf["initial_state"] = "stopped"
    persist_state(conf, State.RUNNING)
    mocker.patch("freqtrade.exchange.binance.Binance.validate_config", MagicMock())
    bot = get_patched_freqtradebot(mocker, conf)
    assert bot.state == State.STOPPED


def test_dry_run_bot_ignores_persisted_state(mocker, default_conf_usdt, tmp_path):
    from tests.conftest import get_patched_freqtradebot

    conf = _live_pm_conf(default_conf_usdt, tmp_path)
    conf["dry_run"] = True
    conf["initial_state"] = "running"
    persist_state(conf, State.PAUSED)
    mocker.patch("freqtrade.exchange.binance.Binance.validate_config", MagicMock())
    bot = get_patched_freqtradebot(mocker, conf)
    assert bot.state == State.RUNNING
