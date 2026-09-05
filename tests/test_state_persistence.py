"""
Tests for production bot-state persistence across restarts.

A bot that paused or stopped itself (risk fail-closed or operator command)
must stay in that state after a container restart / server reboot instead of
silently re-arming as RUNNING.  A corrupt state record must block
auto-trading entirely.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

from freqtrade.enums import State
from freqtrade.state_persistence import (
    PersistedState,
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
    assert read_persisted_state(conf) == PersistedState()
    for state in (State.PAUSED, State.RUNNING, State.STOPPED):
        persist_state(conf, state)
        assert read_persisted_state(conf) == PersistedState(state=state)
    # File lives under user_data/.freqtrade/state (the bind-mounted volume).
    assert (tmp_path / ".freqtrade" / "state").is_file()


def test_stopped_is_persisted_not_deleted(tmp_path):
    """An intentional /stop must survive a restart as STOPPED."""
    conf = live_conf(tmp_path)
    persist_state(conf, State.PAUSED)
    persist_state(conf, State.STOPPED)
    assert read_persisted_state(conf) == PersistedState(state=State.STOPPED)


def test_missing_file_is_no_record_not_corrupt(tmp_path):
    conf = live_conf(tmp_path)
    assert read_persisted_state(conf) == PersistedState(state=None, corrupt=False)


def test_corrupt_file_is_flagged(tmp_path, caplog):
    conf = live_conf(tmp_path)
    state_file_path(conf).parent.mkdir(parents=True)
    state_file_path(conf).write_text("{not json", encoding="utf-8")
    result = read_persisted_state(conf)
    assert result.corrupt is True
    assert result.state is None
    assert "corrupt" in caplog.text


def test_unknown_state_name_is_corrupt(tmp_path, caplog):
    conf = live_conf(tmp_path)
    state_file_path(conf).parent.mkdir(parents=True)
    state_file_path(conf).write_text(json.dumps({"state": "PAUSEDX"}), encoding="utf-8")
    result = read_persisted_state(conf)
    assert result.corrupt is True
    assert "unknown value" in caplog.text


def test_dry_run_never_persists_or_reads(tmp_path):
    conf = live_conf(tmp_path)
    conf["dry_run"] = True
    assert not persistence_enabled(conf)
    persist_state(conf, State.PAUSED)
    assert not (tmp_path / ".freqtrade" / "state").exists()
    assert read_persisted_state(conf) == PersistedState()


def test_internals_flag_disables(tmp_path):
    conf = live_conf(tmp_path)
    conf["internals"]["persist_state"] = False
    assert not persistence_enabled(conf)
    persist_state(conf, State.PAUSED)
    assert read_persisted_state(conf) == PersistedState()


def test_reload_config_never_persisted(tmp_path):
    conf = live_conf(tmp_path)
    persist_state(conf, State.RELOAD_CONFIG)
    assert read_persisted_state(conf) == PersistedState()


def test_set_state_persists_synchronously(mocker, default_conf_usdt, tmp_path):
    """The state change is durable at the assignment site - no worker iteration
    or notification happens in between, so a crash right after a pause/stop
    decision cannot lose it."""
    from tests.conftest import get_patched_freqtradebot

    conf = default_conf_usdt.copy()
    conf["dry_run"] = False
    conf["user_data_dir"] = tmp_path
    conf["exchange"]["portfolio_margin"] = False
    mocker.patch("freqtrade.exchange.binance.Binance.validate_config", MagicMock())
    bot = get_patched_freqtradebot(mocker, conf)

    bot.set_state(State.PAUSED)
    # Persisted IMMEDIATELY - before any notify/worker involvement.
    assert read_persisted_state(conf) == PersistedState(state=State.PAUSED)
    bot.set_state(State.STOPPED)
    assert read_persisted_state(conf) == PersistedState(state=State.STOPPED)


def test_rpc_pause_and_start_persist(mocker):
    """rpc state changes go through set_state (synchronous persistence)."""
    from freqtrade.rpc.rpc import RPC

    mocker.patch("freqtrade.rpc.rpc.CryptoToFiatConverter")
    bot = MagicMock()
    bot.config = {"fiat_display_currency": None, "dry_run": False}
    bot.state = State.RUNNING
    rpc = RPC(bot)
    rpc._rpc_pause()
    bot.set_state.assert_called_with(State.PAUSED)

    bot.state = State.PAUSED
    rpc._rpc_start()
    bot.set_state.assert_called_with(State.RUNNING)

    bot.state = State.RUNNING
    rpc._rpc_stop()
    bot.set_state.assert_called_with(State.STOPPED)


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


def _make_bot(mocker, conf):
    from tests.conftest import get_patched_freqtradebot

    mocker.patch("freqtrade.exchange.binance.Binance.validate_config", MagicMock())
    return get_patched_freqtradebot(mocker, conf)


def test_first_boot_uses_config(mocker, default_conf_usdt, tmp_path):
    """No record + configured running -> RUNNING (first boot, normal path)."""
    conf = _live_pm_conf(default_conf_usdt, tmp_path)
    conf["initial_state"] = "running"
    bot = _make_bot(mocker, conf)
    assert bot.state == State.RUNNING


def test_persisted_paused_restored(mocker, default_conf_usdt, tmp_path):
    conf = _live_pm_conf(default_conf_usdt, tmp_path)
    conf["initial_state"] = "running"
    persist_state(conf, State.PAUSED)
    bot = _make_bot(mocker, conf)
    assert bot.state == State.PAUSED


def test_persisted_stopped_restored(mocker, default_conf_usdt, tmp_path):
    """An intentional /stop is restored: the bot does NOT re-arm itself."""
    conf = _live_pm_conf(default_conf_usdt, tmp_path)
    conf["initial_state"] = "running"
    persist_state(conf, State.STOPPED)
    bot = _make_bot(mocker, conf)
    assert bot.state == State.STOPPED


def test_corrupt_state_blocks_auto_trading(mocker, default_conf_usdt, tmp_path, caplog):
    """A corrupt record must NOT fall back to the configured running state."""
    conf = _live_pm_conf(default_conf_usdt, tmp_path)
    conf["initial_state"] = "running"
    state_file_path(conf).parent.mkdir(parents=True, exist_ok=True)
    state_file_path(conf).write_text("{broken", encoding="utf-8")
    bot = _make_bot(mocker, conf)
    assert bot.state == State.PAUSED
    assert "corrupt" in caplog.text


def test_persisted_running_never_overrides_explicit_config(mocker, default_conf_usdt, tmp_path):
    conf = _live_pm_conf(default_conf_usdt, tmp_path)
    conf["initial_state"] = "stopped"
    persist_state(conf, State.RUNNING)
    bot = _make_bot(mocker, conf)
    assert bot.state == State.STOPPED


def test_dry_run_bot_ignores_persisted_state(mocker, default_conf_usdt, tmp_path):
    conf = _live_pm_conf(default_conf_usdt, tmp_path)
    conf["dry_run"] = True
    conf["initial_state"] = "running"
    persist_state(conf, State.PAUSED)
    bot = _make_bot(mocker, conf)
    assert bot.state == State.RUNNING
