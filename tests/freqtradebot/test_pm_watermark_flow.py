"""
Bot-level tests for the durable PM candle-watermark wiring.

The watermark is the progress cursor of the signal decision ledger: it must
advance ONLY in the transaction that writes a new entry-scope decision row,
latch on a data discontinuity (missing closed candle), keep the pair
fail-closed until the hole is backfilled, and stay untouched by exit-scope
rows.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

from freqtrade.persistence import PMCandleWatermark, PMSignalLedger, init_db
from tests.conftest import get_patched_freqtradebot


@pytest.fixture(autouse=True)
def pm_db(default_conf_usdt):
    init_db(default_conf_usdt["db_url"])


BASE = datetime(2026, 9, 3, 0, 0)
PAIR = "ETH/USDT:USDT"
TF = "5m"


def _df(*offsets_min: int) -> pd.DataFrame:
    return pd.DataFrame({"date": [BASE + timedelta(minutes=m) for m in offsets_min]})


def _payload(candle: datetime, data_fresh: bool = True) -> dict:
    return {
        "candle_open_time": candle,
        "factor_hash": "h" * 64,
        "data_fresh": data_fresh,
        "freshness_detail": None,
        "signal_tag": None,
    }


def _make_bot(mocker, default_conf_usdt, payload_fn, df) -> "FreqtradeBot":  # noqa: F821
    bot = get_patched_freqtradebot(mocker, default_conf_usdt)
    mocker.patch.object(bot, "_pm_ledger_enabled", return_value=True)
    bot.strategy = MagicMock()
    bot.strategy.strategy_version = "TestStrategy-1"
    bot.strategy.get_strategy_name.return_value = "TestStrategy"
    bot.strategy.pm_signal_snapshot = lambda pair, dataframe: payload_fn(pair, dataframe)
    mocker.patch.object(bot.dataprovider, "get_analyzed_dataframe", return_value=(df, BASE))
    return bot


def _watermark() -> PMCandleWatermark:
    return PMCandleWatermark.get(PAIR, TF)


def test_first_decision_advances_cursor_same_transaction(mocker, default_conf_usdt):
    c0 = BASE
    bot = _make_bot(mocker, default_conf_usdt, lambda p, d: _payload(c0), _df(-5, 0))

    assert bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")

    wm = _watermark()
    assert wm is not None
    assert wm.last_decision_candle_open_time == c0
    assert not wm.gap_active
    row = PMSignalLedger.get_by_candle(PAIR, TF, c0)
    assert row is not None and row.decision == "no_signal"


def test_duplicate_candle_write_once_does_not_redecide(mocker, default_conf_usdt):
    c0 = BASE
    bot = _make_bot(mocker, default_conf_usdt, lambda p, d: _payload(c0), _df(-5, 0))

    assert bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")
    # A later signal for the same candle must not overwrite the first decision.
    assert not bot._pm_record_signal_decision(
        PAIR, "entry_submitted", reason="late", order_client_id="ft-late"
    )
    row = PMSignalLedger.get_by_candle(PAIR, TF, c0)
    assert row.decision == "no_signal"
    assert row.order_client_id is None
    assert _watermark().last_decision_candle_open_time == c0


def test_contiguous_next_candle_advances(mocker, default_conf_usdt):
    c0, c1 = BASE, BASE + timedelta(minutes=5)
    calls = {"n": 0}

    def payload(p, d):
        calls["n"] += 1
        return _payload(c1 if calls["n"] > 1 else c0)

    bot = _make_bot(mocker, default_conf_usdt, payload, _df(-10, -5, 0))
    assert bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")
    assert bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")
    assert _watermark().last_decision_candle_open_time == c1


def test_gap_latches_watermark_and_records_blocked_data(mocker, default_conf_usdt):
    c0, c2 = BASE, BASE + timedelta(minutes=10)
    calls = {"n": 0}

    def payload(p, d):
        calls["n"] += 1
        return _payload(c0 if calls["n"] == 1 else c2)

    bot = _make_bot(mocker, default_conf_usdt, payload, _df(-5, 0))
    assert bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")

    # c2 = c0 + 2 candles, and the dataframe now only holds c2: the candle
    # immediately before c2 is MISSING - a data discontinuity, not downtime.
    mocker.patch.object(bot.dataprovider, "get_analyzed_dataframe", return_value=(_df(10), BASE))
    # A NEW blocked_data row is recorded (returns True) - the entry itself
    # must be refused by the caller after seeing the decision.
    assert bot._pm_record_signal_decision(PAIR, "entry_submitted", reason="would-be-entry")

    wm = _watermark()
    assert wm.gap_active
    assert wm.gap_expected_open_time == c0 + timedelta(minutes=5)
    assert wm.gap_reason == "missing_closed_candle"
    assert wm.last_decision_candle_open_time == c0  # cursor stays behind
    row = PMSignalLedger.get_by_candle(PAIR, TF, c2)
    assert row is not None and row.decision == "blocked_data"
    assert "candle_gap" in (row.decision_reason or "")


def test_gap_unrecovered_keeps_blocking(mocker, default_conf_usdt):
    c0 = BASE
    current = {"candle": c0}
    bot = _make_bot(
        mocker, default_conf_usdt, lambda p, d: _payload(current["candle"]), _df(-5, 0)
    )
    assert bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")

    # Latch the gap first: the current candle jumps to c2 while c1 is missing.
    current["candle"] = BASE + timedelta(minutes=10)
    mocker.patch.object(bot.dataprovider, "get_analyzed_dataframe", return_value=(_df(10), BASE))
    bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")
    assert _watermark().gap_active

    # Later candles arrive but the missing c1 is still not backfilled.
    c3 = BASE + timedelta(minutes=15)
    current["candle"] = c3
    mocker.patch.object(bot.dataprovider, "get_analyzed_dataframe", return_value=(_df(10, 15), BASE))
    assert bot._pm_record_signal_decision(PAIR, "entry_submitted", reason="entry")

    row = PMSignalLedger.get_by_candle(PAIR, TF, c3)
    assert row is not None and row.decision == "blocked_data"
    assert "candle_gap_unrecovered" in (row.decision_reason or "")
    assert _watermark().last_decision_candle_open_time == c0

    # The real (unpatched) snapshot layer also reports the unrecovered latch
    # so execute_entry refuses the order up front.
    snapshot, block_reason = bot._pm_ledger_snapshot(PAIR)
    assert "candle_gap_unrecovered" in (block_reason or "")


def test_gap_recovery_clears_latch_and_advances(mocker, default_conf_usdt):
    c0 = BASE
    current = {"candle": c0}
    bot = _make_bot(
        mocker, default_conf_usdt, lambda p, d: _payload(current["candle"]), _df(-5, 0)
    )
    assert bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")

    current["candle"] = BASE + timedelta(minutes=10)
    mocker.patch.object(bot.dataprovider, "get_analyzed_dataframe", return_value=(_df(10), BASE))
    bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")
    assert _watermark().gap_active

    # The hole (c1) is backfilled; the series is contiguous again.
    c4 = BASE + timedelta(minutes=20)
    current["candle"] = c4
    mocker.patch.object(
        bot.dataprovider,
        "get_analyzed_dataframe",
        return_value=(_df(5, 10, 15, 20), BASE),
    )
    assert bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")

    wm = _watermark()
    assert not wm.gap_active
    assert wm.last_decision_candle_open_time == c4
    assert wm.gap_reason is None


def test_unhealthy_snapshot_forces_blocked_data(mocker, default_conf_usdt):
    c0 = BASE
    bot = _make_bot(mocker, default_conf_usdt, lambda p, d: _payload(c0, data_fresh=False), _df(-5, 0))

    assert bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")
    row = PMSignalLedger.get_by_candle(PAIR, TF, c0)
    assert row is not None and row.decision == "blocked_data"
    assert row.data_fresh is False
    assert _watermark().last_decision_candle_open_time == c0


def test_exit_scope_records_separately_and_never_touches_watermark(mocker, default_conf_usdt):
    c0 = BASE
    bot = _make_bot(mocker, default_conf_usdt, lambda p, d: _payload(c0), _df(-5, 0))

    assert bot._pm_record_signal_decision(PAIR, "entry_submitted", order_client_id="ft-1")
    assert bot._pm_record_signal_decision(
        PAIR, "exit", reason="exit_signal", decision_scope="exit"
    )

    entry_row = PMSignalLedger.get_by_candle(PAIR, TF, c0, "entry")
    exit_row = PMSignalLedger.get_by_candle(PAIR, TF, c0, "exit")
    assert entry_row.decision == "entry_submitted"
    assert exit_row is not None and exit_row.decision == "exit"
    assert _watermark().last_decision_candle_open_time == c0


def test_snapshot_none_records_nothing(mocker, default_conf_usdt):
    bot = _make_bot(mocker, default_conf_usdt, lambda p, d: None, _df(-5, 0))
    assert not bot._pm_record_signal_decision(PAIR, "no_signal", reason="no_entry_signal")
    assert _watermark() is None
    assert PMSignalLedger.get_by_candle(PAIR, TF, BASE) is None
