"""
Tests for the PM signal decision ledger: write-once per candle, factor hashing,
freshness gating and the bot-level record hooks.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from freqtrade.persistence import PMSignalDecisionEvent, PMSignalLedger, init_db


@pytest.fixture(autouse=True)
def ledger_db(default_conf):
    init_db(default_conf["db_url"])


def candle(dt_str="2026-09-03 00:00:00"):
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


def test_record_once_write_once_per_candle():
    first = PMSignalLedger.record_once(
        pair="ETH/USDT:USDT",
        timeframe="5m",
        candle_open_time=candle(),
        strategy_version="VWAP_V4-1",
        factor_hash="abc",
        data_fresh=True,
        signal_tag="jc_trend_reclaim",
        decision="entry_submitted",
        order_client_id="ft123",
    )
    PMSignalLedger.session.commit()
    assert first.decision == "entry_submitted"

    # Second decision for the same candle: first write wins.
    second = PMSignalLedger.record_once(
        pair="ETH/USDT:USDT",
        timeframe="5m",
        candle_open_time=candle(),
        strategy_version="VWAP_V4-1",
        factor_hash="abc",
        data_fresh=True,
        signal_tag="jc_trend_reclaim",
        decision="blocked",
    )
    PMSignalLedger.session.commit()
    assert second.decision == "entry_submitted"
    assert len(PMSignalLedger.session.query(PMSignalLedger).all()) == 1


def test_set_order_client_id_backfills():
    PMSignalLedger.record_once(
        pair="ETH/USDT:USDT",
        timeframe="5m",
        candle_open_time=candle(),
        strategy_version="v",
        factor_hash="h",
        data_fresh=True,
        decision="entry_submitted",
    )
    PMSignalLedger.session.commit()
    PMSignalLedger.set_order_client_id(
        "ETH/USDT:USDT", "5m", candle(), "ft999"
    )
    PMSignalLedger.session.commit()
    row = PMSignalLedger.get_by_candle("ETH/USDT:USDT", "5m", candle())
    assert row.order_client_id == "ft999"


def test_recent_orders_by_pair():
    for i in range(3):
        PMSignalLedger.record_once(
            pair="ETH/USDT:USDT",
            timeframe="5m",
            candle_open_time=candle(f"2026-09-03 00:0{i}:00"),
            strategy_version="v",
            factor_hash=f"h{i}",
            data_fresh=True,
            decision="blocked",
            decision_reason="test",
        )
    PMSignalLedger.session.commit()
    recent = PMSignalLedger.recent(pair="ETH/USDT:USDT", limit=2)
    assert len(recent) == 2
    assert recent[0].candle_open_time >= recent[1].candle_open_time


def test_strategy_snapshot_shape():
    """VWAP_V4.pm_signal_snapshot returns the ledger contract (factor hash + freshness)."""
    import sys

    sys.path.insert(0, "user_data/strategies")
    try:
        from VWAP_V4 import VWAP_V4  # noqa: E402
    except ImportError as e:
        # The strategy needs talib/technical/numba; when the local environment
        # has incompatible numpy/numba versions the bot still runs in the pinned
        # Docker image - skip rather than fail the suite.
        pytest.skip(f"VWAP_V4 cannot be imported in this environment: {e}")

    strategy = VWAP_V4({"timeframe": "5m", "trading_mode": "futures"})
    df = pd.DataFrame(
        {
            "date": pd.date_range("2026-09-03", periods=5, freq="5min", tz="UTC"),
            "open": [100.0] * 5,
            "high": [101.0] * 5,
            "low": [99.0] * 5,
            "close": [100.5] * 5,
            "volume": [1000.0] * 5,
            "pair_1h_fresh": [1] * 5,
            "enter_tag": [None] * 4 + ["jc_trend_reclaim"],
        }
    )
    snapshot = strategy.pm_signal_snapshot("ETH/USDT:USDT", df)
    assert snapshot is not None
    assert len(snapshot["factor_hash"]) == 64
    assert snapshot["data_fresh"] is True
    assert snapshot["signal_tag"] == "jc_trend_reclaim"
    assert snapshot["candle_open_time"].tzinfo is None

    # stale 1h -> not fresh
    df["pair_1h_fresh"] = 0
    snapshot = strategy.pm_signal_snapshot("ETH/USDT:USDT", df)
    assert snapshot["data_fresh"] is False
    assert "1h_stale" in snapshot["freshness_detail"]

    # empty dataframe -> None (fail-closed)
    assert strategy.pm_signal_snapshot("ETH/USDT:USDT", pd.DataFrame()) is None


def test_decision_events_append_blocked_then_submitted_with_stable_id():
    snapshot = PMSignalLedger.record_once(
        pair="ETH/USDT:USDT",
        timeframe="5m",
        candle_open_time=candle(),
        strategy_version="VWAP_V4-1",
        factor_hash="f" * 64,
        data_fresh=True,
        decision="blocked",
        decision_reason="global_pairlock",
    )
    PMSignalLedger.session.flush()
    first, created_first = PMSignalDecisionEvent.append_event(
        signal_ledger_id=snapshot.id,
        pair=snapshot.pair,
        timeframe=snapshot.timeframe,
        candle_open_time=snapshot.candle_open_time,
        decision_scope=snapshot.decision_scope,
        strategy_version=snapshot.strategy_version,
        factor_hash=snapshot.factor_hash,
        decision="blocked",
        decision_reason="global_pairlock",
    )
    second, created_second = PMSignalDecisionEvent.append_event(
        signal_ledger_id=snapshot.id,
        pair=snapshot.pair,
        timeframe=snapshot.timeframe,
        candle_open_time=snapshot.candle_open_time,
        decision_scope=snapshot.decision_scope,
        strategy_version=snapshot.strategy_version,
        factor_hash=snapshot.factor_hash,
        decision="entry_submitted",
        decision_reason="order_id=123",
        order_client_id="ft-123",
    )
    duplicate, created_duplicate = PMSignalDecisionEvent.append_event(
        signal_ledger_id=snapshot.id,
        pair=snapshot.pair,
        timeframe=snapshot.timeframe,
        candle_open_time=snapshot.candle_open_time,
        decision_scope=snapshot.decision_scope,
        strategy_version=snapshot.strategy_version,
        factor_hash=snapshot.factor_hash,
        decision="entry_submitted",
        decision_reason="order_id=123",
        order_client_id="ft-123",
    )
    PMSignalLedger.session.commit()

    assert created_first is True
    assert created_second is True
    assert created_duplicate is False
    assert first.decision_id == second.decision_id == duplicate.decision_id
    assert [e.decision for e in PMSignalDecisionEvent.recent_for_decision(first.decision_id)] == [
        "blocked",
        "entry_submitted",
    ]
