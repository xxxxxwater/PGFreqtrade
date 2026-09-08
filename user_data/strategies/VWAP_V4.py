import hashlib
import json
import logging
import math
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
import pandas_ta as pta
import talib.abstract as ta
from pandas import DataFrame, Series
from technical.indicators import RMI

import freqtrade.vendor.qtpylib.indicators as qtpylib
from freqtrade.persistence import Trade
from freqtrade.exchange import timeframe_to_prev_date, timeframe_to_seconds
from freqtrade.strategy import DecimalParameter, IntParameter, stoploss_from_open
from freqtrade.strategy.interface import IStrategy
from freqtrade.enums import CandleType


logger = logging.getLogger(__name__)



def top_percent_change_dca(dataframe: DataFrame, length: int) -> float:
    """
    Percentage change of the current close from the range maximum Open price
    :param dataframe: DataFrame The original OHLC dataframe
    :param length: int The length to look back
    """
    if length == 0:
        return (dataframe['open'] - dataframe['close']) / dataframe['close']
    else:
        return (dataframe['open'].rolling(length).max() - dataframe['close']) / dataframe['close']

def EWO(dataframe, ema_length=5, ema2_length=3):
    df = dataframe.copy()
    ema1 = ta.EMA(df, timeperiod=ema_length)
    ema2 = ta.EMA(df, timeperiod=ema2_length)
    emadif = (ema1 - ema2) / df['close'] * 100
    return emadif

def VWAPB(dataframe, window_size=20, num_of_std=1):
    df = dataframe.copy()
    df['vwap'] = qtpylib.rolling_vwap(df, window=window_size)
    rolling_std = df['vwap'].rolling(window=window_size).std()
    df['vwap_low'] = df['vwap'] - (rolling_std * num_of_std)
    df['vwap_high'] = df['vwap'] + (rolling_std * num_of_std)
    return df['vwap_low'], df['vwap'], df['vwap_high']

def chaikin_money_flow(dataframe, n=20, fillna=False) -> Series:
    """Chaikin Money Flow (CMF)
    它衡量特定时期的资金流量。
    http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:chaikin_money_flow_cmf
    Args:
        dataframe(pandas.Dataframe): dataframe containing ohlcv
        n(int): n period.
        fillna(bool): if True, fill nan values.
    Returns:
        pandas.Series: New feature generated.
    """
    mfv = ((dataframe['close'] - dataframe['low']) - (dataframe['high'] - dataframe['close'])) / (dataframe['high'] - dataframe['low'])
    mfv = mfv.fillna(0.0)  # float division by zero
    mfv *= dataframe['volume']
    cmf = (mfv.rolling(n, min_periods=0).sum()
           / dataframe['volume'].rolling(n, min_periods=0).sum())
    if fillna:
        cmf = cmf.replace([np.inf, -np.inf], np.nan).fillna(0)
    return Series(cmf, name='cmf')

def williams_r(dataframe: DataFrame, period: int = 14) -> Series:
    """Williams %R oscillator, from -100 (oversold) up to 0 (overbought)."""
    highest_high = dataframe["high"].rolling(center=False, window=period).max()
    lowest_low = dataframe["low"].rolling(center=False, window=period).min()
    wr = Series(
        (highest_high - dataframe["close"]) / (highest_high - lowest_low),
        name=f"{period} Williams %R",
    )
    return wr * -100

def merge_shifted_informative_columns(
    dataframe: DataFrame,
    informative: DataFrame,
    columns: list[str],
    timeframe: str,
) -> DataFrame:
    dataframe['date'] = pd.to_datetime(dataframe['date'], utc=True).astype('datetime64[ns, UTC]')
    informative = informative[['date', *columns]].copy()
    informative['date'] = pd.to_datetime(informative['date'], utc=True).astype('datetime64[ns, UTC]')

    if timeframe.endswith('h'):
        informative['date'] = informative['date'] + pd.Timedelta(hours=int(timeframe[:-1]))
    elif timeframe.endswith('m'):
        informative['date'] = informative['date'] + pd.Timedelta(minutes=int(timeframe[:-1]))
    else:
        raise ValueError(f"Unsupported informative timeframe: {timeframe}")

    return pd.merge_asof(
        dataframe.sort_values('date'),
        informative.sort_values('date'),
        on='date',
        direction='backward',
    )

class VWAP_V4(IStrategy):
    """
    PASTE OUTPUT FROM HYPEROPT HERE
    Can be overridden for specific sub-strategies (stake currencies) at the bottom.
    MaxDrawDownRelativeHyperOptLoss     
    31/900:    801 trades. 800/0/1 Wins/Draws/Losses. Avg profit   4.04%. Median profit   3.70%. Total profit 57.02887841 USDT (   5.70%). Avg duration 3:05:00 min. Objective: -14799833083.36105
    """
    # The PM adapter and the callbacks below use the current Freqtrade v3
    # strategy contract (long/short aware callbacks and after_fill stoplosses).
    INTERFACE_VERSION = 3

    sell_params = {
        "high_offset": 1.012,
        "high_offset_2": 1.016,
        "sell_deadfish_bb_factor": 1.089,
        "sell_deadfish_bb_width": 0.11,
        "sell_deadfish_profit": -0.107,
        "sell_deadfish_volume_factor": 1.761,
        "base_nb_candles_sell": 22,  # value loaded from strategy
        # A -99% spot-style fail-safe is not appropriate for a cross-margin
        # account.  The DCA ladder reaches -18%, so leave room for it but keep
        # a real per-position emergency stop.  Re-optimise this with futures
        # data before raising live limits.
        "pHSL": -0.35,  # value loaded from strategy
        "pPF_1": 0.012,  # value loaded from strategy
        "pPF_2": 0.07,  # value loaded from strategy
        "pSL_1": 0.015,  # value loaded from strategy
        "pSL_2": 0.068,  # value loaded from strategy
        "sell_trail_down_1": 0.03,  # value loaded from strategy
        "sell_trail_down_2": 0.015,  # value loaded from strategy
        "sell_trail_profit_max_1": 0.4,  # value loaded from strategy
        "sell_trail_profit_max_2": 0.11,  # value loaded from strategy
        "sell_trail_profit_min_1": 0.1,  # value loaded from strategy
        "sell_trail_profit_min_2": 0.04,  # value loaded from strategy
    }
    
 #   minimal_roi = {
 #       "0": 0.276,
 #       "32": 0.105,
 #       "88": 0.037,
 #       "208": 0
 #   }

    minimal_roi = {
        "0":     0.075,    # 即时目标 7.5%（原 1.2%）
        "240":   0.0375,   # 4 小时后 3.75%
        "2880":  0.0125,   # 48 小时后 1.25%
        "10080": 0         # 一周后清仓线
    }
    
    position_adjustment_enable = True
    # `entry_stake_amount` is an upper bound for the *first* entry, not the
    # total position.  The helpers below reserve capital for all five DCA
    # orders before allowing a new position.
    # 2x PM leverage: a 25,000 USDT collateral stake opens a 50,000 USDT
    # notional initial position.  Reserve the complete five-order DCA ladder
    # (multiplier 7.71561) before permitting a new trade.
    entry_stake_amount = 25000.0
    max_position_stake = 192890.25
    pm_target_leverage = 2.0
    max_entry_position_adjustment = 5
    protections = [
        {
            "method": "LowProfitPairs",
            "lookback_period": 120,
            "trade_limit": 2,
            "stop_duration": 90,
            "required_profit": -0.04,
        },
        {
            "method": "StoplossGuard",
            "lookback_period": 240,
            "trade_limit": 2,
            "stop_duration": 120,
            "required_profit": 0.0,
            "only_per_pair": True,
        },
        {
            "method": "MaxDrawdown",
            "lookback_period": 360,
            "trade_limit": 4,
            "stop_duration": 90,
            "max_allowed_drawdown": 0.12,
        }
    ]
    stoploss = -0.35
    trailing_stop = False
    trailing_stop_positive = 0.02  # povodne 0.001
    trailing_stop_positive_offset = 0.10  # povodne 0.012
    trailing_only_offset_is_reached = True
    """
    END HYPEROPT
    """
    
    timeframe = '5m'
    can_short = False
    use_exit_signal = True
    exit_profit_only = False

    # Durable signal-decision ledger identity: bump whenever factor logic changes
    # so historical ledger rows can be mapped to the strategy version that
    # produced them.
    strategy_version = "VWAP_V4-2026.09-ledger-v1"

    # Factor columns hashed into the per-candle signal snapshot. Only these
    # values define a signal - anything not listed is considered cosmetic.
    _signal_ledger_factor_columns = [
        "close", "volume", "rsi", "rsi_fast", "rsi_slow", "ema_5", "ema_10",
        "ema_16", "ema_50", "ema_200", "EWO", "cti", "r_14", "cmf",
        "bb_width", "bb_delta", "bb_lowerband2", "bb_lowerband3",
        "vwap_lowerband", "ema_vwap_diff_50", "tcp_percent_4",
        "tpct_change_0", "srsi_fk", "tail", "closedelta", "bb_delta_cluc",
        "hma_50", "btc_close", "down", "pair_1h_fresh",
    ]

    @staticmethod
    def _pm_diag_bool(value) -> bool:
        """Observability-only truth conversion: NaN/missing is always a failed predicate."""
        try:
            return False if value is None or pd.isna(value) else bool(value)
        except (TypeError, ValueError):
            return False

    def pm_no_signal_detail(self, dataframe: DataFrame) -> str | None:
        """Return the first failed predicate for each entry branch on the latest candle.

        This method is diagnostic only.  It is called AFTER ``populate_entry_trend``
        and never feeds ``enter_long`` or sizing/order logic.  Keeping one concise
        failure per branch makes a durable no-signal ledger row explainable without
        turning routine no-signal candles into log spam.
        """
        if dataframe is None or dataframe.empty:
            return "no_dataframe"
        last = dataframe.iloc[-1]
        if self._pm_diag_bool(last.get('enter_long', 0)):
            return None

        def value(column: str, offset: int = 0):
            idx = len(dataframe) - 1 - offset
            if idx < 0 or column not in dataframe.columns:
                return np.nan
            return dataframe[column].iat[idx]

        def finite_number(raw) -> float | None:
            try:
                number = float(raw)
                return number if math.isfinite(number) else None
            except (TypeError, ValueError):
                return None

        def gt(a, b) -> bool:
            av, bv = finite_number(a), finite_number(b)
            return av is not None and bv is not None and av > bv

        def ge(a, b) -> bool:
            av, bv = finite_number(a), finite_number(b)
            return av is not None and bv is not None and av >= bv

        def lt(a, b) -> bool:
            av, bv = finite_number(a), finite_number(b)
            return av is not None and bv is not None and av < bv

        def le(a, b) -> bool:
            av, bv = finite_number(a), finite_number(b)
            return av is not None and bv is not None and av <= bv

        def first_failed(branch: str, checks: list[tuple[str, bool]]) -> str:
            failed = next((name for name, passed in checks if not passed), "unknown")
            return f"{branch}:{failed}"

        rolling_low_reclaim = (
            finite_number(dataframe['low'].rolling(self.trend_reclaim_lookback).min().iat[-1])
            if 'low' in dataframe.columns
            else None
        )
        rolling_low_retest = (
            finite_number(dataframe['low'].rolling(self.trend_retest_lookback).min().iat[-1])
            if 'low' in dataframe.columns
            else None
        )
        close48_max = (
            finite_number(dataframe['close'].rolling(48).max().iat[-1])
            if 'close' in dataframe.columns
            else None
        )
        btc24_max = (
            finite_number(dataframe['btc_close'].rolling(24).max().iat[-1])
            if 'btc_close' in dataframe.columns
            else None
        )

        reclaim = [
            ('1h_fresh', self._pm_diag_bool(value('pair_1h_fresh'))),
            ('trend_ok', finite_number(value('pair_trend_ok_1h')) == 1),
            ('not_extended', finite_number(value('pair_not_overextended_1h')) == 1),
            ('ema50_slope', gt(value('ema_50'), (finite_number(value('ema_50', 12)) or math.inf) * 0.999)),
            ('strong_1h', finite_number(value('pair_trend_strong_1h')) == 1),
            ('ema50_gt_200', gt(value('ema_50'), value('ema_200'))),
            ('ema5_cross', gt(value('ema_5'), value('ema_10')) and le(value('ema_5', 1), value('ema_10', 1))),
            ('pullback_low', rolling_low_reclaim is not None and lt(rolling_low_reclaim, (finite_number(value('ema_16')) or -math.inf) * self.trend_reclaim_low_ratio)),
            ('reclaim_close', gt(value('close'), (finite_number(value('ema_16')) or math.inf) * self.trend_reclaim_close_floor) and lt(value('close'), (finite_number(value('ema_16')) or -math.inf) * self.trend_reclaim_close_ceiling)),
            ('rsi', ge(value('rsi'), self.trend_reclaim_rsi_min) and le(value('rsi'), self.trend_reclaim_rsi_max)),
            ('cti', ge(value('cti'), self.trend_reclaim_cti_min) and lt(value('cti'), self.trend_reclaim_cti_max)),
            ('ewo', gt(value('EWO'), 0)),
            ('volume', gt(value('volume'), value('volume_mean_24'))),
        ]
        retest = [
            ('1h_fresh', self._pm_diag_bool(value('pair_1h_fresh'))),
            ('trend_ok', finite_number(value('pair_trend_ok_1h')) == 1),
            ('not_extended', finite_number(value('pair_not_overextended_1h')) == 1),
            ('ema50_slope', gt(value('ema_50'), (finite_number(value('ema_50', 12)) or math.inf) * 0.999)),
            ('strong_1h', finite_number(value('pair_trend_strong_1h')) == 1),
            ('ema50_gt_200', gt(value('ema_50'), value('ema_200'))),
            ('retest_cross', gt(value('close'), value('ema_16')) and le(value('close', 1), value('ema_16', 1))),
            ('pullback_low', rolling_low_retest is not None and lt(rolling_low_retest, (finite_number(value('ema_16')) or -math.inf) * self.trend_retest_low_ratio)),
            ('close_ceiling', lt(value('close'), (finite_number(value('ema_16')) or -math.inf) * self.trend_retest_close_ceiling)),
            ('rsi', ge(value('rsi'), self.trend_retest_rsi_min) and le(value('rsi'), self.trend_retest_rsi_max)),
            ('cti', ge(value('cti'), self.trend_retest_cti_min) and lt(value('cti'), self.trend_retest_cti_max)),
            ('ewo', gt(value('EWO'), 0)),
            ('volume', gt(value('volume'), (finite_number(value('volume_mean_24')) or math.inf) * self.trend_retest_volume_ratio)),
        ]
        insta = [
            ('1h_fresh', self._pm_diag_bool(value('pair_1h_fresh'))),
            ('bb1h', gt(value('bb_width_1h'), 0.131)),
            ('r14', lt(value('r_14'), -51)),
            ('r84_1h', lt(value('r_84_1h'), -70)),
            ('cti', lt(value('cti'), -0.845)),
            ('cti40_1h', lt(value('cti_40_1h'), -0.735)),
            ('pair_drawdown', close48_max is not None and ge(close48_max, (finite_number(value('close')) or math.inf) * 1.1)),
            ('btc_drawdown', btc24_max is not None and ge(btc24_max, (finite_number(value('btc_close')) or math.inf) * 1.03)),
        ]
        dip = [
            ('1h_fresh', self._pm_diag_bool(value('pair_1h_fresh'))),
            ('rmi', lt(value(f'rmi_length_{self.buy_rmi_length.value}'), self.buy_rmi.value)),
            ('cci', le(value(f'cci_length_{self.buy_cci_length.value}'), self.buy_cci.value)),
            ('srsi', lt(value('srsi_fk'), self.buy_srsi_fk.value)),
            ('bb_delta', gt(value('bb_delta'), self.buy_bb_delta.value)),
            ('bb_width', gt(value('bb_width'), self.buy_bb_width.value)),
            ('closedelta', gt(value('closedelta'), (finite_number(value('close')) or math.inf) * self.buy_closedelta.value / 1000)),
            ('bb_factor', lt(value('close'), (finite_number(value('bb_lowerband3')) or -math.inf) * self.buy_bb_factor.value)),
            ('roc_1h', lt(value('roc_1h'), self.buy_roc_1h.value)),
            ('bb_width_1h', lt(value('bb_width_1h'), self.buy_bb_width_1h.value)),
        ]
        vwap = [
            ('below_vwap', lt(value('close'), value('vwap_lowerband'))),
            ('tcp4', gt(value('tcp_percent_4'), 0.053)),
            ('cti', lt(value('cti'), -0.8)),
            ('rsi', lt(value('rsi'), 35)),
            ('rsi84', lt(value('rsi_84'), 60)),
            ('rsi112', lt(value('rsi_112'), 60)),
            ('volume', gt(value('volume'), 0)),
        ]
        nfix = [
            ('ema12', gt(value('ema_200'), (finite_number(value('ema_200', 12)) or math.inf) * 1.01)),
            ('ema48', gt(value('ema_200'), (finite_number(value('ema_200', 48)) or math.inf) * 1.07)),
            ('bb40_prev', gt(value('bb_lowerband2_40', 1), 0)),
            ('bb_delta', gt(value('bb_delta_cluc'), (finite_number(value('close')) or math.inf) * 0.056)),
            ('closedelta', gt(value('closedelta'), (finite_number(value('close')) or math.inf) * 0.01)),
            ('tail', lt(value('tail'), (finite_number(value('bb_delta_cluc')) or -math.inf) * 0.5)),
            ('below_bb40_prev', lt(value('close'), value('bb_lowerband2_40', 1))),
            ('nonrising', le(value('close'), value('close', 1))),
            ('ema50_floor', gt(value('close'), (finite_number(value('ema_50')) or math.inf) * 0.912)),
        ]
        return ';'.join(
            [
                first_failed('reclaim', reclaim),
                first_failed('retest', retest),
                first_failed('insta', insta),
                first_failed('dip', dip),
                first_failed('vwap', vwap),
                first_failed('nfix', nfix),
            ]
        )

    def pm_signal_snapshot(self, pair: str, dataframe: DataFrame) -> dict | None:
        """
        Durable per-candle signal snapshot for the PM decision ledger.

        Returns None when there is no usable closed candle - the caller must
        treat that as fail-closed (no decision may be recorded without factors).
        """
        if dataframe is None or dataframe.empty:
            return None
        last = dataframe.iloc[-1]
        if last.get("date") is None or pd.isna(last.get("date")):
            return None
        columns = [c for c in self._signal_ledger_factor_columns if c in dataframe.columns]
        payload: dict = {}
        for column in columns:
            value = last.get(column)
            if value is None or pd.isna(value):
                payload[column] = None
                continue
            try:
                payload[column] = float(value)
            except (TypeError, ValueError):
                payload[column] = str(value)
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        volume_ok = float(last.get("volume", 0) or 0) > 0
        fresh_1h = bool(last.get("pair_1h_fresh"))
        detail = ""
        if not fresh_1h:
            detail = "1h_stale"
        if not volume_ok:
            detail = (detail + "," if detail else "") + "volume_zero"
        tag = last.get("enter_tag")
        no_signal_detail = (
            self.pm_no_signal_detail(dataframe)
            if not self._pm_diag_bool(last.get("enter_long", 0))
            else None
        )
        return {
            "factor_hash": digest,
            "data_fresh": fresh_1h and volume_ok,
            "freshness_detail": detail or None,
            "signal_tag": str(tag) if tag is not None and str(tag) != "None" else None,
            "no_signal_detail": no_signal_detail,
            "candle_open_time": pd.to_datetime(last["date"], utc=True)
            .to_pydatetime()
            .replace(tzinfo=None),
        }
    ignore_roi_if_entry_signal = False
    use_custom_stoploss = True
    process_only_new_candles = True
    startup_candle_count = 240
    # Live-only, rate-limited observability.  The values are measured locally
    # and never alter factor values, signal generation, sizing or order flow.
    metrics_window_seconds = 300
    metrics_history_size = 512
    # --- 1h data freshness (live only) ---
    # A 1h candle is naturally up to one hour old before the next one closes.
    # Therefore this gate measures missing *completed candles*, not elapsed
    # wall-clock age.  Block only when the 1h series is at least one full
    # candle behind the latest 5m candle being evaluated.  5m-only branches
    # (vwap / NFIX39) are never blocked by this gate.
    pair_1h_max_missing_candles = 0
    # After an hour boundary, force a targeted REST refresh of stale 1h data
    # for up to this many seconds past the hour.
    pair_1h_force_refresh_window_s = 600
    # Per-pair cooldown between two forced 1h refresh attempts.
    pair_1h_force_refresh_cooldown_s = 120
    # An entry signal can remain true during a whole 5m candle while the bot
    # loop runs several times.  When the live orderbook guard rejects that
    # initial entry, do not re-query the book / re-attempt the same signal
    # until the next strategy candle.  This is deliberately scoped to
    # ``confirm_trade_entry``: Freqtrade invokes that callback only for
    # ``mode="initial"``; DCA uses the existing Trade / ``pos_adjust`` path.
    liquidity_entry_reject_cooldown_seconds = 300
    order_types = {
        'entry': 'market',
        'exit': 'market',
        'emergency_exit': 'market',
        'force_entry': "market",
        'force_exit': 'market',
        'stoploss': 'market',
        'stoploss_on_exchange': True,
        'stoploss_on_exchange_interval': 60,
        'stoploss_on_exchange_limit_ratio': 0.99
    }
    
    def bot_start(self, **kwargs) -> None:
        self._vwap_metrics = self._new_metrics_state()
        self._entry_liquidity_cooldowns = {}
        try:
            dca_multiplier = self._dca_total_multiplier()
            initial_stake = min(
                self.entry_stake_amount,
                self.max_position_stake / dca_multiplier,
            )
            logger.info(
                "PM stake plan: leverage="
                f"{self.pm_target_leverage:.0f}x, initial entry up to "
                f"{initial_stake:.2f} USDT, DCA multiplier={dca_multiplier:.4f}, "
                f"per-position cap={self.max_position_stake:.2f} USDT."
            )
        except Exception as exception:
            logger.info(
                "PM stake plan could not be fully calculated at startup; "
                f"entry cap={self.entry_stake_amount:.2f} USDT, error={exception}"
            )
        logger.info(
            "VWAP_V4 observability enabled: window=%ss, rolling_history=%s samples.",
            self.metrics_window_seconds,
            self.metrics_history_size,
        )

    fast_ewo = 50
    slow_ewo = 200
    pair_trend_close_floor = 0.960
    pair_trend_ema20_floor = 0.985
    pair_trend_ema50_floor = 0.970
    pair_trend_slope_floor = 0.980
    pair_trend_max_extension = 1.080
    trend_reclaim_lookback = 12
    trend_reclaim_low_ratio = 0.995
    trend_reclaim_close_floor = 1.000
    trend_reclaim_close_ceiling = 1.010
    trend_reclaim_rsi_min = 44
    trend_reclaim_rsi_max = 56
    trend_reclaim_cti_min = -0.65
    trend_reclaim_cti_max = -0.10
    trend_retest_lookback = 24
    trend_retest_low_ratio = 0.992
    trend_retest_close_ceiling = 1.012
    trend_retest_rsi_min = 44
    trend_retest_rsi_max = 57
    trend_retest_cti_min = -0.75
    trend_retest_cti_max = 0.10
    trend_retest_volume_ratio = 0.70
    
    is_optimize_deadfish = True
    sell_deadfish_bb_width = DecimalParameter(0.03, 0.75, default=0.05, space='sell', optimize=is_optimize_deadfish)
    sell_deadfish_profit = DecimalParameter(-0.15, -0.05, default=-0.08, space='sell', optimize=is_optimize_deadfish)
    sell_deadfish_bb_factor = DecimalParameter(0.90, 1.20, default=1.0, space='sell', optimize=is_optimize_deadfish)
    sell_deadfish_volume_factor = DecimalParameter(1, 2.5, default=1.5, space='sell', optimize=is_optimize_deadfish)
    
    base_nb_candles_sell = IntParameter(8, 20, default=sell_params['base_nb_candles_sell'], space='sell', optimize=False)
    high_offset = DecimalParameter(1.005, 1.015, default=sell_params['high_offset'], space='sell', optimize=True)
    high_offset_2 = DecimalParameter(1.010, 1.020, default=sell_params['high_offset_2'], space='sell', optimize=True)
    
    sell_trail_profit_min_1 = DecimalParameter(0.1, 0.25, default=0.1, space='sell', decimals=3, optimize=False, load=True)
    sell_trail_profit_max_1 = DecimalParameter(0.3, 0.5, default=0.4, space='sell', decimals=2, optimize=False, load=True)
    sell_trail_down_1 = DecimalParameter(0.04, 0.1, default=0.03, space='sell', decimals=3, optimize=False, load=True)
    
    sell_trail_profit_min_2 = DecimalParameter(0.04, 0.1, default=0.04, space='sell', decimals=3, optimize=False, load=True)
    sell_trail_profit_max_2 = DecimalParameter(0.08, 0.25, default=0.11, space='sell', decimals=2, optimize=False, load=True)
    sell_trail_down_2 = DecimalParameter(0.04, 0.2, default=0.015, space='sell', decimals=3, optimize=False, load=True)
    
    pHSL = DecimalParameter(-0.990, -0.040, default=sell_params['pHSL'], decimals=3, space='sell', optimize=False, load=True)
    pPF_1 = DecimalParameter(0.008, 0.020, default=0.016, decimals=3, space='sell', optimize=False, load=True)
    pSL_1 = DecimalParameter(0.008, 0.020, default=0.011, decimals=3, space='sell', optimize=False, load=True)
    pPF_2 = DecimalParameter(0.040, 0.100, default=0.080, decimals=3, space='sell', optimize=False, load=True)
    pSL_2 = DecimalParameter(0.020, 0.070, default=0.040, decimals=3, space='sell', optimize=False, load=True)

    # Oversold entry parameters (ported from VWAP_ZQ DIP signal)
    is_optimize_dip = False
    buy_rmi = IntParameter(30, 50, default=35, optimize=is_optimize_dip)
    buy_cci = IntParameter(-135, -90, default=-133, optimize=is_optimize_dip)
    buy_srsi_fk = IntParameter(30, 50, default=25, optimize=is_optimize_dip)
    buy_cci_length = IntParameter(25, 45, default=25, optimize=is_optimize_dip)
    buy_rmi_length = IntParameter(8, 20, default=8, optimize=is_optimize_dip)

    is_optimize_break = False
    buy_bb_width = DecimalParameter(0.065, 0.135, default=0.095, optimize=is_optimize_break)
    buy_bb_delta = DecimalParameter(0.018, 0.035, default=0.025, optimize=is_optimize_break)

    is_optimize_check = False
    buy_roc_1h = IntParameter(-25, 200, default=10, optimize=is_optimize_check)
    buy_bb_width_1h = DecimalParameter(0.3, 2.0, default=0.3, optimize=is_optimize_check)

    is_optimize_local_uptrend = False
    buy_bb_factor = DecimalParameter(0.990, 0.999, default=0.995, optimize=False)
    buy_closedelta = DecimalParameter(12.0, 18.0, default=15.0, optimize=is_optimize_local_uptrend)

    orderbook_depth_levels = 20
    # Require 2x the initial order notional within 2% of the best price.
    # This remains a market-order slippage safeguard while admitting PM
    # perpetuals whose liquidity is distributed beyond the first 1% band.
    orderbook_depth_price_band = 0.02
    min_depth_to_stake_ratio = 2.0
    max_entry_spread_ratio = 0.00405
    max_entry_slippage_ratio = 0.0035
    max_exit_slippage_ratio = 0.005
    @staticmethod
    def _normalize_orderbook_side(levels) -> list[tuple[float, float]]:
        normalized = []
        if not isinstance(levels, list):
            return normalized
        for level in levels:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            try:
                price = float(level[0])
                amount = float(level[1])
            except (TypeError, ValueError):
                continue
            if price > 0 and amount > 0:
                normalized.append((price, amount))
        return normalized

    @staticmethod
    def _depth_quote_within(levels: list[tuple[float, float]], lower: float, upper: float) -> float:
        return sum(price * amount for price, amount in levels if lower <= price <= upper)

    @staticmethod
    def _estimate_market_buy_slippage(asks: list[tuple[float, float]], stake_amount: float, reference_rate: float) -> float:
        remaining_quote = stake_amount
        spent_quote = 0.0
        acquired_base = 0.0
        for price, amount in sorted(asks, key=lambda item: item[0]):
            quote_available = price * amount
            quote_take = min(remaining_quote, quote_available)
            if quote_take <= 0:
                continue
            spent_quote += quote_take
            acquired_base += quote_take / price
            remaining_quote -= quote_take
            if remaining_quote <= 1e-9:
                break
        if remaining_quote > 1e-6 or acquired_base <= 0 or reference_rate <= 0:
            return math.inf
        average_price = spent_quote / acquired_base
        return max(0.0, (average_price / reference_rate) - 1)

    @staticmethod
    def _estimate_market_sell_slippage(bids: list[tuple[float, float]], base_amount: float, reference_rate: float) -> float:
        remaining_base = base_amount
        received_quote = 0.0
        sold_base = 0.0
        for price, amount in sorted(bids, key=lambda item: item[0], reverse=True):
            base_take = min(remaining_base, amount)
            if base_take <= 0:
                continue
            received_quote += base_take * price
            sold_base += base_take
            remaining_base -= base_take
            if remaining_base <= 1e-12:
                break
        if remaining_base > 1e-9 or sold_base <= 0 or reference_rate <= 0:
            return math.inf
        average_price = received_quote / sold_base
        return max(0.0, 1 - (average_price / reference_rate))

    def _liquidity_entry_block_reason(self, pair: str, rate: float, amount: float) -> str | None:
        if self._is_backtest_mode() or not self.dp:
            return None
        try:
            orderbook = self.dp.orderbook(pair, self.orderbook_depth_levels)
        except Exception as exception:
            return f'orderbook unavailable: {exception}'

        asks = self._normalize_orderbook_side(orderbook.get('asks') if isinstance(orderbook, dict) else None)
        bids = self._normalize_orderbook_side(orderbook.get('bids') if isinstance(orderbook, dict) else None)
        if not asks or not bids:
            return 'orderbook missing bids or asks'

        best_ask = min(price for price, _ in asks)
        best_bid = max(price for price, _ in bids)
        if best_ask <= 0 or best_bid <= 0 or best_bid > best_ask:
            return f'invalid orderbook top: bid={best_bid}, ask={best_ask}'

        mid = (best_bid + best_ask) / 2
        spread_ratio = (best_ask - best_bid) / mid
        if spread_ratio > self.max_entry_spread_ratio:
            return f'spread too wide: {spread_ratio:.4%}'

        stake_amount = float(amount or 0) * float(rate or best_ask)
        if stake_amount <= 0:
            return f'invalid stake amount for liquidity check: amount={amount}, rate={rate}'

        base_amount = stake_amount / best_ask
        required_depth = stake_amount * self.min_depth_to_stake_ratio
        band = self.orderbook_depth_price_band
        ask_depth = self._depth_quote_within(asks, best_ask, best_ask * (1 + band))
        bid_depth = self._depth_quote_within(bids, best_bid * (1 - band), best_bid)
        if ask_depth < required_depth:
            return f'ask depth too thin: {ask_depth:.2f} < {required_depth:.2f}'
        if bid_depth < required_depth:
            return f'bid depth too thin: {bid_depth:.2f} < {required_depth:.2f}'

        entry_slippage = self._estimate_market_buy_slippage(asks, stake_amount, best_ask)
        if entry_slippage > self.max_entry_slippage_ratio:
            return f'estimated entry slippage too high: {entry_slippage:.4%}'
        exit_slippage = self._estimate_market_sell_slippage(bids, base_amount, best_bid)
        if exit_slippage > self.max_exit_slippage_ratio:
            return f'estimated exit slippage too high: {exit_slippage:.4%}'
        return None

    def _entry_liquidity_cooldown_until(self, pair: str, current_time: datetime) -> datetime | None:
        """Return the active initial-entry liquidity cooldown for ``pair``.

        The cache is intentionally in-memory and only suppresses duplicate
        attempts within a live candle.  A process restart clears it; PM's
        durable intent store, not this cache, is the protection against order
        duplication across restarts or uncertain exchange responses.
        """
        cooldowns = getattr(self, '_entry_liquidity_cooldowns', {})
        until = cooldowns.get(pair)
        if until is None:
            return None
        if current_time >= until:
            cooldowns.pop(pair, None)
            self._entry_liquidity_cooldowns = cooldowns
            return None
        return until

    def _set_entry_liquidity_cooldown(self, pair: str, current_time: datetime) -> datetime:
        """Suppress initial-entry retries through the end of the current candle."""
        candle_open = timeframe_to_prev_date(self.timeframe, current_time)
        candle_end = candle_open + timedelta(seconds=timeframe_to_seconds(self.timeframe))
        # The configured value is a fallback cap for non-standard timeframes;
        # never extend a rejection beyond the current signal candle.
        configured_end = current_time + timedelta(
            seconds=max(1, int(self.liquidity_entry_reject_cooldown_seconds))
        )
        until = min(candle_end, configured_end)
        if until <= current_time:
            until = candle_end + timedelta(seconds=1)
        cooldowns = getattr(self, '_entry_liquidity_cooldowns', {})
        if len(cooldowns) >= 256:
            cooldowns.clear()
        cooldowns[pair] = until
        self._entry_liquidity_cooldowns = cooldowns
        return until

    def _is_backtest_mode(self) -> bool:
        runmode = str(getattr(getattr(self, 'dp', None), 'runmode', '')).lower()
        return 'backtest' in runmode or 'hyperopt' in runmode

    def _new_metrics_state(self) -> dict:
        return {
            'started_at': time.monotonic(),
            'factor_ms': deque(maxlen=self.metrics_history_size),
            'entry_ms': deque(maxlen=self.metrics_history_size),
            'base_5m_age_s': deque(maxlen=self.metrics_history_size),
            'btc_5m_age_s': deque(maxlen=self.metrics_history_size),
            'pair_1h_age_s': deque(maxlen=self.metrics_history_size),
            'pair_1h_closed_age_s': deque(maxlen=self.metrics_history_size),
            'pair_1h_alignment_lag_s': deque(maxlen=self.metrics_history_size),
            'pairs': set(),
            'entry_candidates': 0,
            'signal_counts': {},
            'liquidity_entry_rejections': 0,
            'liquidity_entry_cooldown_skips': 0,
        }

    def _metrics_state(self) -> dict:
        state = getattr(self, '_vwap_metrics', None)
        if state is None:
            state = self._new_metrics_state()
            self._vwap_metrics = state
        return state

    @staticmethod
    def _candle_age_seconds(value) -> float | None:
        if value is None:
            return None
        timestamp = pd.to_datetime(value, utc=True, errors='coerce')
        if pd.isna(timestamp):
            return None
        return max(0.0, (pd.Timestamp.now(tz='UTC') - timestamp).total_seconds())

    @staticmethod
    def _metric_percentiles(values) -> tuple[float | None, float | None]:
        if not values:
            return None, None
        samples = np.asarray(values, dtype=float)
        return float(np.percentile(samples, 50)), float(np.percentile(samples, 95))

    @staticmethod
    def _format_percentiles(values, *, suffix: str = '') -> str:
        p50, p95 = VWAP_V4._metric_percentiles(values)
        if p50 is None or p95 is None:
            return 'n/a'
        return f'{p50:.1f}/{p95:.1f}{suffix}'

    def _record_factor_metrics(
        self,
        pair: str,
        elapsed_ms: float,
        dataframe: DataFrame,
        btc_candle_date=None,
        pair_1h_candle_date=None,
        pair_1h_closed_age_s: float | None = None,
        pair_1h_alignment_lag_s: float | None = None,
    ) -> None:
        if self._is_backtest_mode():
            return
        state = self._metrics_state()
        state['factor_ms'].append(elapsed_ms)
        state['pairs'].add(pair)
        if not dataframe.empty and 'date' in dataframe:
            age = self._candle_age_seconds(dataframe['date'].iat[-1])
            if age is not None:
                state['base_5m_age_s'].append(age)
        btc_age = self._candle_age_seconds(btc_candle_date)
        if btc_age is not None:
            state['btc_5m_age_s'].append(btc_age)
        pair_1h_age = self._candle_age_seconds(pair_1h_candle_date)
        if pair_1h_age is not None:
            state['pair_1h_age_s'].append(pair_1h_age)
        if pair_1h_closed_age_s is not None:
            state['pair_1h_closed_age_s'].append(pair_1h_closed_age_s)
        if pair_1h_alignment_lag_s is not None:
            state['pair_1h_alignment_lag_s'].append(pair_1h_alignment_lag_s)

    def _record_entry_metrics(self, pair: str, elapsed_ms: float, dataframe: DataFrame) -> None:
        if self._is_backtest_mode():
            return
        state = self._metrics_state()
        state['entry_ms'].append(elapsed_ms)
        state['pairs'].add(pair)
        if not dataframe.empty:
            latest = dataframe.iloc[-1]
            if latest.get('enter_long') == 1:
                tag = latest.get('enter_tag')
                tag = str(tag) if tag else 'untagged'
                state['entry_candidates'] += 1
                state['signal_counts'][tag] = state['signal_counts'].get(tag, 0) + 1
        self._emit_metrics_if_due()

    def _emit_metrics_if_due(self) -> None:
        state = self._metrics_state()
        elapsed_s = time.monotonic() - state['started_at']
        if elapsed_s < self.metrics_window_seconds:
            return
        whitelist_count = -1
        try:
            if self.dp:
                whitelist_count = len(self.dp.current_whitelist())
        except Exception:
            whitelist_count = -1
        logger.info(
            "VWAP_V4 telemetry: window_s=%.1f whitelist=%s analyzed_pairs=%s factor_samples=%s "
            "factor_ms_p50/p95=%s entry_samples=%s entry_ms_p50/p95=%s "
            "latest_candle_age_s_p50/p95=%s btc_5m_age_s_p50/p95=%s "
            "pair_1h_open_age_s_p50/p95=%s pair_1h_closed_age_s_p50/p95=%s "
            "pair_1h_alignment_lag_s_p50/p95=%s "
            "entry_candidates=%s signal_counts=%s liquidity_rejections=%s cooldown_skips=%s.",
            elapsed_s,
            whitelist_count,
            len(state['pairs']),
            len(state['factor_ms']),
            self._format_percentiles(state['factor_ms'], suffix='ms'),
            len(state['entry_ms']),
            self._format_percentiles(state['entry_ms'], suffix='ms'),
            self._format_percentiles(state['base_5m_age_s'], suffix='s'),
            self._format_percentiles(state['btc_5m_age_s'], suffix='s'),
            self._format_percentiles(state['pair_1h_age_s'], suffix='s'),
            self._format_percentiles(state['pair_1h_closed_age_s'], suffix='s'),
            self._format_percentiles(state['pair_1h_alignment_lag_s'], suffix='s'),
            state['entry_candidates'],
            dict(sorted(state['signal_counts'].items())),
            state['liquidity_entry_rejections'],
            state['liquidity_entry_cooldown_skips'],
        )
        self._vwap_metrics = self._new_metrics_state()

    def _log_dca_block_once(self, pair: str, reason: str, current_time: datetime) -> None:
        if self._is_backtest_mode():
            return
        hour = current_time.astimezone(timezone.utc).replace(
            minute=0,
            second=0,
            microsecond=0,
        ).isoformat()
        key = (pair, reason, hour)
        cache = getattr(self, '_dca_block_log_cache', set())
        if key in cache:
            return
        logger.info(f'DCA blocked for {pair}: {reason}')
        if len(cache) >= 256:
            cache.clear()
        cache.add(key)
        self._dca_block_log_cache = cache

    def bot_loop_start(self, current_time: datetime, **kwargs) -> None:
        """Live-only: reconcile stale 1h data right after an hour boundary.

        Never raises into the main loop; all failures degrade to a warning and
        the normal refresh path (plus the entry freshness gate) stays in charge.
        """
        if self._is_backtest_mode():
            return
        try:
            self._force_refresh_stale_1h(current_time)
        except Exception as exception:
            logger.warning(f"VWAP_V4 1h force-refresh aborted: {exception}")

    def _log_1h_stale_once(
        self,
        pair: str,
        actual_open=None,
        required_open=None,
        alignment_lag_s: float | None = None,
    ) -> None:
        if self._is_backtest_mode():
            return
        hour = datetime.now(timezone.utc).replace(
            minute=0, second=0, microsecond=0
        ).isoformat()
        key = (pair, hour)
        cache = getattr(self, '_1h_stale_log_cache', set())
        if key in cache:
            return
        actual_txt = 'n/a' if actual_open is None else str(actual_open)
        required_txt = 'n/a' if required_open is None else str(required_open)
        lag_txt = 'n/a' if alignment_lag_s is None else f'{alignment_lag_s:.0f}s'
        logger.info(
            f'1h data stale for {pair}: latest_open={actual_txt}, '
            f'required_open={required_txt}, alignment_lag={lag_txt}; '
            'missing one or more completed 1h candles, blocking 1h-dependent entry '
            'branches (vwap/NFIX39 unaffected).'
        )
        if len(cache) >= 256:
            cache.clear()
        cache.add(key)
        self._1h_stale_log_cache = cache

    def _force_refresh_stale_1h(self, current_time: datetime) -> None:
        now = current_time.astimezone(timezone.utc)
        seconds_into_hour = now.minute * 60 + now.second
        if seconds_into_hour > self.pair_1h_force_refresh_window_s:
            return
        dp = getattr(self, 'dp', None)
        exchange = getattr(dp, '_exchange', None)
        if dp is None or exchange is None:
            return
        try:
            whitelist = list(dp.current_whitelist())
        except Exception:
            return
        if not whitelist:
            return
        hour_open = now.replace(minute=0, second=0, microsecond=0)
        required_open = hour_open - pd.Timedelta(hours=1)
        attempts = getattr(self, '_1h_force_refresh_attempts', {})
        now_mono = time.monotonic()
        stale_pairs = []
        oldest_pair, oldest_open = None, None
        for pair in whitelist:
            try:
                df_1h = dp.get_pair_dataframe(pair=pair, timeframe='1h')
            except Exception:
                df_1h = None
            last_open = None
            if df_1h is not None and not df_1h.empty and 'date' in df_1h:
                last_open = pd.to_datetime(df_1h['date'].iat[-1], utc=True)
            if last_open is None or last_open < required_open:
                if now_mono - attempts.get(pair, 0.0) >= self.pair_1h_force_refresh_cooldown_s:
                    stale_pairs.append(pair)
                if last_open is not None and (oldest_open is None or last_open < oldest_open):
                    oldest_pair, oldest_open = pair, last_open
        if not stale_pairs:
            return
        started = time.perf_counter()
        candle_type = (
            CandleType.FUTURES
            if self.config.get('trading_mode') == 'futures'
            else CandleType.SPOT
        )
        klines = getattr(exchange, '_klines', {})
        refresh_marks = getattr(exchange, '_pairs_last_refresh_time', {})
        for pair in stale_pairs:
            key = (pair, '1h', candle_type)
            # Evict the stale cache entry so the refresh is a real REST
            # reconciliation instead of a gated/websocket reuse no-op.
            klines.pop(key, None)
            refresh_marks.pop(key, None)
            attempts[pair] = now_mono
        self._1h_force_refresh_attempts = attempts
        dp.refresh([], [(pair, '1h', candle_type) for pair in stale_pairs])
        refreshed = 0
        empty = 0
        for pair in stale_pairs:
            try:
                df_1h = dp.get_pair_dataframe(pair=pair, timeframe='1h')
            except Exception:
                df_1h = None
            if df_1h is None or df_1h.empty:
                empty += 1
                continue
            last_open = pd.to_datetime(df_1h['date'].iat[-1], utc=True)
            if last_open >= required_open:
                refreshed += 1
        elapsed_s = time.perf_counter() - started
        logger.info(
            f'VWAP_V4 1h force-refresh: stale={len(stale_pairs)} '
            f'refreshed={refreshed} empty={empty} '
            f'oldest={oldest_pair}@{oldest_open} '
            f'elapsed={elapsed_s:.1f}s window={seconds_into_hour}s past hour.'
        )

    def confirm_trade_entry(self, pair: str, order_type: str, amount: float, rate: float,
                            time_in_force: str, current_time: datetime,
                            entry_tag: Optional[str], side: str, **kwargs) -> bool:
        # Freqtrade calls confirm_trade_entry only for mode="initial".  Never
        # apply this cooldown from the position-adjustment path, so filled
        # positions retain their independent DCA schedule and protective exits.
        cooldown_until = self._entry_liquidity_cooldown_until(pair, current_time)
        if cooldown_until is not None:
            self._metrics_state()['liquidity_entry_cooldown_skips'] += 1
            return False
        liquidity_block_reason = self._liquidity_entry_block_reason(pair, rate, amount)
        if liquidity_block_reason:
            cooldown_until = self._set_entry_liquidity_cooldown(pair, current_time)
            self._metrics_state()['liquidity_entry_rejections'] += 1
            logger.info(
                'JC initial entry blocked by liquidity guard for %s: %s; '
                'suppressing duplicate initial-entry attempts until %s.',
                pair,
                liquidity_block_reason,
                cooldown_until.isoformat(),
            )
            return False
        return True

    def leverage(self, pair: str, current_time: datetime, current_rate: float,
                 proposed_leverage: float, max_leverage: float,
                 entry_tag: Optional[str], side: str, **kwargs) -> float:
        """Use the configured, risk-capped 2x PM leverage for every entry."""
        return min(self.pm_target_leverage, float(max_leverage))

    def _dca_total_multiplier(self) -> float:
        """Total stake multiplier for the initial entry plus all DCA entries."""
        return sum(
            self.safety_order_volume_scale ** index
            for index in range(self.max_safety_orders + 1)
        )

    def _configured_open_trade_slots(self) -> int:
        """Return a safe finite slot count for DCA capital reservation."""
        configured_slots = self.config.get('max_open_trades', 1) if getattr(self, 'config', None) else 1
        try:
            slots = int(configured_slots)
        except (TypeError, ValueError, OverflowError):
            slots = 1
        return max(1, slots)

    def _position_budget_for_new_trade(self, max_stake: float) -> float:
        """Reserve enough free collateral for every configured concurrent trade."""
        available_stake = max(0.0, float(max_stake or 0.0))
        reserved_per_slot = available_stake / self._configured_open_trade_slots()
        return min(float(self.max_position_stake), reserved_per_slot)

    def _initial_entry_stake_amount(
        self, min_stake: Optional[float], max_stake: float
    ) -> float:
        """Size an entry only when its complete DCA ladder can be funded."""
        position_budget = self._position_budget_for_new_trade(max_stake)
        stake_amount = min(
            float(self.entry_stake_amount),
            position_budget / self._dca_total_multiplier(),
        )
        if min_stake and stake_amount < min_stake:
            return 0.0
        return max(0.0, min(stake_amount, float(max_stake or 0.0)))

    def custom_stake_amount(self, pair: str, current_time: datetime, current_rate: float,
                            proposed_stake: float, min_stake: Optional[float], max_stake: float,
                            leverage: float, entry_tag: Optional[str], side: str, **kwargs) -> float:
        stake_amount = self._initial_entry_stake_amount(min_stake, max_stake)
        if not self._is_backtest_mode():
            logger.info(
                f"PM entry stake for {pair}: {stake_amount:.2f} USDT; "
                f"reserved DCA cap={self._position_budget_for_new_trade(max_stake):.2f} USDT"
            )
        return stake_amount

    def informative_pairs(self):
        pairs = self.dp.current_whitelist() if self.dp else ['BTC/USDT', 'ETH/USDT']
        informative = {(pair, '1h') for pair in pairs}
        # BTC is used as a 5m market-regime input in ``populate_indicators``.
        # It is not guaranteed to be in the dynamic Top-20 whitelist, so request
        # it explicitly instead of relying on a coincidental whitelist membership.
        btc_pair = 'BTC/USDT:USDT' if self.config.get('trading_mode') == 'futures' else 'BTC/USDT'
        informative.add((btc_pair, self.timeframe))
        return sorted(informative)
    
    def custom_exit(self, pair: str, trade: 'Trade', current_time: 'datetime', current_rate: float, current_profit: float, **kwargs):
        dataframe, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
        if dataframe.empty:
            return None

        last_candle = dataframe.iloc[-1].squeeze()
        filled_buys = trade.select_filled_orders('buy')
        count_of_buys = len(filled_buys)
        max_profit = 0.0

        if trade.max_rate and trade.open_rate:
            max_profit = (trade.max_rate / trade.open_rate) - 1
        
        if last_candle is not None:
            if (
                current_profit > self.sell_trail_profit_min_1.value
                and current_profit < self.sell_trail_profit_max_1.value
                and max_profit > (current_profit + self.sell_trail_down_1.value)
            ):
                return 'trail_target_1'
            elif (
                current_profit > self.sell_trail_profit_min_2.value
                and current_profit < self.sell_trail_profit_max_2.value
                and max_profit > (current_profit + self.sell_trail_down_2.value)
            ):
                return 'trail_target_2'
            elif current_profit > 0.03 and last_candle['rsi'] > 85:
                return 'RSI-85 target'
            
            if (current_profit > 0) & (count_of_buys < 6) & (last_candle['close'] > last_candle['hma_50']) & \
               (last_candle['close'] > (last_candle[f'ma_sell_{self.base_nb_candles_sell.value}'] * self.high_offset_2.value)) & \
               (last_candle['rsi'] > 50) & (last_candle['volume'] > 0) & (last_candle['rsi_fast'] > last_candle['rsi_slow']):
                return 'sell signal1'
            
            if (current_profit > 0) & (count_of_buys >= 6) & (last_candle['close'] > last_candle['hma_50'] * 1.01) & \
               (last_candle['close'] > (last_candle[f'ma_sell_{self.base_nb_candles_sell.value}'] * self.high_offset_2.value)) & \
               (last_candle['rsi'] > 50) & (last_candle['volume'] > 0) & (last_candle['rsi_fast'] > last_candle['rsi_slow']):
                return 'sell signal1 * 1.01'
            
            if (current_profit > 0) & (last_candle['close'] > last_candle['hma_50']) & \
               (last_candle['close'] > (last_candle[f'ma_sell_{self.base_nb_candles_sell.value}'] * self.high_offset.value)) & \
               (last_candle['volume'] > 0) & (last_candle['rsi_fast'] > last_candle['rsi_slow']):
                return 'sell signal2'
            
            if (current_profit < self.sell_deadfish_profit.value) and \
               (last_candle['close'] < last_candle['ema_200']) and \
               (last_candle['bb_width'] < self.sell_deadfish_bb_width.value) and \
               (last_candle['close'] > last_candle['bb_middleband2'] * self.sell_deadfish_bb_factor.value) and \
               (last_candle['volume_mean_12'] < last_candle['volume_mean_24'] * self.sell_deadfish_volume_factor.value) and \
               (last_candle['cmf'] < 0.0):
                return f"sell_stoploss_deadfish"
    
    def custom_stoploss(self, pair: str, trade: 'Trade', current_time: datetime,
                        current_rate: float, current_profit: float, after_fill: bool,
                        **kwargs) -> float | None:
        HSL = self.pHSL.value
        PF_1 = self.pPF_1.value
        SL_1 = self.pSL_1.value
        PF_2 = self.pPF_2.value
        SL_2 = self.pSL_2.value
        
        # A newly filled DCA changes the average entry.  The v3 after_fill
        # callback lets Freqtrade replace the exchange stoploss with the stop
        # derived from that new average price.
        if after_fill:
            return stoploss_from_open(
                HSL,
                current_profit,
                is_short=trade.is_short,
                leverage=trade.leverage,
            )

        if current_profit > PF_2:
            sl_profit = SL_2 + (current_profit - PF_2)
        elif current_profit > PF_1:
            sl_profit = SL_1 + ((current_profit - PF_1) * (SL_2 - SL_1) / (PF_2 - PF_1))
        else:
            sl_profit = HSL
        
        if sl_profit >= current_profit:
            return None

        return stoploss_from_open(
            sl_profit,
            current_profit,
            is_short=trade.is_short,
            leverage=trade.leverage,
        )
    
    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        factor_started = time.perf_counter()
        btc_candle_date = None
        pair_1h_candle_date = None
        dataframe['date'] = pd.to_datetime(dataframe['date'], utc=True).astype('datetime64[ns, UTC]')
        inf_tf = '5m'
        btc_pair = 'BTC/USDT:USDT' if self.config.get('trading_mode') == 'futures' else 'BTC/USDT'
        informative = self.dp.get_pair_dataframe(btc_pair, timeframe=inf_tf)
        if not informative.empty:
            informative['date'] = pd.to_datetime(informative['date'], utc=True).astype('datetime64[ns, UTC]')
            btc_candle_date = informative['date'].iat[-1]
            # Align BTC market-regime inputs by timestamp instead of row number
            # (robust to gaps / new listings).  The +5m shift inside the merge
            # helper keeps only fully closed BTC candles, matching the old
            # shift(1) no-lookahead semantics.
            informative['btc_close'] = informative['close']
            informative['btc_ema_fast'] = ta.EMA(informative, timeperiod=20)
            informative['btc_ema_slow'] = ta.EMA(informative, timeperiod=25)
            dataframe = merge_shifted_informative_columns(
                dataframe,
                informative,
                ['btc_close', 'btc_ema_fast', 'btc_ema_slow'],
                inf_tf,
            )
            dataframe['down'] = (dataframe['btc_ema_fast'] < dataframe['btc_ema_slow']).astype('int')
        else:
            dataframe['btc_close'] = np.nan
            dataframe['btc_ema_fast'] = np.nan
            dataframe['btc_ema_slow'] = np.nan
            dataframe['down'] = np.nan
            logger.warning(f"No BTC/USDT:USDT data available for {metadata['pair']}")

        for val in self.base_nb_candles_sell.range:
            dataframe[f'ma_sell_{val}'] = ta.EMA(dataframe, timeperiod=val)
        dataframe['volume_mean_12'] = dataframe['volume'].rolling(12).mean().shift(1)
        dataframe['volume_mean_24'] = dataframe['volume'].rolling(24).mean().shift(1)
        dataframe['cmf'] = chaikin_money_flow(dataframe, 20)
        bollinger2 = qtpylib.bollinger_bands(qtpylib.typical_price(dataframe), window=20, stds=2)
        dataframe['bb_lowerband2'] = bollinger2['lower']
        dataframe['bb_middleband2'] = bollinger2['mid']
        dataframe['bb_upperband2'] = bollinger2['upper']
        dataframe['bb_width'] = ((dataframe['bb_upperband2'] - dataframe['bb_lowerband2']) / dataframe['bb_middleband2'])
        dataframe['ema_200'] = ta.EMA(dataframe, timeperiod=200)
        dataframe['ema_50'] = ta.EMA(dataframe, timeperiod=50)
        dataframe['rsi'] = ta.RSI(dataframe, timeperiod=14)
        dataframe['rsi_fast'] = ta.RSI(dataframe, timeperiod=4)
        dataframe['rsi_slow'] = ta.RSI(dataframe, timeperiod=20)

        vwap_low, _, _ = VWAPB(dataframe, 20, 1)
        dataframe['vwap_lowerband'] = vwap_low
        dataframe['ema_vwap_diff_50'] = ((dataframe['ema_50'] - dataframe['vwap_lowerband']) / dataframe['ema_50'])

        dataframe['tpct_change_0'] = top_percent_change_dca(dataframe, 0)

        dataframe['cti'] = pta.cti(dataframe["close"], length=20)
        dataframe['r_14'] = williams_r(dataframe, period=14)
        dataframe['ema_16'] = ta.EMA(dataframe, timeperiod=16)
        dataframe['EWO'] = EWO(dataframe, self.fast_ewo, self.slow_ewo)
        dataframe['ema_5'] = ta.EMA(dataframe, timeperiod=5)
        dataframe['ema_10'] = ta.EMA(dataframe, timeperiod=10)

        # Heikin-Ashi
        heikinashi = qtpylib.heikinashi(dataframe)
        dataframe['ha_open'] = heikinashi['open']
        dataframe['ha_close'] = heikinashi['close']
        dataframe['ha_high'] = heikinashi['high']
        dataframe['ha_low'] = heikinashi['low']

        # Bollinger Bands 40 (for NFIX39)
        bollinger2_40 = qtpylib.bollinger_bands(qtpylib.typical_price(dataframe), window=40, stds=2)
        dataframe['bb_lowerband2_40'] = bollinger2_40['lower']
        dataframe['bb_middleband2_40'] = bollinger2_40['mid']
        dataframe['bb_upperband2_40'] = bollinger2_40['upper']
        dataframe['bb_delta_cluc'] = (dataframe['bb_middleband2_40'] - dataframe['bb_lowerband2_40']).abs()

        # Stochastic RSI
        stoch = ta.STOCHRSI(dataframe, 15, 20, 2, 2)
        dataframe['srsi_fk'] = stoch['fastk']

        # HA close delta / tail (for DIP / NFIX39)
        dataframe['closedelta'] = (dataframe['ha_close'] - dataframe['ha_close'].shift()).abs()
        dataframe['tail'] = (dataframe['ha_close'] - dataframe['ha_low']).abs()

        # Bollinger Bands 3 std
        bollinger3 = qtpylib.bollinger_bands(qtpylib.typical_price(dataframe), window=20, stds=3)
        dataframe['bb_lowerband3'] = bollinger3['lower']
        dataframe['bb_delta'] = ((dataframe['bb_lowerband2'] - dataframe['bb_lowerband3']) / dataframe['bb_lowerband2'])

        # Additional RSIs (for vwap signal)
        dataframe['rsi_84'] = ta.RSI(dataframe, timeperiod=84)
        dataframe['rsi_112'] = ta.RSI(dataframe, timeperiod=112)

        # Price change metrics (for vwap signal)
        dataframe['tcp_percent_4'] = top_percent_change_dca(dataframe, 4)

        # RMI and CCI (for DIP signal)
        for val in self.buy_rmi_length.range:
            dataframe[f'rmi_length_{val}'] = RMI(dataframe, length=val, mom=4)
        for val in self.buy_cci_length.range:
            dataframe[f'cci_length_{val}'] = ta.CCI(dataframe, val)
        
        # ===== 3. 获取当前交易对的1h数据 =====
        inf_tf = '1h'
        pair_1h_columns = [
            'rsi_14', 'cmf', 'trend_ema20', 'trend_ema50', 'trend_ema100',
            'trend_ema50_slope_12h', 'pair_trend_ok', 'pair_trend_strong',
            'pair_not_overextended', 'r_84', 'bb_width', 'cti_40', 'roc',
        ]

        def set_pair_1h_defaults() -> None:
            for column in pair_1h_columns:
                dataframe[f'{column}_1h'] = 0 if column.startswith('pair_') else np.nan

        try:
            informative = self.dp.get_pair_dataframe(pair=metadata['pair'], timeframe=inf_tf)
            if informative is None or informative.empty:
                informative = pd.DataFrame()
        except Exception:
            informative = pd.DataFrame()
        
        if not informative.empty and len(informative) > 0:
            try:
                informative['date'] = pd.to_datetime(informative['date'], utc=True).astype('datetime64[ns, UTC]')
                pair_1h_candle_date = informative['date'].iat[-1]
                informative['rsi_14'] = ta.RSI(informative, timeperiod=14)
                informative['cmf'] = chaikin_money_flow(informative, 20)
                informative['r_84'] = williams_r(informative, period=84)
                informative['cti_40'] = pta.cti(informative["close"], length=40)
                informative['roc'] = ta.ROC(informative, timeperiod=9)
                bollinger_1h = qtpylib.bollinger_bands(qtpylib.typical_price(informative), window=20, stds=2)
                informative['bb_width'] = (
                    (bollinger_1h['upper'] - bollinger_1h['lower']) / bollinger_1h['mid']
                )

                informative['trend_ema20'] = ta.EMA(informative, timeperiod=20)
                informative['trend_ema50'] = ta.EMA(informative, timeperiod=50)
                informative['trend_ema100'] = ta.EMA(informative, timeperiod=100)
                informative['trend_ema50_slope_12h'] = (
                    (informative['trend_ema50'] / informative['trend_ema50'].shift(12)) - 1
                )
                informative['pair_trend_ok'] = (
                    (informative['close'] > (informative['trend_ema50'] * self.pair_trend_close_floor)) &
                    (informative['trend_ema20'] > (informative['trend_ema50'] * self.pair_trend_ema20_floor)) &
                    (informative['trend_ema50'] > (informative['trend_ema100'] * self.pair_trend_ema50_floor)) &
                    (informative['trend_ema50'] > (informative['trend_ema50'].shift(12) * self.pair_trend_slope_floor))
                ).astype('int')
                informative['pair_trend_strong'] = (
                    (informative['close'] > informative['trend_ema50']) &
                    (informative['trend_ema20'] > informative['trend_ema50']) &
                    (informative['trend_ema50'] > informative['trend_ema100']) &
                    (informative['trend_ema50_slope_12h'] > 0)
                ).astype('int')
                informative['pair_not_overextended'] = (
                    informative['close'] < (informative['trend_ema20'] * self.pair_trend_max_extension)
                ).astype('int')

                renamed_columns = {
                    column: f'{column}_1h'
                    for column in pair_1h_columns
                }
                informative = informative.rename(columns=renamed_columns)
                dataframe = merge_shifted_informative_columns(
                    dataframe,
                    informative,
                    list(renamed_columns.values()),
                    inf_tf,
                )
            except Exception as e:
                logger.error(f"Error processing 1h informative data: {str(e)}")
                set_pair_1h_defaults()
        else:
            logger.warning(f"No 1h data available for {metadata['pair']}")
            set_pair_1h_defaults()
        
        # ===== 3b. 1h completed-candle alignment gate =====
        # A 1h input's closed-information age naturally rises from 0 to almost
        # 3600 seconds during the following hour.  It is therefore a useful
        # telemetry value but not a freshness verdict.  The entry gate instead
        # compares the latest 1h source open to the latest source open that can
        # legally be used by this completed 5m candle.  This preserves the
        # lookahead-safe merge above while blocking a genuinely missing 1h bar.
        pair_1h_closed_age_s: float | None = None
        pair_1h_alignment_lag_s: float | None = None
        required_1h_open = None
        actual_1h_open = None
        if not dataframe.empty:
            last_5m_open = pd.to_datetime(dataframe['date'].iat[-1], utc=True)
            # A 5m bar opened at 17:55 may only use the 1h bar opened at
            # 16:00 (which closed at 17:00); a 5m bar opened at 18:00 may use
            # the 17:00 1h bar.  This matches merge_shifted_informative_columns.
            required_1h_open = last_5m_open.floor('h') - pd.Timedelta(hours=1)
        if pair_1h_candle_date is not None and not dataframe.empty:
            actual_1h_open = pd.to_datetime(pair_1h_candle_date, utc=True)
            closed_info_time = pd.to_datetime(pair_1h_candle_date, utc=True) + pd.Timedelta(hours=1)
            pair_1h_closed_age_s = max(
                0.0,
                (last_5m_open + pd.Timedelta(minutes=5) - closed_info_time).total_seconds(),
            )
        if actual_1h_open is not None and required_1h_open is not None:
            pair_1h_alignment_lag_s = max(
                0.0,
                (required_1h_open - actual_1h_open).total_seconds(),
            )
        pair_1h_fresh = (
            pair_1h_alignment_lag_s is not None
            and pair_1h_alignment_lag_s
            <= self.pair_1h_max_missing_candles * 60 * 60
        )
        dataframe['pair_1h_fresh'] = 1 if pair_1h_fresh else 0
        if not pair_1h_fresh:
            self._log_1h_stale_once(
                metadata['pair'],
                actual_1h_open,
                required_1h_open,
                pair_1h_alignment_lag_s,
            )

        # ===== 4. 其他必要指标 =====
        dataframe['hma_50'] = qtpylib.hull_moving_average(dataframe['close'], window=50)

        self._record_factor_metrics(
            metadata['pair'],
            (time.perf_counter() - factor_started) * 1000,
            dataframe,
            btc_candle_date,
            pair_1h_candle_date,
            pair_1h_closed_age_s,
            pair_1h_alignment_lag_s,
        )
        return dataframe
    
    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        entry_started = time.perf_counter()
        dataframe['enter_long'] = 0
        dataframe['enter_tag'] = None

        trend_base = (
            (dataframe['pair_trend_ok_1h'].fillna(0) == 1) &
            (dataframe['pair_not_overextended_1h'].fillna(0) == 1) &
            (dataframe['ema_50'] > (dataframe['ema_50'].shift(12) * 0.999)) &
            (dataframe['volume'] > 0)
        )
        trend_reclaim = (
            (dataframe['pair_1h_fresh'] == 1) &
            trend_base &
            (dataframe['pair_trend_strong_1h'].fillna(0) == 1) &
            (dataframe['ema_50'] > dataframe['ema_200']) &
            (dataframe['ema_5'] > dataframe['ema_10']) &
            (dataframe['ema_5'].shift(1) <= dataframe['ema_10'].shift(1)) &
            (dataframe['low'].rolling(self.trend_reclaim_lookback).min() <
             (dataframe['ema_16'] * self.trend_reclaim_low_ratio)) &
            (dataframe['close'] > (dataframe['ema_16'] * self.trend_reclaim_close_floor)) &
            (dataframe['close'] < (dataframe['ema_16'] * self.trend_reclaim_close_ceiling)) &
            (dataframe['rsi'] >= self.trend_reclaim_rsi_min) &
            (dataframe['rsi'] <= self.trend_reclaim_rsi_max) &
            (dataframe['cti'] >= self.trend_reclaim_cti_min) &
            (dataframe['cti'] < self.trend_reclaim_cti_max) &
            (dataframe['EWO'] > 0) &
            (dataframe['volume'] > dataframe['volume_mean_24'])
        )
        dataframe.loc[
            trend_reclaim,
            ['enter_long', 'enter_tag']
        ] = (1, 'jc_trend_reclaim')

        trend_retest = (
            (dataframe['pair_1h_fresh'] == 1) &
            trend_base &
            (dataframe['pair_trend_strong_1h'].fillna(0) == 1) &
            (dataframe['ema_50'] > dataframe['ema_200']) &
            (dataframe['close'] > dataframe['ema_16']) &
            (dataframe['close'].shift(1) <= dataframe['ema_16'].shift(1)) &
            (dataframe['low'].rolling(self.trend_retest_lookback).min() <
             (dataframe['ema_16'] * self.trend_retest_low_ratio)) &
            (dataframe['close'] <
             (dataframe['ema_16'] * self.trend_retest_close_ceiling)) &
            (dataframe['rsi'] >= self.trend_retest_rsi_min) &
            (dataframe['rsi'] <= self.trend_retest_rsi_max) &
            (dataframe['cti'] >= self.trend_retest_cti_min) &
            (dataframe['cti'] < self.trend_retest_cti_max) &
            (dataframe['EWO'] > 0) &
            (dataframe['volume'] >
             (dataframe['volume_mean_24'] * self.trend_retest_volume_ratio))
        )
        dataframe.loc[
            trend_retest,
            ['enter_long', 'enter_tag']
        ] = (1, 'jc_trend_retest')

        insta_signal = (
            (dataframe['pair_1h_fresh'] == 1) &
            (dataframe['bb_width_1h'] > 0.131) &
            (dataframe['r_14'] < -51) &
            (dataframe['r_84_1h'] < -70) &
            (dataframe['cti'] < -0.845) &
            (dataframe['cti_40_1h'] < -0.735) &
            (dataframe['close'].rolling(48).max() >= (dataframe['close'] * 1.1)) &
            (dataframe['btc_close'].rolling(24).max() >= (dataframe['btc_close'] * 1.03))
        )
        dataframe.loc[
            insta_signal,
            ['enter_long', 'enter_tag']
        ] = (1, 'insta_signal')

        dip_signal = (
            (dataframe['pair_1h_fresh'] == 1) &
            (dataframe[f'rmi_length_{self.buy_rmi_length.value}'] < self.buy_rmi.value) &
            (dataframe[f'cci_length_{self.buy_cci_length.value}'] <= self.buy_cci.value) &
            (dataframe['srsi_fk'] < self.buy_srsi_fk.value) &
            (dataframe['bb_delta'] > self.buy_bb_delta.value) &
            (dataframe['bb_width'] > self.buy_bb_width.value) &
            (dataframe['closedelta'] > dataframe['close'] * self.buy_closedelta.value / 1000) &
            (dataframe['close'] < dataframe['bb_lowerband3'] * self.buy_bb_factor.value) &
            (dataframe['roc_1h'] < self.buy_roc_1h.value) &
            (dataframe['bb_width_1h'] < self.buy_bb_width_1h.value)
        )
        dataframe.loc[
            dip_signal,
            ['enter_long', 'enter_tag']
        ] = (1, 'DIP signal')

        vwap_signal = (
            (dataframe['close'] < dataframe['vwap_lowerband']) &
            (dataframe['tcp_percent_4'] > 0.053) &
            (dataframe['cti'] < -0.8) &
            (dataframe['rsi'] < 35) &
            (dataframe['rsi_84'] < 60) &
            (dataframe['rsi_112'] < 60) &
            (dataframe['volume'] > 0)
        )
        dataframe.loc[
            vwap_signal,
            ['enter_long', 'enter_tag']
        ] = (1, 'vwap')

        nfix39_signal = (
            (dataframe['ema_200'] > (dataframe['ema_200'].shift(12) * 1.01)) &
            (dataframe['ema_200'] > (dataframe['ema_200'].shift(48) * 1.07)) &
            (dataframe['bb_lowerband2_40'].shift().gt(0)) &
            (dataframe['bb_delta_cluc'].gt(dataframe['close'] * 0.056)) &
            (dataframe['closedelta'].gt(dataframe['close'] * 0.01)) &
            (dataframe['tail'].lt(dataframe['bb_delta_cluc'] * 0.5)) &
            (dataframe['close'].lt(dataframe['bb_lowerband2_40'].shift())) &
            (dataframe['close'].le(dataframe['close'].shift())) &
            (dataframe['close'] > dataframe['ema_50'] * 0.912)
        )
        dataframe.loc[
            nfix39_signal,
            ['enter_long', 'enter_tag']
        ] = (1, 'NFIX39')

        self._record_entry_metrics(
            metadata['pair'],
            (time.perf_counter() - entry_started) * 1000,
            dataframe,
        )
        return dataframe
    
    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe['exit_long'] = 0
        dataframe['exit_tag'] = None

        trend_failure = (
            (dataframe['pair_trend_ok_1h'].fillna(0) == 0) &
            (dataframe['close'] < (dataframe['ema_50'] * 0.998)) &
            (dataframe['ema_5'] < dataframe['ema_10']) &
            (dataframe['rsi'] < 44) &
            (dataframe['volume'] > 0)
        )
        dataframe.loc[
            trend_failure,
            ['exit_long', 'exit_tag']
        ] = (1, 'jc_trend_failure')

        structure_break = (
            (dataframe['close'] < dataframe['ema_200']) &
            (dataframe['ema_50'] < dataframe['ema_200']) &
            (dataframe['rsi'] < 42) &
            (dataframe['volume'] > 0)
        )
        dataframe.loc[
            structure_break,
            ['exit_long', 'exit_tag']
        ] = (1, 'jc_structure_break')
        return dataframe
    
    safety_order_triggers = [0.03, 0.0675, 0.105, 0.1425, 0.18]
    initial_safety_order_trigger = -0.03
    max_safety_orders = 5
    safety_order_step_scale = 1.0
    # Grow successive safety orders modestly, while `_initial_entry_stake_amount`
    # reserves the whole geometric ladder before the position is opened.
    safety_order_volume_scale = 1.1
    
    def top_percent_change_dca(self, dataframe: DataFrame, length: int) -> float:
        """
        Percentage change of the current close from the range maximum Open price
        :param dataframe: DataFrame The original OHLC dataframe
        :param length: int The length to look back
        """
        if length == 0:
            return (dataframe['open'] - dataframe['close']) / dataframe['close']
        else:
            return (dataframe['open'].rolling(length).max() - dataframe['close']) / dataframe['close']
    
    def adjust_trade_position(self, trade: Trade, current_time: datetime,
                              current_rate: float, current_profit: float, min_stake: float,
                              max_stake: float, **kwargs):
        if current_profit > self.initial_safety_order_trigger:
            return None
        if trade.has_open_orders:
            return None
        
        dataframe, _ = self.dp.get_analyzed_dataframe(trade.pair, self.timeframe)
        if dataframe.empty:
            return None

        last_candle = dataframe.iloc[-1].squeeze()
        filled_buys = trade.select_filled_orders('buy')
        count_of_buys = len(filled_buys)

        if count_of_buys == 0 or count_of_buys > min(self.max_safety_orders, len(self.safety_order_triggers)):
            return None

        bearish_pullback = (last_candle['tpct_change_0'] > 0.018) and (last_candle['close'] < last_candle['open'])
        weak_vwap_reversion = last_candle['ema_vwap_diff_50'] < 0.215
        short_term_strength = last_candle['ema_5'] >= last_candle['ema_10']

        if bearish_pullback and count_of_buys == 1:
            return None
        if bearish_pullback and count_of_buys >= 2 and weak_vwap_reversion:
            return None
        if bearish_pullback and count_of_buys >= 3 and weak_vwap_reversion and short_term_strength:
            return None
        if bearish_pullback and count_of_buys >= 4 and weak_vwap_reversion:
            return None

        if count_of_buys >= 4 and (last_candle['cmf_1h'] < 0.0) and (last_candle['rsi_14_1h'] < 35):
            self._log_dca_block_once(
                trade.pair,
                f"cmf_1h={last_candle['cmf_1h']}, rsi_14_1h={last_candle['rsi_14_1h']}",
                current_time,
            )
            return None
        
        if 1 <= count_of_buys <= min(self.max_safety_orders, len(self.safety_order_triggers)):
            safety_order_trigger = self.safety_order_triggers[count_of_buys - 1]
            
            if current_profit <= (-1 * safety_order_trigger):
                try:
                    initial_stake = float(filled_buys[0].stake_amount_filled)
                    planned_position_stake = min(
                        float(self.max_position_stake),
                        initial_stake * self._dca_total_multiplier(),
                    )
                    remaining_position_stake = max(0.0, planned_position_stake - trade.stake_amount)
                    requested_stake = initial_stake * (self.safety_order_volume_scale ** count_of_buys)
                    stake_amount = min(requested_stake, remaining_position_stake, float(max_stake or 0.0))
                    if min_stake and stake_amount < min_stake:
                        logger.info(
                            f"Safety order #{count_of_buys} blocked for {trade.pair}: "
                            f"remaining reserved stake={stake_amount:.2f} is below min_stake={min_stake}."
                        )
                        return None
                    amount = stake_amount / current_rate
                    logger.info(
                        f"Initiating PM safety order buy #{count_of_buys} for {trade.pair} with "
                        f"stake={stake_amount:.2f} USDT (planned cap={planned_position_stake:.2f}, "
                        f"remaining={remaining_position_stake:.2f}), amount={amount}"
                    )
                    return stake_amount, f'pm_dca_{count_of_buys}'
                except Exception as exception:
                    logger.info(f'Error occured while trying to get stake amount for {trade.pair}: {str(exception)}') 
                    return None
        return None
