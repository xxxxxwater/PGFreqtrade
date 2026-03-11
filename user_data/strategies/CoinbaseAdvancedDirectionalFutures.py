from __future__ import annotations

"""Directional futures strategy for the Coinbase Advanced test tree.

Compared with the VWAP wrapper, this version is more explicitly futures-oriented:
- separate long / short branches
- BTC regime filter as market-state controller
- no DCA
- lower capital amplification
- cleaner tags for later log / DB inspection
"""

import numpy as np
import pandas as pd
import talib.abstract as ta
from pandas import DataFrame

from freqtrade.strategy.interface import IStrategy
from freqtrade.strategy import merge_informative_pair
import freqtrade.vendor.qtpylib.indicators as qtpylib


class CoinbaseAdvancedDirectionalFutures(IStrategy):
    INTERFACE_VERSION = 3
    can_short = True

    timeframe = "5m"
    startup_candle_count = 240
    process_only_new_candles = True
    position_adjustment_enable = False

    minimal_roi = {
        "0": 0.012,
        "45": 0.008,
        "120": 0.003,
        "180": 0,
    }
    stoploss = -0.08
    use_custom_stoploss = False

    order_types = {
        "entry": "market",
        "exit": "market",
        "emergency_exit": "market",
        "force_entry": "market",
        "force_exit": "market",
        "stoploss": "market",
        "stoploss_on_exchange": False,
    }

    order_time_in_force = {
        "entry": "GTC",
        "exit": "GTC",
    }

    @property
    def protections(self):
        return [
            {"method": "LowProfitPairs", "lookback_period": 60, "trade_limit": 1, "stop_duration": 60},
            {"method": "MaxDrawdown", "lookback_period": 180, "trade_limit": 3, "stop_duration": 60, "max_relative_drawdown": 0.15},
        ]

    def informative_pairs(self):
        pairs = self.dp.current_whitelist()
        btc_pair = "BTC/USDC:USDC"
        return list(dict.fromkeys([(p, "1h") for p in pairs] + [(btc_pair, "1h")]))

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["ema_20"] = ta.EMA(dataframe, timeperiod=20)
        dataframe["ema_50"] = ta.EMA(dataframe, timeperiod=50)
        dataframe["ema_200"] = ta.EMA(dataframe, timeperiod=200)
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)
        dataframe["adx"] = ta.ADX(dataframe, timeperiod=14)
        dataframe["atr"] = ta.ATR(dataframe, timeperiod=14)
        dataframe["bb_mid"] = ta.SMA(dataframe["close"], timeperiod=20)
        dataframe["bb_std"] = dataframe["close"].rolling(20).std()
        dataframe["bb_upper"] = dataframe["bb_mid"] + (2 * dataframe["bb_std"])
        dataframe["bb_lower"] = dataframe["bb_mid"] - (2 * dataframe["bb_std"])
        dataframe["bb_width"] = ((dataframe["bb_upper"] - dataframe["bb_lower"]) / dataframe["bb_mid"]).replace([np.inf, -np.inf], np.nan)
        dataframe["volume_mean_20"] = dataframe["volume"].rolling(20).mean()
        dataframe["roc"] = ta.ROC(dataframe, timeperiod=9)
        dataframe["cti_proxy"] = ((dataframe["close"] - dataframe["close"].rolling(20).mean()) / dataframe["close"].rolling(20).std()).replace([np.inf, -np.inf], np.nan)

        pair_1h = self.dp.get_pair_dataframe(metadata["pair"], timeframe="1h")
        if not pair_1h.empty:
            pair_1h["ema_200"] = ta.EMA(pair_1h, timeperiod=200)
            pair_1h["rsi"] = ta.RSI(pair_1h, timeperiod=14)
            pair_1h["adx"] = ta.ADX(pair_1h, timeperiod=14)
            dataframe = merge_informative_pair(dataframe, pair_1h[["date", "ema_200", "rsi", "adx"]], self.timeframe, "1h", ffill=True)

        btc_1h = self.dp.get_pair_dataframe("BTC/USDC:USDC", timeframe="1h")
        if not btc_1h.empty:
            btc_1h["btc_close"] = btc_1h["close"]
            btc_1h["btc_ema_50"] = ta.EMA(btc_1h, timeperiod=50)
            btc_1h["btc_ema_200"] = ta.EMA(btc_1h, timeperiod=200)
            btc_1h["btc_adx"] = ta.ADX(btc_1h, timeperiod=14)
            btc_1h["btc_bull"] = ((btc_1h["btc_close"] > btc_1h["btc_ema_200"]) & (btc_1h["btc_ema_50"] > btc_1h["btc_ema_200"]) & (btc_1h["btc_adx"] > 18)).astype(int)
            btc_1h["btc_bear"] = ((btc_1h["btc_close"] < btc_1h["btc_ema_200"]) & (btc_1h["btc_ema_50"] < btc_1h["btc_ema_200"]) & (btc_1h["btc_adx"] > 18)).astype(int)
            dataframe = merge_informative_pair(dataframe, btc_1h[["date", "btc_close", "btc_ema_50", "btc_ema_200", "btc_adx", "btc_bull", "btc_bear"]], self.timeframe, "1h", ffill=True)
        else:
            dataframe["btc_bull_1h"] = 0
            dataframe["btc_bear_1h"] = 0

        dataframe = dataframe.fillna(method="ffill")
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        long_cond = (
            (dataframe.get("btc_bull_1h", 0) == 1)
            & (dataframe["close"] > dataframe["ema_200"])
            & (dataframe["close"] < dataframe["bb_lower"] * 1.01)
            & (dataframe["rsi"] < 38)
            & (dataframe["adx"] > 16)
            & (dataframe["volume"] > dataframe["volume_mean_20"] * 0.8)
        )

        short_cond = (
            (dataframe.get("btc_bear_1h", 0) == 1)
            & (dataframe["close"] < dataframe["ema_200"])
            & (dataframe["close"] > dataframe["bb_upper"] * 0.99)
            & (dataframe["rsi"] > 62)
            & (dataframe["adx"] > 16)
            & (dataframe["volume"] > dataframe["volume_mean_20"] * 0.8)
        )

        dataframe.loc[long_cond, ["enter_long", "enter_tag"]] = (1, "cbadv_fut_long_pullback")
        dataframe.loc[short_cond, ["enter_short", "enter_tag"]] = (1, "cbadv_fut_short_reject")
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        exit_long = (
            (dataframe["rsi"] > 58)
            | (dataframe["close"] > dataframe["bb_mid"])
        )
        exit_short = (
            (dataframe["rsi"] < 42)
            | (dataframe["close"] < dataframe["bb_mid"])
        )

        dataframe.loc[exit_long, ["exit_long", "exit_tag"]] = (1, "cbadv_fut_exit_long")
        dataframe.loc[exit_short, ["exit_short", "exit_tag"]] = (1, "cbadv_fut_exit_short")
        return dataframe
