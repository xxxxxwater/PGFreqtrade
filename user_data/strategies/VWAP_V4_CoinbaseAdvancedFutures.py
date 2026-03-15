from __future__ import annotations

"""Coinbase Advanced futures strategy for the test tree.

This keeps the original VWAP_V4 signal engine for long entries, but adds:
- CCXT futures pair normalization helpers
- an optional short signal path
- futures-friendly risk defaults
- reduced DCA exposure
- more explicit BTC informative pair handling for futures symbols
"""

from pandas import DataFrame

from VWAP_V4 import SampleStrategy as SpotVWAPStrategy
from coinbase_advanced_strategy_utils import (
    default_coinbase_btc_reference_pair,
    normalize_coinbase_futures_pairs,
)


class CoinbaseAdvancedFuturesVWAP(SpotVWAPStrategy):
    INTERFACE_VERSION = 3
    can_short = True
    timeframe = "5m"

    stoploss = -0.12
    startup_candle_count = 240
    position_adjustment_enable = False
    use_custom_stoploss = True

    minimal_roi = {
        "0": 0.015,
        "60": 0.01,
        "120": 0.005,
        "180": 0,
    }

    order_types = {
        "entry": "market",
        "exit": "market",
        "emergency_exit": "market",
        "force_entry": "market",
        "force_exit": "market",
        "stoploss": "market",
        "stoploss_on_exchange": False,
        "stoploss_on_exchange_interval": 60,
        "stoploss_on_exchange_limit_ratio": 0.99,
    }

    order_time_in_force = {
        "entry": "GTC",
        "exit": "GTC",
    }

    short_rsi_threshold = 66
    short_bb_width_threshold = 0.08
    short_cti_threshold = 0.72

    @property
    def btc_reference_pair(self) -> str:
        return default_coinbase_btc_reference_pair('USDC')

    def informative_pairs(self):
        pairs = normalize_coinbase_futures_pairs(self.dp.current_whitelist(), settle='USDC')
        btc_pair = self.btc_reference_pair
        informative_pairs = [(pair, "1h") for pair in pairs]
        informative_pairs += [(btc_pair, "5m"), (btc_pair, "1h")]
        return list(dict.fromkeys(informative_pairs))

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        return super().populate_indicators(dataframe, metadata)

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe = super().populate_entry_trend(dataframe, metadata)

        trend_regime = dataframe["btc_regime_trend"] == 1
        range_regime = dataframe["btc_regime_range"] == 1

        dataframe.loc[
            (
                trend_regime
                & (dataframe["rsi"] > self.short_rsi_threshold)
                & (dataframe["cti"] > self.short_cti_threshold)
                & (dataframe["bb_width"] > self.short_bb_width_threshold)
                & (dataframe["close"] > dataframe["bb_upperband2"])
                & (dataframe["close"] > dataframe["ema_50"])
                & (dataframe["volume"] > 0)
            ),
            ["enter_short", "enter_tag"],
        ] = (1, "coinbase_futures_short_trend_exhaust")

        dataframe.loc[
            (
                range_regime
                & (dataframe["rsi"] > 70)
                & (dataframe["fisher"] > 0.8)
                & (dataframe["close"] > dataframe["bb_upperband3"])
                & (dataframe["volume"] > 0)
            ),
            ["enter_short", "enter_tag"],
        ] = (1, "coinbase_futures_short_mean_revert")

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe = super().populate_exit_trend(dataframe, metadata)

        dataframe.loc[
            (
                (dataframe["rsi"] < 35)
                & (dataframe["close"] < dataframe["bb_middleband2"])
                & (dataframe["volume"] > 0)
            ),
            ["exit_short", "exit_tag"],
        ] = (1, "coinbase_futures_short_cover")

        return dataframe
