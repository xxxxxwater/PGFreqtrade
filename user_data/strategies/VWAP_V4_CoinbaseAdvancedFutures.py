from __future__ import annotations

"""Coinbase Advanced futures-compatible strategy wrapper for the test tree.

This keeps the original VWAP_V4 logic intact but adapts runtime defaults for:
- Futures mode
- Linear USDC contracts (CCXT symbol style: BASE/USDC:USDC)
- More conservative capital deployment for derivatives testing
"""

from VWAP_V4 import SampleStrategy as SpotVWAPStrategy


class CoinbaseAdvancedFuturesVWAP(SpotVWAPStrategy):
    INTERFACE_VERSION = 3
    can_short = False
    timeframe = "5m"

    # More conservative futures defaults than the spot version.
    stoploss = -0.12
    startup_candle_count = 240
    position_adjustment_enable = False

    minimal_roi = {
        "0": 0.015,
        "60": 0.01,
        "120": 0.005,
        "180": 0
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

    def informative_pairs(self):
        pairs = self.dp.current_whitelist()
        btc_pair = "BTC/USDC:USDC"
        informative_pairs = [(pair, "1h") for pair in pairs]
        informative_pairs += [(btc_pair, "5m"), (btc_pair, "1h")]
        return list(dict.fromkeys(informative_pairs))
