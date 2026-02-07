"""Coinbase exchange subclass"""

import logging

from freqtrade.exchange import Exchange
from freqtrade.exchange.exchange_types import FtHas


logger = logging.getLogger(__name__)


class Coinbase(Exchange):
    """Coinbase exchange class.
    Contains adjustments needed for Freqtrade to work with this exchange.
    """

    _ft_has: FtHas = {
        "ohlcv_candle_limit": 300,
        "order_time_in_force": ["GTC", "IOC", "FOK", "PO"],
        "trades_has_history": True,
    }
