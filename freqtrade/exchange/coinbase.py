"""Coinbase exchange subclass"""

import logging

from freqtrade.exchange import Exchange


logger = logging.getLogger(__name__)


class Coinbase(Exchange):
    """Coinbase exchange class.
    Contains adjustments needed for Freqtrade to work with this exchange.
    """

    _ft_has = {
        "ohlcv_candle_limit": 300,
    }
