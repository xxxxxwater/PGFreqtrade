"""Coinbase Advanced Trade exchange subclass."""

import logging

from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exchange import Exchange


logger = logging.getLogger(__name__)


class Coinbase(Exchange):
    """Coinbase Advanced Trade exchange class."""

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
    ]
