"""Static assumptions / notes for Coinbase Advanced futures support in test branch.

These values are NOT exchange truth. They document project-side assumptions so
future iterations can tighten them against real Coinbase Advanced responses.
"""

from __future__ import annotations

COINBASE_ADVANCED_FUTURES_ASSUMPTIONS = {
    "symbol_style": "BASE/USDC:USDC",
    "default_settle": "USDC",
    "default_margin_mode": "isolated",
    "default_linear_only": True,
    "supports_reduce_only": True,
    "supports_post_only": True,
    "supports_market_orders": True,
    "supports_mark_price_ohlcv": True,
    "supports_funding_fee_fetch_fallback": True,
}
