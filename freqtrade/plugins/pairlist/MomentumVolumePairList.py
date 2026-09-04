# ruff: noqa: N999
"""
Momentum / quote-volume pairlist provider.

Builds a dynamic universe of linear perpetual contracts in the configured
stake currency.  Candidates must satisfy a rolling 24-hour quote-volume
threshold, then are ranked by their rolling 24-hour price momentum.
"""

import logging
import math
from typing import Any

from freqtrade.exceptions import OperationalException
from freqtrade.exchange.exchange_types import Ticker, Tickers
from freqtrade.plugins.pairlist.IPairList import IPairList, PairlistParameter, SupportsBacktesting
from freqtrade.util import FtTTLCache


logger = logging.getLogger(__name__)


class MomentumVolumePairList(IPairList):
    """
    Select active, tradable linear perpetuals by 24-hour momentum after a
    24-hour quote-volume screen.

    The market universe is derived from the exchange adapter's live market
    metadata.  In Binance PM mode this therefore inherits the adapter's
    USDT/USDC linear-perpetual eligibility rules, rather than making an
    assumption about the underlying asset class.
    """

    is_pairlist_generator = True
    supports_backtesting = SupportsBacktesting.NO

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if self._config.get("trading_mode") != "futures":
            raise OperationalException("MomentumVolumePairList requires trading_mode='futures'.")
        if "number_assets" not in self._pairlistconfig:
            raise OperationalException(
                "`number_assets` not specified. Please check your configuration "
                'for "pairlist.config.number_assets"'
            )

        self._stake_currency: str = self._config["stake_currency"]
        self._number_pairs = self._positive_int("number_assets")
        self._min_quote_volume = self._nonnegative_float("min_quote_volume", default=200_000_000.0)
        # "Top N by momentum" means ranking the whole eligible universe.  Do
        # not silently discard negative-momentum contracts in a broad downtrend
        # unless an operator explicitly asks for that extra entry gate.
        self._min_momentum = self._optional_float("min_momentum", default=None)
        self._refresh_period = self._positive_int("refresh_period", default=900)
        self._pair_cache: FtTTLCache = FtTTLCache(maxsize=1, ttl=self._refresh_period)

        if not self._exchange.exchange_has("fetchTickers"):
            raise OperationalException(
                f"Exchange {self._exchange.name} does not support fetchTickers, so "
                "MomentumVolumePairList cannot build a dynamic whitelist."
            )
        if not self._exchange.get_option("tickers_have_quoteVolume"):
            raise OperationalException(
                f"Exchange {self._exchange.name} does not provide ticker quoteVolume, so "
                "MomentumVolumePairList cannot enforce min_quote_volume."
            )

    @property
    def needstickers(self) -> bool:
        """This pairlist uses the exchange's rolling 24-hour ticker statistics."""
        return True

    def short_desc(self) -> str:
        min_momentum = (
            f", momentum >= {self._min_momentum:g}%" if self._min_momentum is not None else ""
        )
        return (
            f"{self.name} - top {self._number_pairs} {self._stake_currency} linear perpetuals "
            f"by 24h momentum after 24h quote volume >= {self._min_quote_volume:,.0f}"
            f"{min_momentum}."
        )

    @staticmethod
    def description() -> str:
        return (
            "Provides a dynamic list of active linear perpetuals, screened by rolling "
            "24-hour quote volume and ranked by rolling 24-hour price momentum."
        )

    @staticmethod
    def available_parameters() -> dict[str, PairlistParameter]:
        return {
            "number_assets": {
                "type": "number",
                "default": 20,
                "description": "Number of assets",
                "help": "Maximum number of contracts to retain after ranking by momentum.",
            },
            "min_quote_volume": {
                "type": "number",
                "default": 200_000_000,
                "description": "Minimum 24h quote volume",
                "help": "Rolling 24-hour quote volume threshold in the stake currency.",
            },
            "min_momentum": {
                "type": "number",
                "default": None,
                "description": "Minimum 24h momentum",
                "help": (
                    "Minimum rolling 24-hour percentage price change. "
                    "Set to null to allow negative values."
                ),
            },
            **IPairList.refresh_period_parameter(),
        }

    def gen_pairlist(self, tickers: Tickers) -> list[str]:
        """Generate a fresh list from eligible exchange-market metadata and tickers."""
        if pairlist := self._pair_cache.get("pairlist"):
            return pairlist.copy()

        eligible_pairs = list(
            self._exchange.get_markets(
                quote_currencies=[self._stake_currency],
                futures_only=True,
                tradable_only=True,
                active_only=True,
            ).keys()
        )
        eligible_pairs = self._pm_verified_pairs(eligible_pairs)
        eligible_pairs = self.verify_blacklist(eligible_pairs, logger.info)
        pairlist = self._rank_pairs(eligible_pairs, tickers)
        self._pair_cache["pairlist"] = pairlist.copy()
        logger.info(
            "MomentumVolumePairList refreshed: eligible=%s selected=%s "
            "min_quote_volume=%.0f refresh_period_s=%s pairs=%s.",
            len(eligible_pairs),
            len(pairlist),
            self._min_quote_volume,
            self._refresh_period,
            pairlist,
        )
        return pairlist

    def filter_pairlist(self, pairlist: list[str], tickers: Tickers) -> list[str]:
        """Allow the selector to be composed after another pairlist if desired."""
        return self._rank_pairs(pairlist, tickers)

    def _rank_pairs(self, pairlist: list[str], tickers: Tickers) -> list[str]:
        ranked: list[tuple[str, float, float]] = []
        for pair in pairlist:
            market = self._exchange.markets.get(pair)
            ticker = tickers.get(pair)
            if not market or not ticker or not self._is_eligible_market(market):
                continue

            quote_volume = self._quote_volume(ticker)
            momentum = self._momentum_percent(ticker)
            if quote_volume is None or momentum is None:
                continue
            if quote_volume < self._min_quote_volume:
                continue
            if self._min_momentum is not None and momentum < self._min_momentum:
                continue
            ranked.append((pair, momentum, quote_volume))

        # Stable final symbol ordering makes equal ticker values reproducible.
        ranked.sort(key=lambda item: (-item[1], -item[2], item[0]))
        pairs = self._whitelist_for_active_markets([pair for pair, _, _ in ranked])
        pairs = self.verify_blacklist(pairs, logger.info)
        return pairs[: self._number_pairs]

    def _pm_verified_pairs(self, pairs: list[str]) -> list[str]:
        """Intersect public perpetual metadata with the PM account's symbol configuration."""
        exchange_config = self._config.get("exchange", {})
        if not exchange_config.get("portfolio_margin"):
            return pairs

        provider = getattr(self._exchange, "get_pm_tradable_pairs", None)
        if not callable(provider):
            raise OperationalException(
                "Portfolio Margin requires an exchange adapter that exposes "
                "get_pm_tradable_pairs() for account-level symbol verification."
            )
        pm_pairs = provider()
        if not pm_pairs:
            raise OperationalException(
                "Binance PM returned no account-tradable UM perpetuals; refusing the dynamic whitelist."
            )
        return [pair for pair in pairs if pair in pm_pairs]

    def _is_eligible_market(self, market: dict[str, Any]) -> bool:
        """Limit the selector to the configured quote/settlement linear-perpetual universe."""
        return (
            self._exchange.market_is_tradable(market)
            and self._exchange.market_is_future(market)
            and market.get("swap") is True
            and market.get("linear") is True
            and market.get("quote") == self._stake_currency
            and market.get("settle") == self._stake_currency
        )

    @staticmethod
    def _finite_float(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    def _quote_volume(self, ticker: Ticker) -> float | None:
        quote_volume = self._finite_float(ticker.get("quoteVolume"))
        if quote_volume is not None:
            return quote_volume
        info = ticker.get("info")
        return self._finite_float(info.get("quoteVolume")) if isinstance(info, dict) else None

    def _momentum_percent(self, ticker: Ticker) -> float | None:
        momentum = self._finite_float(ticker.get("percentage"))
        if momentum is not None:
            return momentum

        # CCXT normally maps Binance priceChangePercent to ``percentage``.  The fallback
        # keeps the selector compatible with exchanges/adapters that expose open and last only.
        open_price = self._finite_float(ticker.get("open"))
        last_price = self._finite_float(ticker.get("last"))
        if open_price is None or last_price is None or open_price <= 0:
            return None
        return (last_price / open_price - 1.0) * 100.0

    def _positive_int(self, key: str, default: int | None = None) -> int:
        value = self._pairlistconfig.get(key, default)
        if isinstance(value, bool):
            value = None
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise OperationalException(
                f"pairlist.config.{key} must be a positive integer."
            ) from exc
        if parsed <= 0 or parsed != value:
            raise OperationalException(f"pairlist.config.{key} must be a positive integer.")
        return parsed

    def _nonnegative_float(self, key: str, default: float) -> float:
        value = self._finite_float(self._pairlistconfig.get(key, default))
        if value is None or value < 0:
            raise OperationalException(f"pairlist.config.{key} must be a non-negative number.")
        return value

    def _optional_float(self, key: str, default: float | None) -> float | None:
        raw_value = self._pairlistconfig.get(key, default)
        if raw_value is None:
            return None
        value = self._finite_float(raw_value)
        if value is None:
            raise OperationalException(f"pairlist.config.{key} must be a number or null.")
        return value
