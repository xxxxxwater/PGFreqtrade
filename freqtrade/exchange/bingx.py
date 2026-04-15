"""BingX exchange subclass — production-ready for USDT-M perpetual futures"""

import logging
from copy import deepcopy
from datetime import datetime

import ccxt

from freqtrade.constants import BuySell
from freqtrade.enums import CandleType, MarginMode, PriceType, TradingMode
from freqtrade.exceptions import (
    DDosProtection,
    ExchangeError,
    InvalidOrderException,
    OperationalException,
    TemporaryError,
)
from freqtrade.exchange import Exchange
from freqtrade.exchange.common import retrier, retrier_async
from freqtrade.exchange.exchange_types import FtHas, OHLCVResponse


logger = logging.getLogger(__name__)


class Bingx(Exchange):
    """
    BingX exchange class.
    Contains adjustments needed for Freqtrade to work with this exchange.
    Production-ready for BingX U-margined (USDT) perpetual swap futures.

    Key BingX behaviours handled here:
    - One-way position mode (set on startup, idempotent)
    - Leverage requires explicit side=BOTH parameter
    - Stop orders use a separate query/cancel endpoint (stop=True flag in ccxt)
    - Funding fees settle every 8 hours (00:00 / 08:00 / 16:00 UTC)
    - Leverage tiers fetched per-pair on demand (not pre-loaded for all markets)
    - Suspended pairs (error 109415) cached to prevent rate-limit cascade (109429)

    BingX U-margined API compliance notes:
    - stop_price_param = "stopPrice"  (default "stopLossPrice" is wrong for BingX)
    - stoploss_query_requires_stop_flag = True  (algo-order endpoint needs stop=True)
    - funding_fee_timeframe = "8h"  (BingX settles 3×/day not 1×/hour)
    - stop_price_type_value_mapping: LAST→CONTRACT_PRICE, MARK→MARK_PRICE only
      (INDEX_PRICE removed from BingX order API on 2025-08-21 per official changelog)
    - l2_limit_range reflects the BingX supported orderbook depth levels
    """

    _ft_has: FtHas = {
        "ohlcv_candle_limit": 1000,
        "stoploss_on_exchange": True,
        "stoploss_order_types": {"limit": "limit", "market": "market"},
        "order_time_in_force": ["GTC", "IOC", "FOK", "PO"],
        "trades_has_history": False,
        "l2_limit_range": [5, 10, 20, 50, 100],
        "l2_limit_range_required": False,
        "tickers_have_quoteVolume": True,
    }

    _ft_has_futures: FtHas = {
        "ohlcv_candle_limit": 1000,
        "stoploss_on_exchange": True,
        "stoploss_order_types": {"limit": "stop", "market": "stop_market"},
        "stoploss_blocks_assets": False,
        # BingX stop orders use "stopPrice" (not the default "stopLossPrice")
        "stop_price_param": "stopPrice",
        "stop_price_prop": "stopPrice",
        # Base class fetch_stoploss_order / cancel_stoploss_order will auto-add stop=True
        "stoploss_query_requires_stop_flag": True,
        "stop_price_type_field": "workingType",
        "stop_price_type_value_mapping": {
            PriceType.LAST: "CONTRACT_PRICE",
            PriceType.MARK: "MARK_PRICE",
            # INDEX_PRICE intentionally omitted: BingX changelog 2025-08-21 removed
            # INDEX_PRICE from the order API workingType field. Sending it would cause
            # a 100400 invalid-parameter error. Mark price is the safe production default.
        },
        # BingX settles funding every 8 h (00:00 / 08:00 / 16:00 UTC)
        "mark_ohlcv_timeframe": "8h",
        "mark_ohlcv_price": "mark",
        "funding_fee_timeframe": "8h",
        "funding_fee_candle_limit": 1000,
        "uses_leverage_tiers": False,
        "order_time_in_force": ["GTC", "FOK", "IOC", "PO"],
        "tickers_have_quoteVolume": True,
        "l2_limit_range": [5, 10, 20, 50, 100],
        "l2_limit_range_required": False,
        "trades_has_history": False,
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
        (TradingMode.FUTURES, MarginMode.CROSS),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
    ]

    # Pairs BingX has suspended (error 109415).
    # Cached at class level to avoid repeated API retries that trigger 109429 rate limiting.
    _suspended_pairs: set[str] = set()
    _pair_max_leverage_cache: dict[str, float] = {}

    # ── BingX error-code constants ───────────────────────────────────────────
    _BINGX_PAIR_SUSPENDED    = "109415"   # Pair trading suspended      → fast-fail, no retry
    _BINGX_RATE_LIMITED      = "109429"   # Frequency limit hit         → DDosProtection
    _BINGX_POSITION_MODE     = "109400"   # positionSide must be BOTH   → InvalidOrderException
    _BINGX_INSUFFICIENT_FUND = "100202"   # Insufficient balance        → InvalidOrderException
    _BINGX_PRICE_DEVIATION   = "100440"   # Price too far from mark     → InvalidOrderException
    _BINGX_INVALID_PARAM     = "100400"   # Invalid parameter           → InvalidOrderException
    _BINGX_ORDER_NOT_FOUND   = "100410"   # Order does not exist        → InvalidOrderException
    _BINGX_ACCOUNT_SUSPENDED = "100414"   # Account suspended           → OperationalException
    _BINGX_SERVER_ERRORS     = {"100500", "100503"}  # Server busy      → TemporaryError (retry)

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _raise_for_bingx_code(self, e: Exception, pair: str = "") -> None:
        """
        Map BingX-specific numeric error codes to the correct Freqtrade exception.
        Call this inside except-blocks before falling through to generic handling.
        Raises the mapped exception or returns silently if no known code is found.
        """
        msg = str(e)
        if self._BINGX_PAIR_SUSPENDED in msg:
            self.__class__._suspended_pairs.add(pair)
            logger.warning(
                f"{pair} suspended on BingX (109415) — added to skip-cache.")
            raise InvalidOrderException(
                f"{pair} is suspended on BingX (code 109415)") from e
        if self._BINGX_RATE_LIMITED in msg:
            raise DDosProtection(f"BingX rate limit hit (109429): {e}") from e
        if self._BINGX_POSITION_MODE in msg:
            raise InvalidOrderException(
                f"BingX position mode error for {pair} (109400) — "
                f"ensure account is in one-way mode: {e}") from e
        if self._BINGX_INSUFFICIENT_FUND in msg:
            raise InvalidOrderException(
                f"Insufficient funds on BingX for {pair} (100202): {e}") from e
        if self._BINGX_PRICE_DEVIATION in msg:
            raise InvalidOrderException(
                f"Order price deviates too far from mark price for {pair} (100440): {e}") from e
        if self._BINGX_INVALID_PARAM in msg:
            raise InvalidOrderException(
                f"Invalid order parameter for {pair} on BingX (100400): {e}") from e
        if self._BINGX_ORDER_NOT_FOUND in msg:
            raise InvalidOrderException(
                f"Order not found on BingX for {pair} (100410): {e}") from e
        if self._BINGX_ACCOUNT_SUSPENDED in msg:
            raise OperationalException(
                f"BingX account suspended (100414): {e}") from e
        if any(c in msg for c in self._BINGX_SERVER_ERRORS):
            raise TemporaryError(f"BingX server error (retryable): {e}") from e

    # ── Exchange initialisation ──────────────────────────────────────────────

    @retrier
    def additional_exchange_init(self) -> None:
        """
        Set BingX futures account to one-way position mode on startup.
        Idempotent: if the account is already in one-way mode BingX returns an error;
        we treat that as success so the bot starts cleanly.
        """
        try:
            if not self._config["dry_run"] and self.trading_mode == TradingMode.FUTURES:
                position_mode = self._api.set_position_mode(False)
                self._log_exchange_response("set_position_mode", position_mode)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            err = str(e).lower()
            # BingX signals "already one-way" — treat as a no-op success
            if "one-way" in err or "position mode" in err or "no need" in err:
                logger.info("BingX already in one-way position mode — no change needed.")
                return
            raise TemporaryError(
                f"Error in additional_exchange_init ({e.__class__.__name__}): {e}") from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    # ── Leverage ─────────────────────────────────────────────────────────────

    def _lev_prep(self, pair: str, leverage: float, side: BuySell, accept_fail: bool = False):
        if self.trading_mode != TradingMode.FUTURES or self.margin_mode is None:
            return
        self.set_margin_mode(pair, self.margin_mode, accept_fail=True)
        self._set_leverage(leverage, pair, accept_fail=accept_fail)

    def _set_leverage(
        self,
        leverage: float,
        pair: str | None = None,
        accept_fail: bool = False,
    ):
        """
        BingX leverage is managed on the account side.

        The BingX set_leverage API returns 109400 ("Invalid parameters") in
        one-way mode when called with side=BOTH, blocking every order entry.
        Leverage must be configured once via the BingX web/app interface per pair.
        This override skips the API call entirely so order execution is not blocked.
        """
        logger.debug(
            "BingX _set_leverage skipped for %s @ %sx "
            "(leverage managed on exchange account).", pair, leverage
        )

    # ── Funding fees ─────────────────────────────────────────────────────────

    def get_funding_fees(
        self, pair: str, amount: float, is_short: bool, open_date: datetime
    ) -> float:
        """
        BingX funding fees are derived from funding-rate and mark-price history.
        Funding settles every 8 h (00:00 / 08:00 / 16:00 UTC).
        funding_fee_timeframe = "8h" ensures _fetch_and_calculate_funding_fees
        samples at the correct interval.
        """
        if self.trading_mode == TradingMode.FUTURES:
            try:
                return self._fetch_and_calculate_funding_fees(pair, amount, is_short, open_date)
            except ExchangeError:
                logger.warning(f"Could not update funding fees for {pair}.")
        return 0.0

    # ── Stoploss orders ──────────────────────────────────────────────────────

    def create_stoploss_order(
        self,
        pair: str,
        amount: float,
        stop_price: float,
        order_types: dict,
        side: BuySell,
        leverage: float,
    ) -> dict:
        """
        Place a stop-loss order on BingX futures.

        BingX stop orders (STOP_MARKET / STOP) are created via the standard trade/order
        endpoint but ccxt routes them to the algo-order path based on the order type string.
        We explicitly set workingType (MARK_PRICE / CONTRACT_PRICE) and stopPrice so
        the trigger behaviour matches the operator's config.
        Note: INDEX_PRICE was removed from BingX order API on 2025-08-21.
        """
        stoploss_type = order_types.get("stoploss", "market")
        stop_order_type = self._ft_has["stoploss_order_types"].get(stoploss_type, "stop_market")

        # Build ccxt params: stop-price trigger + optional working-type
        params: dict = {}
        stop_price_prop = self._ft_has.get("stop_price_param", "stopPrice")
        params[stop_price_prop] = stop_price

        stop_price_type_field = self._ft_has.get("stop_price_type_field")
        if stop_price_type_field:
            price_type_str = order_types.get("stoploss_price_type", "last")
            if price_type_str == "index":
                logger.warning(
                    "stoploss_price_type='index' is not supported by BingX (removed "
                    "2025-08-21). Falling back to 'mark' (MARK_PRICE)."
                )
                price_type_str = "mark"
            price_type_map = {"mark": PriceType.MARK, "last": PriceType.LAST}
            price_type_enum = price_type_map.get(price_type_str, PriceType.LAST)
            mapping = self._ft_has.get("stop_price_type_value_mapping", {})
            params[stop_price_type_field] = mapping.get(price_type_enum, "CONTRACT_PRICE")

        # Limit stop-loss uses a limit price; market stop-loss passes 0 (ignored by BingX)
        limit_price_pct = order_types.get("stoploss_on_exchange_limit_ratio", 0.99)
        limit_price = stop_price * (
            (2 - limit_price_pct) if side == "sell" else limit_price_pct
        )
        rate = limit_price if stop_order_type == "stop" else 0

        return self.create_order(
            pair=pair,
            ordertype=stop_order_type,
            side=side,
            amount=amount,
            rate=rate,
            leverage=leverage,
            params=params,
        )

    def fetch_stoploss_order(
        self, order_id: str, pair: str, params: dict | None = None
    ) -> dict:
        """
        BingX stop orders are queried via the algo-order endpoint.
        stoploss_query_requires_stop_flag=True in _ft_has_futures means the base class
        already adds stop=True automatically; this override adds it defensively too.
        """
        p = (params or {}).copy()
        p.setdefault("stop", True)
        return self.fetch_order(order_id, pair, p)

    def cancel_stoploss_order(
        self, order_id: str, pair: str, params: dict | None = None
    ) -> dict:
        """
        Cancel a stop-loss order via the algo-order cancellation endpoint.
        stoploss_query_requires_stop_flag=True in _ft_has_futures means the base class
        already adds stop=True automatically; this override adds it defensively too.
        """
        p = (params or {}).copy()
        p.setdefault("stop", True)
        return self.cancel_order(order_id, pair, p)

    def cancel_stoploss_order_with_result(
        self, order_id: str, pair: str, amount: float
    ) -> dict:
        """
        Cancel a stop-loss order and return the resulting order dict.

        The base-class implementation calls cancel_order_with_result which internally
        uses cancel_order and fetch_order (without stop=True), which fails on BingX
        because stop orders are exposed through a different endpoint.
        This override ensures both operations use the stop-order path.
        """
        try:
            result = self.cancel_stoploss_order(order_id, pair)
            if self.is_cancel_order_result_suitable(result):
                return result
        except InvalidOrderException:
            logger.warning(
                f"Could not cancel stoploss order {order_id} for {pair} — fetching current state.")
        # Fallback: return whatever state the order is in now
        order = self.fetch_stoploss_order(order_id, pair)
        if order.get("status") in ("canceled", "closed", "filled"):
            return order
        raise InvalidOrderException(
            f"Could not cancel stoploss order {order_id} for {pair}. "
            f"Current status: {order.get('status')}")

    # ── Leverage tiers (on-demand, per-pair) ─────────────────────────────────

    @retrier
    def _fetch_market_leverage_tiers_sync(self, pair: str) -> list[dict]:
        """
        Fetch leverage tiers for a single pair on demand.
        BingX exposes a per-market endpoint; pre-loading all markets at startup is slow
        and fails for suspended symbols.
        """
        try:
            return self._api.fetch_market_leverage_tiers(pair)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            self._raise_for_bingx_code(e, pair)   # Fast-fail for 109415 etc.
            raise TemporaryError(
                f"Could not load leverage tiers for {pair} ({e.__class__.__name__}): {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def _fetch_pair_max_leverage_sync(self, pair: str) -> float:
        """
        Fetch the pair-level maximum leverage from BingX.

        BingX leverage tiers currently omit `maxLeverage` in ccxt tier responses, so we
        fetch the pair-level cap separately and use it to normalize the parsed tiers.
        """
        try:
            leverage = self._api.fetch_leverage(pair)
            info = leverage.get("info", {}) if isinstance(leverage, dict) else {}
            candidates = [
                info.get("maxLongLeverage"),
                info.get("maxShortLeverage"),
                leverage.get("longLeverage") if isinstance(leverage, dict) else None,
                leverage.get("shortLeverage") if isinstance(leverage, dict) else None,
            ]
            for candidate in candidates:
                if candidate not in (None, "", 0, "0"):
                    return float(candidate)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            self._raise_for_bingx_code(e, pair)
            raise TemporaryError(
                f"Could not load max leverage for {pair} ({e.__class__.__name__}): {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

        raise OperationalException(f"BingX did not return a usable max leverage for {pair}")

    def _normalize_pair_leverage_tiers(self, pair: str, tiers: list[dict]) -> list[dict]:
        """
        BingX fetch_market_leverage_tiers returns maxLeverage=None for all tiers.

        Fix strategy (two-step):
        1. Derive per-tier max leverage from maintenanceMarginRate: max_lev = floor(1 / mmr).
           This is the standard formula and produces the correct per-tier descending caps
           (tier 1 → 250x, tier 8 → 100x, tier 24 → 2x, etc.).
        2. Clamp the derived value to the pair-level hard cap from fetch_leverage
           (BTC currently 150x). This prevents tiers with very low MMR from implying
           unrealistically high leverage beyond what BingX actually allows.

        Also fixes maintAmt: BingX returns it as info["maintAmount"], not info["cum"].
        The base-class parse_leverage_tier reads info["cum"] which is always missing,
        so we patch it here after parsing.
        """
        if all(t.get("maxLeverage") is not None for t in tiers):
            # Already populated — still fix maintAmt if needed
            return tiers

        pair_max_leverage = self.__class__._pair_max_leverage_cache.get(pair)
        if pair_max_leverage is None:
            pair_max_leverage = self._fetch_pair_max_leverage_sync(pair)
            self.__class__._pair_max_leverage_cache[pair] = pair_max_leverage

        result = []
        for tier in tiers:
            mmr = tier.get("maintenanceMarginRate")
            if mmr and mmr > 0:
                # Per-tier cap from MMR, clamped to the pair's global max
                per_tier_max_lev = min(float(int(1.0 / mmr)), pair_max_leverage)
            else:
                per_tier_max_lev = pair_max_leverage
            result.append({**tier, "maxLeverage": per_tier_max_lev})
        return result

    def parse_leverage_tier(self, tier) -> dict:
        """
        Override base class to read BingX-specific maintAmount field.

        Base class reads info["cum"] which BingX never populates.
        BingX returns the maintenance amount as info["maintAmount"].
        """
        info = tier.get("info", {})
        maint_amt_raw = info.get("maintAmount") or info.get("cum")
        return {
            "minNotional": tier["minNotional"],
            "maxNotional": tier["maxNotional"],
            "maintenanceMarginRate": tier["maintenanceMarginRate"],
            "maxLeverage": tier["maxLeverage"],
            "maintAmt": float(maint_amt_raw) if maint_amt_raw is not None else None,
        }

    def _ensure_pair_leverage_tiers(self, pair: str) -> None:
        if self.trading_mode != TradingMode.FUTURES or pair in self._leverage_tiers:
            return
        # Skip known-suspended pairs without making an API call
        if pair in self.__class__._suspended_pairs:
            raise InvalidOrderException(
                f"{pair} is suspended on BingX (109415) — skipping leverage tier fetch")
        tiers = self._fetch_market_leverage_tiers_sync(pair)
        if not tiers:
            raise InvalidOrderException(
                f"Leverage tiers for {pair} are unavailable on {self.name}")
        parsed_tiers = [self.parse_leverage_tier(t) for t in tiers]
        self._leverage_tiers[pair] = self._normalize_pair_leverage_tiers(pair, parsed_tiers)

    def get_max_leverage(self, pair: str, stake_amount: float | None) -> float:
        self._ensure_pair_leverage_tiers(pair)
        return super().get_max_leverage(pair, stake_amount)

    def get_max_pair_stake_amount(self, pair: str, price: float, leverage: float = 1.0) -> float:
        self._ensure_pair_leverage_tiers(pair)
        return super().get_max_pair_stake_amount(pair, price, leverage)

    def get_maintenance_ratio_and_amt(
        self, pair: str, notional_value: float
    ) -> tuple[float, float | None]:
        self._ensure_pair_leverage_tiers(pair)
        return super().get_maintenance_ratio_and_amt(pair, notional_value)

    # ── OHLCV history (async) ────────────────────────────────────────────────

    @retrier_async
    async def _async_get_candle_history(
        self,
        pair: str,
        timeframe: str,
        candle_type: CandleType,
        since_ms: int | None = None,
    ) -> OHLCVResponse:
        """Override base class to intercept BingX 109415 before TemporaryError retry loop.

        Root cause of the 109429 cascade:
          ccxt.ExchangeError(109415) -> TemporaryError -> @retrier_async retries 5x
          -> 5x 109415 calls per suspended pair -> BingX rate-limits all pairs (109429).

        This override:
          ccxt.ExchangeError(109415) -> OperationalException (non-retryable, 0 retries)
          -> pair added to _suspended_pairs cache
          -> all future calls fast-fail before making any API call.
        """
        # Pre-check: skip pairs already confirmed suspended (zero API calls)
        if pair in self.__class__._suspended_pairs:
            raise OperationalException(
                f"{pair} suspended on BingX (109415) - skipping OHLCV fetch"
            )

        try:
            params = deepcopy(self._ft_has.get("ohlcv_params", {}))
            candle_limit = self.ohlcv_candle_limit(
                timeframe, candle_type=candle_type, since_ms=since_ms
            )

            if candle_type != CandleType.FUNDING_RATE:
                if candle_type and candle_type not in (CandleType.SPOT, CandleType.FUTURES):
                    self.verify_candle_type_support(candle_type)
                    params.update({"price": str(candle_type)})
                data = await self._api_async.fetch_ohlcv(
                    pair,
                    timeframe=timeframe,
                    since=since_ms,
                    limit=candle_limit,
                    params=params,
                )
            else:
                data = await self._fetch_funding_rate_history(
                    pair=pair,
                    timeframe=timeframe,
                    limit=candle_limit,
                    since_ms=since_ms,
                )

            try:
                if data and data[0][0] > data[-1][0]:
                    data = sorted(data, key=lambda x: x[0])
            except IndexError:
                logger.exception("Error loading %s. Result was %s.", pair, data)
                return pair, timeframe, candle_type, [], self._ohlcv_partial_candle

            return (
                pair,
                timeframe,
                candle_type,
                data,
                self._ohlcv_partial_candle if candle_type != CandleType.FUNDING_RATE else False,
            )

        except ccxt.NotSupported as e:
            raise OperationalException(
                f"Exchange {self._api.name} does not support fetching historical "
                f"candle (OHLCV) data. Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            err = str(e)
            if self._BINGX_PAIR_SUSPENDED in err:
                self.__class__._suspended_pairs.add(pair)
                logger.warning(
                    "%s suspended on BingX (109415) during OHLCV fetch. "
                    "Added to suspended cache - no further retries.",
                    pair,
                )
                raise OperationalException(
                    f"{pair} suspended on BingX (109415) - OHLCV fetch cancelled"
                ) from e
            if self._BINGX_RATE_LIMITED in err:
                raise DDosProtection(
                    f"BingX rate limit during OHLCV fetch (109429): {e}"
                ) from e
            raise TemporaryError(
                f"Could not fetch OHLCV data for {pair}, {timeframe}, {candle_type} "
                f"({e.__class__.__name__}): {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(
                f"Could not fetch OHLCV data for {pair}, {timeframe}, {candle_type}: {e}"
            ) from e

    # ── Liquidation price (dry-run) ───────────────────────────────────────────

    def dry_run_liquidation_price(
        self,
        pair: str,
        open_rate: float,
        is_short: bool,
        amount: float,
        stake_amount: float,
        leverage: float,
        wallet_balance: float,
        open_trades: list,
    ) -> float | None:
        """
        Estimated BingX liquidation price for USDⓢ-M perpetual futures.

        Isolated:
            long  liq = entry - (initial_margin - maintenance_margin) / size
            short liq = entry + (initial_margin - maintenance_margin) / size

        Cross: solves the account risk equation across all cross positions.
        """
        market = self.markets[pair]
        mm_ratio, maintenance_amt = self.get_maintenance_ratio_and_amt(pair, stake_amount)
        maintenance_amt = maintenance_amt or 0.0

        if self.trading_mode != TradingMode.FUTURES:
            raise OperationalException(
                "Freqtrade only supports futures mode for leverage trading")
        if market["inverse"]:
            raise OperationalException(
                "Freqtrade does not support inverse contracts")

        if self.margin_mode == MarginMode.ISOLATED:
            position_value = amount * open_rate
            initial_margin = position_value / leverage
            maintenance_margin = position_value * mm_ratio - maintenance_amt
            diff = (initial_margin - maintenance_margin) / amount
            return open_rate + diff if is_short else open_rate - diff

        if self.margin_mode == MarginMode.CROSS:
            cross_vars = 0.0
            if open_trades:
                pairs = [t.pair for t in open_trades]
                if self._config["runmode"] in ("live", "dry_run"):
                    funding_rates = self.fetch_funding_rates(pairs)
                for trade in open_trades:
                    if trade.pair == pair:
                        continue
                    mark_price = (
                        funding_rates[trade.pair]["markPrice"]
                        if self._config["runmode"] in ("live", "dry_run")
                        else trade.open_rate
                    )
                    t_mm_ratio, t_maint_amt = self.get_maintenance_ratio_and_amt(
                        trade.pair, trade.stake_amount)
                    t_maint = trade.amount * mark_price * t_mm_ratio - (t_maint_amt or 0.0)
                    cross_vars += (
                        trade.amount * mark_price - trade.amount * trade.open_rate
                    ) - t_maint

            side_1 = -1 if is_short else 1
            return (
                (wallet_balance + cross_vars + maintenance_amt)
                - (side_1 * amount * open_rate)
            ) / ((amount * mm_ratio) - (side_1 * amount))

        raise OperationalException(
            "Freqtrade only supports cross or isolated margin for BingX")
