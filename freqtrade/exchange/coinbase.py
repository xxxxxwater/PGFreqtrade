"""Coinbase Advanced Trade exchange subclass.

Test-branch implementation with extended Coinbase Advanced support for:
- Spot mode (existing behaviour)
- Futures mode scaffolding aligned with Coinbase Advanced Trade docs
- Safer parameter normalization for CCXT unified methods
- Symbol normalization helpers for Coinbase Advanced derivatives style markets

This file intentionally keeps the implementation conservative: it enables
Freqtrade's futures plumbing in the test tree without claiming full production
feature parity with more mature futures adapters like Bybit / Binance.
"""

import logging
from copy import deepcopy
from datetime import datetime
from typing import Any

from freqtrade.constants import BuySell
from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exceptions import OperationalException
from freqtrade.exchange import Exchange
from freqtrade.exchange.exchange_types import CcxtBalances, CcxtPosition, FtHas
from freqtrade.misc import deep_merge_dicts


logger = logging.getLogger(__name__)


class Coinbase(Exchange):
    """Coinbase Advanced Trade exchange class.

    Notes for this test branch:
    - Spot mode remains the default / stable mode.
    - Futures support is enabled as an experimental implementation layer so the
      rest of Freqtrade can operate in FUTURES mode on Coinbase Advanced where
      CCXT exposes compatible unified endpoints.
    - The implementation is intentionally defensive and tries to degrade
      gracefully when certain CCXT features are missing.
    """

    _ft_has: FtHas = {
        "ohlcv_has_history": True,
        "order_time_in_force": ["GTC", "IOC", "FOK", "PO"],
        "trades_has_history": True,
        "tickers_have_bid_ask": True,
        "tickers_have_percentage": True,
        "tickers_have_quoteVolume": True,
        "marketOrderRequiresPrice": False,
        "ws_enabled": False,
    }

    _ft_has_futures: FtHas = {
        "ohlcv_has_history": True,
        "funding_fee_candle_limit": 200,
        "stoploss_on_exchange": False,
        "stoploss_order_types": {},
        "stoploss_blocks_assets": False,
        "order_time_in_force": ["GTC", "IOC", "FOK", "PO"],
        "uses_leverage_tiers": False,
        "mark_ohlcv_price": "mark",
        "ohlcv_partial_candle": True,
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
        # Coinbase Advanced derivatives support is still exchange/account dependent.
        # For the test branch we enable isolated futures first.
        (TradingMode.FUTURES, MarginMode.ISOLATED),
    ]

    @property
    def _ccxt_config(self) -> dict:
        """Return CCXT config with Coinbase-specific defaultType routing."""
        config: dict[str, Any] = {"options": {}}

        if self.trading_mode == TradingMode.SPOT:
            config["options"].update({"defaultType": "spot"})
        elif self.trading_mode == TradingMode.FUTURES:
            # Coinbase Advanced derivatives in CCXT are generally exposed through
            # unified derivatives/swap style market typing.
            config["options"].update(
                {
                    "defaultType": "swap",
                    "defaultSubType": "linear",
                }
            )

        return deep_merge_dicts(config, super()._ccxt_config)

    def additional_exchange_init(self) -> None:
        """Log useful Coinbase Advanced capability information for debugging."""
        if not self._api:
            return

        market_count = len(self.markets or {})
        spot_count = len([m for m in (self.markets or {}).values() if m.get("spot")])
        futures_count = len(
            [
                m
                for m in (self.markets or {}).values()
                if m.get("swap") or m.get("future") or m.get("contract")
            ]
        )
        logger.info(
            "Coinbase Advanced init complete. markets=%s spot=%s futures=%s trading_mode=%s",
            market_count,
            spot_count,
            futures_count,
            self.trading_mode.value,
        )

    def normalize_pair_for_trading(self, pair: str) -> str:
        """Normalize incoming pair names to the closest Coinbase Advanced symbol.

        Examples:
        - Spot: BTC/USDC -> BTC/USDC
        - Futures target style: BTC/USDC -> BTC/USDC:USDC (if such market exists)
        - Already normalized: BTC/USDC:USDC -> BTC/USDC:USDC
        """
        if pair in self.markets:
            return pair

        if self.trading_mode == TradingMode.SPOT:
            return pair

        candidates = [pair]
        if ":" not in pair and "/" in pair:
            _, quote = pair.split("/")
            candidates.append(f"{pair}:{quote}")
            candidates.append(f"{pair}:{self._config.get('stake_currency', quote)}")

        for candidate in candidates:
            if candidate in self.markets:
                return candidate
        return pair

    def market_is_tradable(self, market: dict[str, Any]) -> bool:
        """Keep parent logic and filter obviously unsupported inverse contracts."""
        parent = super().market_is_tradable(market)
        if not parent:
            return False

        if self.trading_mode == TradingMode.FUTURES:
            if market.get("inverse"):
                return False
            if not (market.get("swap") or market.get("future") or market.get("contract")):
                return False
            settle = market.get("settle") or market.get("quote")
            stake = self._config.get("stake_currency")
            if settle and stake and settle != stake:
                return False

        return True

    def get_valid_pair_combination(self, curr_1: str, curr_2: str) -> str:
        """Return valid spot/futures symbol combinations for Coinbase Advanced.

        This makes futures pair handling more explicit in the test tree.
        """
        pair = super().get_valid_pair_combination(curr_1, curr_2)
        return self.normalize_pair_for_trading(pair)

    def get_balances(self, params: dict | None = None) -> CcxtBalances:
        """Fetch balances and normalize Coinbase Advanced style payloads."""
        balances = super().get_balances(params=params)
        if not balances:
            return balances

        normalized: CcxtBalances = {}
        for currency, value in balances.items():
            if currency in {"info", "free", "used", "total", "timestamp", "datetime"}:
                continue
            if not isinstance(value, dict):
                continue
            normalized[currency] = {
                "free": float(value.get("free") or 0.0),
                "used": float(value.get("used") or 0.0),
                "total": float(value.get("total") or 0.0),
            }

        return normalized or balances

    def fetch_positions(
        self, pair: str | None = None, params: dict | None = None
    ) -> list[CcxtPosition]:
        """Fetch and normalize futures positions."""
        pair = self.normalize_pair_for_trading(pair) if pair else pair
        positions = super().fetch_positions(pair, params=params)
        if self.trading_mode != TradingMode.FUTURES:
            return positions

        normalized: list[CcxtPosition] = []
        for pos in positions:
            p = deepcopy(pos)
            contracts = p.get("contracts")
            if contracts is None:
                contracts = p.get("contractSize") or p.get("info", {}).get("number_of_contracts")
            if contracts is None and p.get("amount") is not None:
                contracts = p.get("amount")
            p["contracts"] = float(contracts or 0.0)

            leverage = p.get("leverage")
            if leverage in (None, ""):
                leverage = p.get("info", {}).get("leverage") or 1.0
            try:
                p["leverage"] = float(leverage)
            except Exception:
                p["leverage"] = 1.0

            margin_mode = p.get("marginMode") or p.get("info", {}).get("margin_mode")
            p["marginMode"] = margin_mode or self.margin_mode.value

            side = p.get("side") or p.get("info", {}).get("side")
            if side:
                p["side"] = str(side).lower()
            normalized.append(p)
        return normalized

    def _get_params(
        self,
        side: BuySell,
        ordertype: str,
        leverage: float,
        reduceOnly: bool,
        time_in_force: str = "GTC",
    ) -> dict:
        """Build Coinbase Advanced compatible order params."""
        params = super()._get_params(
            side=side,
            ordertype=ordertype,
            leverage=leverage,
            reduceOnly=reduceOnly,
            time_in_force=time_in_force,
        )

        if time_in_force == "PO":
            params.pop("timeInForce", None)
            params["postOnly"] = True

        if self.trading_mode == TradingMode.FUTURES:
            params["reduceOnly"] = reduceOnly
            params["marginMode"] = self.margin_mode.value
            if leverage and leverage > 1.0:
                params["leverage"] = leverage
        return params

    def _lev_prep(self, pair: str, leverage: float, side: BuySell, accept_fail: bool = False):
        """Prepare leverage / margin settings before order placement."""
        if self.trading_mode == TradingMode.SPOT:
            return

        pair = self.normalize_pair_for_trading(pair)
        try:
            self.set_margin_mode(pair, self.margin_mode, accept_fail=True)
        except Exception as exc:
            if not accept_fail:
                logger.info("Coinbase set_margin_mode skipped for %s: %s", pair, exc)

        try:
            self._set_leverage(leverage, pair, accept_fail=True)
        except Exception as exc:
            if not accept_fail:
                logger.info("Coinbase set_leverage skipped for %s: %s", pair, exc)

    def get_max_leverage(self, pair: str, stake_amount: float | None) -> float:
        """Return a reasonable max leverage from market metadata."""
        if self.trading_mode == TradingMode.SPOT:
            return 1.0

        pair = self.normalize_pair_for_trading(pair)
        market = self.markets.get(pair, {})
        limits = market.get("limits", {}) if isinstance(market, dict) else {}
        lev = (limits.get("leverage") or {}).get("max")
        if lev:
            return float(lev)

        info = market.get("info", {}) if isinstance(market, dict) else {}
        for key in ("max_leverage", "maxLeverage", "intraday_margin_rate"):
            val = info.get(key)
            if val not in (None, ""):
                try:
                    if key == "intraday_margin_rate":
                        rate = float(val)
                        return round(1.0 / rate, 8) if rate > 0 else 1.0
                    return float(val)
                except Exception:
                    continue
        return 3.0

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
        """Approximate liquidation price for isolated linear futures."""
        if self.trading_mode != TradingMode.FUTURES:
            raise OperationalException(
                "Freqtrade only supports liquidation price calculation in futures mode"
            )
        if self.margin_mode != MarginMode.ISOLATED:
            raise OperationalException(
                "Test Coinbase futures support currently expects isolated margin mode"
            )

        pair = self.normalize_pair_for_trading(pair)
        market = self.markets[pair]
        if market.get("inverse"):
            raise OperationalException("Inverse Coinbase contracts are not supported")

        mm_ratio, _ = self.get_maintenance_ratio_and_amt(pair, stake_amount)
        position_value = amount * open_rate
        initial_margin = position_value / leverage
        maintenance_margin = position_value * mm_ratio
        liq_delta = (initial_margin - maintenance_margin) / amount
        return open_rate + liq_delta if is_short else open_rate - liq_delta

    def get_funding_fees(
        self, pair: str, amount: float, is_short: bool, open_date: datetime
    ) -> float:
        """Funding fee wrapper."""
        if self.trading_mode != TradingMode.FUTURES:
            return 0.0
        pair = self.normalize_pair_for_trading(pair)
        try:
            return self._fetch_and_calculate_funding_fees(pair, amount, is_short, open_date)
        except Exception as exc:
            logger.warning("Could not update funding fees for %s on Coinbase: %s", pair, exc)
            return 0.0
