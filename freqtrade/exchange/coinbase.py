"""Coinbase Advanced Trade exchange subclass.

Test-branch implementation with extended Coinbase Advanced support for:
- Spot mode
- Futures mode scaffolding aligned with Coinbase Advanced Trade docs
- CCXT compatibility shaping for futures-style symbols / positions / params
"""

import logging
from datetime import datetime
from typing import Any

import ccxt

from freqtrade.constants import BuySell
from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exceptions import (
    DDosProtection,
    ExchangeError,
    InsufficientFundsError,
    InvalidOrderException,
    OperationalException,
    TemporaryError,
)
from freqtrade.exchange import Exchange
from freqtrade.exchange.coinbase_advanced_compat import (
    build_coinbase_close_position_params,
    build_coinbase_symbol_candidates,
    infer_coinbase_maintenance_ratio,
    infer_coinbase_max_leverage,
    is_coinbase_futures_market,
    normalize_coinbase_balances,
    normalize_coinbase_order_params,
    normalize_coinbase_positions,
    resolve_coinbase_portfolio,
    should_use_coinbase_close_position_fallback,
)
from freqtrade.exchange.exchange_types import CcxtBalances, CcxtPosition, FtHas
from freqtrade.misc import deep_merge_dicts

logger = logging.getLogger(__name__)


class Coinbase(Exchange):
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
        "ccxt_futures_name": "swap",
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
    ]

    @property
    def _ccxt_config(self) -> dict:
        config: dict[str, Any] = {"options": {}}
        if self.trading_mode == TradingMode.SPOT:
            config["options"].update({"defaultType": "spot"})
        elif self.trading_mode == TradingMode.FUTURES:
            config["options"].update({"defaultType": "swap", "defaultSubType": "linear"})
        return deep_merge_dicts(config, super()._ccxt_config)

    def _require_futures_portfolio(self, params: dict[str, Any] | None = None) -> str:
        if self.trading_mode != TradingMode.FUTURES:
            return ""
        merged_params = dict(params or {})
        portfolio = merged_params.get("portfolio") or resolve_coinbase_portfolio(self._api, self._config)
        if not portfolio:
            raise OperationalException(
                "Coinbase Advanced futures requires an explicit portfolio. "
                "Set exchange.portfolio or exchange.ccxt_config.options.portfolio before live trading."
            )
        self._api.options["portfolio"] = portfolio
        return str(portfolio)

    def additional_exchange_init(self) -> None:
        if not self._api:
            return
        if self.trading_mode == TradingMode.FUTURES:
            portfolio = self._require_futures_portfolio()
            logger.info("Coinbase Advanced futures portfolio configured: %s", portfolio)
        market_count = len(self.markets or {})
        spot_count = len([m for m in (self.markets or {}).values() if m.get("spot")])
        futures_count = len(
            [m for m in (self.markets or {}).values() if is_coinbase_futures_market(m, self._config.get("stake_currency"))]
        )
        logger.info(
            "Coinbase Advanced init complete. markets=%s spot=%s futures=%s trading_mode=%s",
            market_count,
            spot_count,
            futures_count,
            self.trading_mode.value,
        )

    def normalize_pair_for_trading(self, pair: str) -> str:
        if not pair:
            return pair
        normalized_pair = str(pair).strip().upper()
        if normalized_pair in self.markets:
            return normalized_pair
        if self.trading_mode == TradingMode.SPOT:
            return normalized_pair
        for candidate in build_coinbase_symbol_candidates(normalized_pair, self._config.get("stake_currency")):
            if candidate in self.markets:
                return candidate
        return normalized_pair

    def market_is_tradable(self, market: dict[str, Any]) -> bool:
        parent = super().market_is_tradable(market)
        if not parent:
            return False
        if self.trading_mode == TradingMode.FUTURES:
            return is_coinbase_futures_market(market, self._config.get("stake_currency"))
        return True

    def get_valid_pair_combination(self, curr_1: str, curr_2: str) -> str:
        pair = super().get_valid_pair_combination(curr_1, curr_2)
        return self.normalize_pair_for_trading(pair)

    def get_balances(self, params: dict | None = None) -> CcxtBalances:
        balances = super().get_balances(params=params)
        if not balances:
            return balances
        return normalize_coinbase_balances(balances) or balances

    def fetch_positions(self, pair: str | None = None, params: dict | None = None) -> list[CcxtPosition]:
        pair = self.normalize_pair_for_trading(pair) if pair else pair
        if self.trading_mode == TradingMode.FUTURES:
            merged_params = dict(params or {})
            merged_params["portfolio"] = self._require_futures_portfolio(merged_params)
            params = merged_params
        positions = super().fetch_positions(pair, params=params)
        if self.trading_mode != TradingMode.FUTURES:
            return positions
        return normalize_coinbase_positions(positions, self.margin_mode.value)

    def _get_params(
        self,
        side: BuySell,
        ordertype: str,
        leverage: float,
        reduceOnly: bool,
        time_in_force: str = "GTC",
    ) -> dict:
        params = super()._get_params(
            side=side,
            ordertype=ordertype,
            leverage=leverage,
            reduceOnly=reduceOnly,
            time_in_force=time_in_force,
        )
        return normalize_coinbase_order_params(
            trading_mode=self.trading_mode.value,
            margin_mode=self.margin_mode.value,
            time_in_force=time_in_force,
            leverage=leverage,
            reduce_only=reduceOnly,
            params=params,
        )

    def _lev_prep(self, pair: str, leverage: float, side: BuySell, accept_fail: bool = False):
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
        if self.trading_mode == TradingMode.SPOT:
            return 1.0
        pair = self.normalize_pair_for_trading(pair)
        market = self.markets.get(pair, {})
        return infer_coinbase_max_leverage(market, default=3.0)

    def get_maintenance_ratio_and_amt(self, pair: str, notional_value: float) -> tuple[float, float | None]:
        try:
            return super().get_maintenance_ratio_and_amt(pair, notional_value)
        except ExchangeError:
            pair = self.normalize_pair_for_trading(pair)
            market = self.markets.get(pair, {})
            return (infer_coinbase_maintenance_ratio(market, default=0.02), None)

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
        if self.trading_mode != TradingMode.FUTURES:
            raise OperationalException("Freqtrade only supports liquidation price calculation in futures mode")
        if self.margin_mode != MarginMode.ISOLATED:
            raise OperationalException("Test Coinbase futures support currently expects isolated margin mode")
        pair = self.normalize_pair_for_trading(pair)
        market = self.markets[pair]
        if market.get("inverse"):
            raise OperationalException("Inverse Coinbase contracts are not supported")

        position_value = amount * open_rate
        mm_ratio, _ = self.get_maintenance_ratio_and_amt(pair, position_value)
        initial_margin = position_value / leverage
        maintenance_margin = position_value * mm_ratio
        liq_delta = (initial_margin - maintenance_margin) / amount
        return open_rate + liq_delta if is_short else open_rate - liq_delta

    def _finalize_coinbase_order_response(self, order: dict[str, Any], ordertype: str, log_name: str):
        if order.get("status") is None:
            order["status"] = "open"
        if order.get("type") is None:
            order["type"] = ordertype
        self._log_exchange_response(log_name, order)
        return self._order_contracts_to_amount(order)

    def _create_coinbase_close_position_order(
        self,
        *,
        pair: str,
        ordertype: str,
        side: BuySell,
        amount: float,
        rate_for_order: float | None,
        leverage: float,
        time_in_force: str,
    ):
        close_params = build_coinbase_close_position_params(side=side)
        close_params.update(self._get_params(side, ordertype, leverage, False, time_in_force))
        close_params["portfolio"] = self._require_futures_portfolio(close_params)
        order = self._api.create_order(pair, ordertype, side, amount, rate_for_order, close_params)
        return self._finalize_coinbase_order_response(order, ordertype, "create_order_close_position_fallback")

    def create_order(
        self,
        *,
        pair: str,
        ordertype: str,
        side: BuySell,
        amount: float,
        rate: float,
        leverage: float,
        time_in_force: str = "GTC",
        reduceOnly: bool = False,
        initial_order: bool = True,
    ):
        """Create order with Coinbase Advanced futures fallback behavior.

        For some Coinbase Advanced venues/products, reduce-only exits may not be allowed via
        the ordinary create-order path. In that case, retry once with explicit
        `close_position` semantics.
        """
        if self._config["dry_run"]:
            return super().create_order(
                pair=pair,
                ordertype=ordertype,
                side=side,
                amount=amount,
                rate=rate,
                leverage=leverage,
                time_in_force=time_in_force,
                reduceOnly=reduceOnly,
                initial_order=initial_order,
            )

        pair = self.normalize_pair_for_trading(pair)
        params = self._get_params(side, ordertype, leverage, reduceOnly, time_in_force)
        if self.trading_mode == TradingMode.FUTURES:
            params["portfolio"] = self._require_futures_portfolio(params)

        amount = self.amount_to_precision(pair, self._amount_to_contracts(pair, amount))
        needs_price = self._order_needs_price(side, ordertype)
        rate_for_order = self.price_to_precision(pair, rate) if needs_price else None

        try:
            if not reduceOnly:
                self._lev_prep(pair, leverage, side, accept_fail=not initial_order)
            order = self._api.create_order(pair, ordertype, side, amount, rate_for_order, params)
            return self._finalize_coinbase_order_response(order, ordertype, "create_order")
        except ccxt.InsufficientFunds as e:
            raise InsufficientFundsError(
                f"Insufficient funds to create {ordertype} {side} order on market {pair}. Tried to {side} amount {amount} at rate {rate}. Message: {e}"
            ) from e
        except ccxt.InvalidOrder as e:
            if self.trading_mode == TradingMode.FUTURES and reduceOnly and should_use_coinbase_close_position_fallback(e):
                logger.warning(
                    "Coinbase rejected reduceOnly exit for %s (%s). Retrying with close_position semantics.",
                    pair,
                    e,
                )
                try:
                    return self._create_coinbase_close_position_order(
                        pair=pair,
                        ordertype=ordertype,
                        side=side,
                        amount=amount,
                        rate_for_order=rate_for_order,
                        leverage=leverage,
                        time_in_force=time_in_force,
                    )
                except ccxt.BaseError as fallback_exc:
                    raise InvalidOrderException(
                        f"Coinbase reduceOnly exit fallback failed for market {pair}. Initial error: {e}. Fallback error: {fallback_exc}"
                    ) from fallback_exc
            raise InvalidOrderException(
                f"Could not create {ordertype} {side} order on market {pair}. Tried to {side} amount {amount} at rate {rate}. Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            if self.trading_mode == TradingMode.FUTURES and reduceOnly and should_use_coinbase_close_position_fallback(e):
                logger.warning(
                    "Coinbase temporary reduceOnly exit failure for %s (%s). Retrying with close_position semantics.",
                    pair,
                    e,
                )
                try:
                    return self._create_coinbase_close_position_order(
                        pair=pair,
                        ordertype=ordertype,
                        side=side,
                        amount=amount,
                        rate_for_order=rate_for_order,
                        leverage=leverage,
                        time_in_force=time_in_force,
                    )
                except ccxt.BaseError as fallback_exc:
                    raise TemporaryError(
                        f"Coinbase close_position fallback failed for {pair}. Initial error: {e}. Fallback error: {fallback_exc}"
                    ) from fallback_exc
            raise TemporaryError(
                f"Could not place {side} order due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def get_funding_fees(self, pair: str, amount: float, is_short: bool, open_date: datetime) -> float:
        if self.trading_mode != TradingMode.FUTURES:
            return 0.0
        pair = self.normalize_pair_for_trading(pair)
        try:
            return self._fetch_and_calculate_funding_fees(pair, amount, is_short, open_date)
        except Exception as exc:
            logger.warning("Could not update funding fees for %s on Coinbase: %s", pair, exc)
            return 0.0
