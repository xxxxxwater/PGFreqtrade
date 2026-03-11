"""Coinbase Advanced Trade exchange subclass.

Test-branch implementation with extended Coinbase Advanced support for:
- Spot mode
- Futures mode scaffolding aligned with Coinbase Advanced Trade docs
- CCXT compatibility shaping for futures-style symbols / positions / params
"""

import logging
from datetime import datetime
from typing import Any

from freqtrade.constants import BuySell
from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exceptions import ExchangeError, OperationalException
from freqtrade.exchange import Exchange
from freqtrade.exchange.coinbase_advanced_compat import (
    build_coinbase_symbol_candidates,
    infer_coinbase_maintenance_ratio,
    infer_coinbase_max_leverage,
    is_coinbase_futures_market,
    normalize_coinbase_balances,
    normalize_coinbase_order_params,
    normalize_coinbase_positions,
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

    def additional_exchange_init(self) -> None:
        if not self._api:
            return
        market_count = len(self.markets or {})
        spot_count = len([m for m in (self.markets or {}).values() if m.get("spot")])
        futures_count = len([m for m in (self.markets or {}).values() if is_coinbase_futures_market(m, self._config.get("stake_currency"))])
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
        if pair in self.markets:
            return pair
        if self.trading_mode == TradingMode.SPOT:
            return pair
        for candidate in build_coinbase_symbol_candidates(pair, self._config.get("stake_currency")):
            if candidate in self.markets:
                return candidate
        return pair

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
        mm_ratio, _ = self.get_maintenance_ratio_and_amt(pair, stake_amount)
        position_value = amount * open_rate
        initial_margin = position_value / leverage
        maintenance_margin = position_value * mm_ratio
        liq_delta = (initial_margin - maintenance_margin) / amount
        return open_rate + liq_delta if is_short else open_rate - liq_delta

    def get_funding_fees(self, pair: str, amount: float, is_short: bool, open_date: datetime) -> float:
        if self.trading_mode != TradingMode.FUTURES:
            return 0.0
        pair = self.normalize_pair_for_trading(pair)
        try:
            return self._fetch_and_calculate_funding_fees(pair, amount, is_short, open_date)
        except Exception as exc:
            logger.warning("Could not update funding fees for %s on Coinbase: %s", pair, exc)
            return 0.0
