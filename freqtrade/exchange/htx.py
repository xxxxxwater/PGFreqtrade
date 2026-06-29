"""HTX exchange subclass"""

import logging
from datetime import datetime
from math import floor
from typing import Any

import ccxt

from freqtrade.constants import BuySell
from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exceptions import (
    DDosProtection,
    OperationalException,
    RetryableOrderError,
    TemporaryError,
)
from freqtrade.exchange import Exchange
from freqtrade.exchange.common import retrier
from freqtrade.exchange.exchange_types import CcxtBalances, CcxtOrder, CcxtPosition, FtHas
from freqtrade.misc import deep_merge_dicts
from freqtrade.util import dt_ts


logger = logging.getLogger(__name__)


class Htx(Exchange):
    """HTX exchange class.
    Contains adjustments needed for Freqtrade to work with this exchange.
    """

    _ft_has: FtHas = {
        "stoploss_on_exchange": True,
        "stop_price_param": "stopPrice",
        "stop_price_prop": "stopPrice",
        "stoploss_order_types": {"limit": "stop-limit"},
        "l2_limit_range": [5, 10, 20],
        "l2_limit_range_required": False,
        "ohlcv_candle_limit_per_timeframe": {
            "1w": 500,
            "1M": 500,
        },
        "trades_has_history": False,  # Endpoint doesn't have a "since" parameter
    }
    _ft_has_futures: FtHas = {
        "floor_leverage": True,
        "marketOrderRequiresPrice": False,
        "stoploss_on_exchange": True,
        "stop_price_param": "stopLossPrice",
        "stop_price_prop": "triggerPrice",
        "stoploss_order_types": {"limit": "limit", "market": "market"},
        "stoploss_blocks_assets": False,
        "funding_fee_candle_limit": 200,
        "fetch_orders_limit_minutes": 7 * 1440,
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
    ]

    def __init__(self, *args, **kwargs) -> None:
        self._position_mode_pairs: set[str] = set()
        super().__init__(*args, **kwargs)

    @property
    def _ccxt_config(self) -> dict:
        config = super()._ccxt_config
        if self.trading_mode == TradingMode.FUTURES:
            config = deep_merge_dicts(
                {
                    "options": {
                        "defaultSubType": "linear",
                        "defaultSettle": self._config["stake_currency"],
                    }
                },
                config,
            )
        return config

    def _margin_mode_params(self, params: dict | None = None) -> dict:
        params = (params or {}).copy()
        if self.trading_mode != TradingMode.FUTURES or not self.margin_mode:
            return params
        params.setdefault("marginMode", self.margin_mode.value.lower())
        return params

    def _set_position_mode(self, pair: str, accept_fail: bool = False) -> None:
        if (
            self._config["dry_run"]
            or self.trading_mode != TradingMode.FUTURES
            or not self.exchange_has("setPositionMode")
            or pair in self._position_mode_pairs
        ):
            return
        try:
            res = self._api.set_position_mode(
                False, symbol=pair, params=self._margin_mode_params()
            )
            self._log_exchange_response("set_position_mode", res)
            self._position_mode_pairs.add(pair)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.BadRequest, ccxt.OperationRejected, ccxt.InsufficientFunds) as e:
            if not accept_fail:
                raise TemporaryError(
                    f"Could not set position mode due to {e.__class__.__name__}. Message: {e}"
                ) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not set position mode due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def additional_exchange_init(self) -> None:
        if self.trading_mode != TradingMode.FUTURES or self._config["dry_run"]:
            return

        for pair in self._config.get("exchange", {}).get("pair_whitelist", []):
            market = self.markets.get(pair)
            if market and self.market_is_future(market):
                self._set_position_mode(pair)

    def _lev_prep(self, pair: str, leverage: float, side: BuySell, accept_fail: bool = False):
        if self.trading_mode != TradingMode.SPOT:
            self._set_position_mode(pair, accept_fail)
        super()._lev_prep(pair, leverage, side, accept_fail)

    def set_margin_mode(
        self,
        pair: str,
        margin_mode: MarginMode,
        accept_fail: bool = False,
        params: dict | None = None,
    ):
        return super().set_margin_mode(
            pair, margin_mode, accept_fail, self._margin_mode_params(params)
        )

    @retrier
    def _set_leverage(
        self,
        leverage: float,
        pair: str | None = None,
        accept_fail: bool = False,
    ):
        if self._config["dry_run"] or not self.exchange_has("setLeverage"):
            return
        leverage = floor(leverage)
        try:
            res = self._api.set_leverage(
                symbol=pair, leverage=leverage, params=self._margin_mode_params()
            )
            self._log_exchange_response("set_leverage", res)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.BadRequest, ccxt.OperationRejected, ccxt.InsufficientFunds) as e:
            if not accept_fail:
                raise TemporaryError(
                    f"Could not set leverage due to {e.__class__.__name__}. Message: {e}"
                ) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not set leverage due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

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
        return self._margin_mode_params(params)

    def get_balances(self, params: dict | None = None) -> CcxtBalances:
        return super().get_balances(self._margin_mode_params(params))

    def fetch_order(self, order_id: str, pair: str, params: dict | None = None) -> CcxtOrder:
        return super().fetch_order(order_id, pair, self._margin_mode_params(params))

    def cancel_order(self, order_id: str, pair: str, params: dict | None = None) -> dict[str, Any]:
        return super().cancel_order(order_id, pair, self._margin_mode_params(params))

    def fetch_positions(
        self, pair: str | None = None, params: dict | None = None
    ) -> list[CcxtPosition]:
        return super().fetch_positions(pair, self._margin_mode_params(params))

    def fetch_orders(
        self, pair: str, since: datetime, params: dict | None = None
    ) -> list[CcxtOrder]:
        return super().fetch_orders(pair, since, self._margin_mode_params(params))

    def _fetch_orders(
        self, pair: str, since: datetime, params: dict | None = None
    ) -> list[CcxtOrder]:
        return super()._fetch_orders(pair, since, self._margin_mode_params(params))

    def get_trades_for_order(
        self, order_id: str, pair: str, since: datetime, params: dict | None = None
    ) -> list:
        return super().get_trades_for_order(order_id, pair, since, self._margin_mode_params(params))

    @retrier
    def _get_funding_fees_from_exchange(self, pair: str, since: datetime | int) -> float:
        if not self.exchange_has("fetchFundingHistory"):
            raise OperationalException(
                f"fetch_funding_history() is not available using {self.name}"
            )

        if type(since) is datetime:
            since = dt_ts(since)

        try:
            funding_history = self._api.fetch_funding_history(
                symbol=pair, since=since, params=self._margin_mode_params()
            )
            self._log_exchange_response(
                "funding_history", funding_history, add_info=f"pair: {pair}, since: {since}"
            )
            return sum(fee["amount"] for fee in funding_history)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get funding fees due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def _stoploss_query_params(self, params: dict | None = None) -> dict:
        params = self._margin_mode_params(params)
        if self.trading_mode == TradingMode.FUTURES:
            params["stopLossTakeProfit"] = True
        return params

    def _get_stop_params(self, side: BuySell, ordertype: str, stop_price: float) -> dict:
        if self.trading_mode == TradingMode.FUTURES:
            params = self._margin_mode_params()
            params[self._ft_has["stop_price_param"]] = stop_price
            return params

        params = self._params.copy()
        params.update(
            {
                "stopPrice": stop_price,
                "operator": "lte",
            }
        )
        return params

    def _convert_stop_order(self, pair: str, order_id: str, order: CcxtOrder) -> CcxtOrder:
        order_info = order.get("info", {})
        if order_info.get("tpsl_order_type"):
            relation_order_id = order_info.get("relation_order_id")
            if relation_order_id and relation_order_id != "-1":
                order_reg = self.fetch_order(str(relation_order_id), pair)
                self._log_exchange_response("fetch_stoploss_order1", order_reg)
                order_reg["id_stop"] = order_reg["id"]
                order_reg["id"] = order_id
                order_reg["type"] = "stoploss"
                order_reg["status_stop"] = "triggered"
                order_reg[self._ft_has["stop_price_prop"]] = order.get(
                    self._ft_has["stop_price_prop"]
                )
                return order_reg

            # HTX TP/SL status codes differ from the regular contract order status codes.
            tpsl_status = str(order_info.get("status"))
            if tpsl_status in {"1", "2", "3", "4"}:
                order["status"] = "open"
            elif tpsl_status in {"5", "6", "8", "10"}:
                order["status"] = "canceled"
            elif tpsl_status == "9":
                order["status"] = "canceling"
            elif tpsl_status in {"11", "12"}:
                order["status"] = "expired"

        order = self._order_contracts_to_amount(order)
        order["type"] = "stoploss"
        return order

    @retrier
    def fetch_stoploss_order(
        self, order_id: str, pair: str, params: dict | None = None
    ) -> CcxtOrder:
        if self._config["dry_run"]:
            return self.fetch_dry_run_order(order_id)

        if self.trading_mode != TradingMode.FUTURES:
            return super().fetch_stoploss_order(order_id, pair, params)

        params = self._stoploss_query_params(params)
        for method in (self._api.fetch_open_orders, self._api.fetch_orders):
            try:
                orders = method(pair, params=params)
                orders_f = [order for order in orders if order.get("id") == order_id]
                if orders_f:
                    order = self._convert_stop_order(pair, order_id, orders_f[0])
                    self._log_exchange_response("fetch_stoploss_order", order)
                    return order
            except (ccxt.OrderNotFound, ccxt.InvalidOrder):
                pass
            except ccxt.DDoSProtection as e:
                raise DDosProtection(e) from e
            except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
                raise TemporaryError(
                    f"Could not get stoploss order due to {e.__class__.__name__}. Message: {e}"
                ) from e
            except ccxt.BaseError as e:
                raise OperationalException(e) from e

        raise RetryableOrderError(f"StoplossOrder not found (pair: {pair} id: {order_id}).")

    def cancel_stoploss_order(self, order_id: str, pair: str, params: dict | None = None) -> dict:
        if self.trading_mode != TradingMode.FUTURES:
            return super().cancel_stoploss_order(order_id, pair, params)

        return self.cancel_order(order_id, pair, self._stoploss_query_params(params))
