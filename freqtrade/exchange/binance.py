"""Binance exchange subclass"""

import logging
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Any

import ccxt
from pandas import DataFrame

from freqtrade.constants import DEFAULT_DATAFRAME_COLUMNS, EntryExecuteMode
from freqtrade.enums import TRADE_MODES, CandleType, MarginMode, PriceType, RunMode, TradingMode
from freqtrade.exceptions import (
    DDosProtection,
    InsufficientFundsError,
    InvalidOrderException,
    OperationalException,
    TemporaryError,
)
from freqtrade.exchange import Exchange
from freqtrade.exchange.binance_pm_user_stream import BinancePMUserStream
from freqtrade.exchange.binance_public_data import (
    concat_safe,
    download_archive_ohlcv,
    download_archive_trades,
)
from freqtrade.exchange.common import retrier
from freqtrade.exchange.exchange_types import CcxtBalances, CcxtOrder, CcxtPosition, FtHas, Tickers
from freqtrade.exchange.exchange_utils_timeframe import timeframe_to_msecs
from freqtrade.misc import deep_merge_dicts, json_load
from freqtrade.util import FtTTLCache
from freqtrade.util.datetime_helpers import dt_from_ts, dt_ts


logger = logging.getLogger(__name__)


class Binance(Exchange):
    """Binance exchange class.
    Contains adjustments needed for Freqtrade to work with this exchange.
    """

    _ft_has: FtHas = {
        "stoploss_on_exchange": True,
        "stop_price_param": "stopPrice",
        "stop_price_prop": "stopPrice",
        "stoploss_order_types": {"limit": "stop_loss_limit"},
        "stoploss_blocks_assets": True,  # By default stoploss orders block assets
        "order_time_in_force": ["GTC", "FOK", "IOC", "PO"],
        "trades_pagination": "id",
        "trades_pagination_arg": "fromId",
        "trades_has_history": True,
        "fetch_orders_limit_minutes": None,
        "l2_limit_range": [5, 10, 20, 50, 100, 500, 1000],
        "ws_enabled": True,
        "has_delisting": True,
    }
    _ft_has_futures: FtHas = {
        "funding_fee_candle_limit": 1000,
        "stoploss_order_types": {"limit": "stop", "market": "stop_market"},
        "stoploss_blocks_assets": False,  # Stoploss orders do not block assets
        "stoploss_query_requires_stop_flag": True,
        "stoploss_algo_order_info_id": "actualOrderId",
        "tickers_have_price": False,
        "floor_leverage": True,
        "fetch_orders_limit_minutes": 7 * 1440,  # "fetch_orders" is limited to 7 days
        "stop_price_type_field": "workingType",
        "order_props_in_contracts": ["amount", "cost", "filled", "remaining"],
        "stop_price_type_value_mapping": {
            PriceType.LAST: "CONTRACT_PRICE",
            PriceType.MARK: "MARK_PRICE",
        },
        "ws_enabled": False,
        "proxy_coin_mapping": {
            "BNFCR": "USDC",
            "BFUSD": "USDT",
        },
    }
    _can_use_data_download_fast = True
    _pm_risk_allowed_config_keys = {
        "min_uni_mmr",
        "warning_uni_mmr",
        "emergency_stop_uni_mmr",
        "user_stream_enabled",
        "user_stream_health_interval_minutes",
        "user_stream_queue_warning_size",
        "user_stream_disconnected_restart_seconds",
        "user_stream_max_restarts_per_hour",
        "user_stream_recover_unmatched_orders",
        "monitor_interval_minutes",
        "order_recovery_interval_minutes",
        "heartbeat_risk_cache_seconds",
        "max_leverage",
        "max_total_notional",
        "max_position_notional",
        "max_daily_loss",
    }
    _pm_risk_positive_number_keys = {
        "min_uni_mmr",
        "warning_uni_mmr",
        "emergency_stop_uni_mmr",
        "max_leverage",
        "max_total_notional",
        "max_daily_loss",
    }
    _pm_risk_positive_integer_keys = {
        "user_stream_health_interval_minutes",
        "user_stream_queue_warning_size",
        "user_stream_disconnected_restart_seconds",
        "user_stream_max_restarts_per_hour",
        "monitor_interval_minutes",
        "order_recovery_interval_minutes",
        "heartbeat_risk_cache_seconds",
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
        # (TradingMode.MARGIN, MarginMode.CROSS),
        (TradingMode.FUTURES, MarginMode.CROSS),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
    ]

    def __init__(self, *args, **kwargs) -> None:
        config = args[0] if args else kwargs.get("config", {})
        exchange_conf = config.get("exchange", {}) if isinstance(config, dict) else {}
        self._portfolio_margin = bool(
            exchange_conf.get("portfolio_margin")
            or exchange_conf.get("binance_portfolio_margin")
            or str(exchange_conf.get("account_type", "")).lower() in {"pm", "portfolio_margin"}
        )
        self._pm_user_stream: BinancePMUserStream | None = None
        self._pm_user_stream_lock = RLock()
        super().__init__(*args, **kwargs)
        self._spot_delist_schedule_cache: FtTTLCache = FtTTLCache(maxsize=100, ttl=300)

    def _is_portfolio_margin(self) -> bool:
        return bool(getattr(self, "_portfolio_margin", False))

    def _skip_fetch_currencies_on_markets_reload(self) -> bool:
        return self._is_portfolio_margin()

    def _papi_request(
        self, path: str, method: str = "GET", params: dict[str, Any] | None = None
    ) -> Any:
        """
        Signed Binance Portfolio Margin request.

        ccxt exposes Binance PAPI raw endpoints inconsistently across releases.  Using request()
        keeps this adapter compatible with older ccxt builds while still reusing ccxt signing,
        throttling and error mapping.
        """
        return self._api.request(path, "papiPrivate", method, params or {})

    def _pm_namespace_for_pair(self, pair: str) -> str:
        return "um"

    def _pm_risk_config(self) -> dict[str, Any]:
        return self._config.get("exchange", {}).get("portfolio_margin_risk", {})

    def _pm_user_stream_enabled(self) -> bool:
        return bool(self._pm_risk_config().get("user_stream_enabled", True))

    def _pm_symbol_for_pair(self, pair: str) -> str:
        return self.markets[pair]["id"]

    @staticmethod
    def _float_or_none(value: Any) -> float | None:
        if value in (None, ""):
            return None
        return float(value)

    @classmethod
    def _float_or_zero(cls, value: Any) -> float:
        return cls._float_or_none(value) or 0.0

    @staticmethod
    def _pm_order_status(status: str | None) -> str | None:
        if status is None:
            return None
        return {
            "NEW": "open",
            "PARTIALLY_FILLED": "open",
            "FILLED": "closed",
            "CANCELED": "canceled",
            "CANCELLED": "canceled",
            "EXPIRED": "expired",
            "REJECTED": "rejected",
        }.get(status.upper(), status.lower())

    def _parse_pm_order(self, order: dict[str, Any], pair: str | None = None) -> CcxtOrder:
        symbol_id = order.get("symbol")
        symbol = pair or self._api.safe_symbol(symbol_id, None, None, "contract")
        amount = self._float_or_none(order.get("origQty"))
        filled = self._float_or_zero(order.get("executedQty"))
        price = self._float_or_none(order.get("price"))
        average = self._float_or_none(order.get("avgPrice"))
        cost = self._float_or_none(order.get("cumQuote"))
        remaining = max(amount - filled, 0.0) if amount is not None else None
        timestamp = self._float_or_none(order.get("updateTime") or order.get("time"))

        return {
            "id": str(order.get("orderId") or order.get("clientOrderId")),
            "clientOrderId": order.get("clientOrderId"),
            "timestamp": int(timestamp) if timestamp else None,
            "datetime": dt_from_ts(int(timestamp)) if timestamp else None,
            "lastTradeTimestamp": int(timestamp) if timestamp else None,
            "symbol": symbol,
            "type": str(order.get("type", "")).lower() or None,
            "timeInForce": order.get("timeInForce"),
            "side": str(order.get("side", "")).lower() or None,
            "price": price,
            "average": average,
            "amount": amount,
            "filled": filled,
            "remaining": remaining,
            "cost": cost,
            "status": self._pm_order_status(order.get("status")),
            "fee": None,
            "trades": [],
            "info": order,
        }

    def _parse_pm_position(self, position: dict[str, Any]) -> CcxtPosition | None:
        pair = self._api.safe_symbol(position.get("symbol"), None, None, "contract")
        if not pair or pair not in self.markets:
            return None

        position_amount = self._float_or_zero(position.get("positionAmt"))
        if position_amount == 0:
            side = None
        else:
            side = "long" if position_amount > 0 else "short"

        notional = self._float_or_zero(position.get("notional"))
        leverage = self._float_or_none(position.get("leverage")) or 1.0
        collateral = abs(notional) / leverage if notional and leverage else 0.0
        return {
            "symbol": pair,
            "side": side,
            "contracts": abs(position_amount),
            "leverage": self._float_or_none(position.get("leverage")) or 1.0,
            "collateral": collateral,
            "initialMargin": collateral,
            "liquidationPrice": self._float_or_none(position.get("liquidationPrice")),
            "info": position,
        }

    def _pm_order_params(
        self,
        pair: str,
        ordertype: str,
        side: str,
        amount: float,
        rate: float | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        request = {
            "symbol": self._pm_symbol_for_pair(pair),
            "side": side.upper(),
            "type": ordertype.upper(),
            "quantity": amount,
        }
        if rate is not None:
            request["price"] = rate
        request.update(params)
        return request

    def _pm_account_float(self, account: dict[str, Any], *keys: str) -> float | None:
        for key in keys:
            if key in account:
                value = self._float_or_none(account.get(key))
                if value is not None:
                    return value
        return None

    @retrier
    def fetch_pm_account_information(self) -> dict[str, Any]:
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return {}
        try:
            account = self._papi_request("account", "GET")
            self._log_exchange_response("papi_account", account)
            return account
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get Binance PM account due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def get_pm_risk_summary(self) -> dict[str, Any]:
        account = self.fetch_pm_account_information()
        if not account:
            return {"enabled": False}

        return {
            "enabled": True,
            "account_status": account.get("accountStatus"),
            "uni_mmr": self._pm_account_float(account, "uniMMR"),
            "account_equity": self._pm_account_float(
                account, "accountEquity", "actualEquity", "totalEquity"
            ),
            "total_collateral_value": self._pm_account_float(
                account, "totalCollateralValue", "accountMaintMargin"
            ),
            "initial_margin": self._pm_account_float(
                account, "accountInitialMargin", "totalInitialMargin"
            ),
            "maintenance_margin": self._pm_account_float(
                account, "accountMaintMargin", "totalMaintMargin"
            ),
            "raw": account,
        }

    def assert_pm_risk_allows_order(
        self,
        pair: str | None = None,
        amount: float = 0.0,
        leverage: float | None = None,
        entry_mode: EntryExecuteMode = "initial",
    ) -> None:
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return

        risk_config = self._pm_risk_config()
        risk = self.get_pm_risk_summary()
        account_status = risk.get("account_status")
        if account_status and account_status != "NORMAL":
            raise OperationalException(
                f"Binance PM account status is {account_status}; refusing to open a new order."
            )

        min_uni_mmr = risk_config.get("min_uni_mmr")
        uni_mmr = risk.get("uni_mmr")
        if min_uni_mmr is not None and uni_mmr is not None and uni_mmr < float(min_uni_mmr):
            raise OperationalException(
                f"Binance PM uniMMR {uni_mmr} is below configured minimum {min_uni_mmr}; "
                "refusing to open a new order."
            )

        max_leverage = risk_config.get("max_leverage")
        if max_leverage is not None and leverage is not None and leverage > float(max_leverage):
            raise OperationalException(
                f"Requested leverage {leverage} exceeds max_leverage {max_leverage}; "
                "refusing to open a new order."
            )

        positions: list[dict[str, Any]] | None = None

        max_total_notional = risk_config.get("max_total_notional")
        max_position_notional_match = risk_config.get("max_position_notional") or {}
        counts_new_notional = entry_mode != "replace" and amount > 0.0
        needs_positions = counts_new_notional and (
            (max_total_notional is not None)
            or (max_position_notional_match and pair in max_position_notional_match)
        )
        if needs_positions and pair is not None:
            positions = self.fetch_positions()

        if max_total_notional is not None and pair is not None and counts_new_notional:
            current_total = (
                sum(
                    self._float_or_zero(p.get("initialMargin", 0))
                    * self._float_or_zero(p.get("leverage", 1))
                    for p in positions or []
                )
                if positions is not None
                else 0.0
            )
            price = self._float_or_zero(self.get_rate(pair, side="entry", refresh=True))
            new_notional = amount * (price or 0.0)
            projected_total = current_total + new_notional

            if projected_total > float(max_total_notional):
                raise OperationalException(
                    f"Projected total notional {projected_total:.2f} exceeds "
                    f"max_total_notional {max_total_notional}; "
                    f"current={current_total:.2f}, new={new_notional:.2f}, "
                    f"entry_mode={entry_mode}. Refusing to open a new order."
                )

        if max_position_notional_match and pair is not None and counts_new_notional:
            pair_cap = max_position_notional_match.get(pair)
            if pair_cap is not None:
                current_pair_notional = (
                    sum(
                        self._float_or_zero(p.get("initialMargin", 0))
                        * self._float_or_zero(p.get("leverage", 1))
                        for p in (positions or [])
                        if p.get("symbol") == pair
                    )
                    if positions is not None
                    else 0.0
                )
                price = self._float_or_zero(self.get_rate(pair, side="entry", refresh=True))
                new_notional = amount * (price or 0.0)
                projected_pair = current_pair_notional + new_notional
                if projected_pair > float(pair_cap):
                    raise OperationalException(
                        f"Projected {pair} notional {projected_pair:.2f} exceeds "
                        f"per-pair max {pair_cap} "
                        f"(current={current_pair_notional:.2f}, new={new_notional:.2f}, "
                        f"entry_mode={entry_mode}). Refusing order."
                    )

    def _validate_pm_risk_config(self, config: dict[str, Any]) -> None:
        risk_config = config.get("exchange", {}).get("portfolio_margin_risk", {})
        if risk_config is None:
            return
        if not isinstance(risk_config, dict):
            raise OperationalException("exchange.portfolio_margin_risk must be an object.")

        unknown_keys = sorted(set(risk_config) - self._pm_risk_allowed_config_keys)
        if unknown_keys:
            raise OperationalException(
                "Unknown Binance PM risk config key(s): "
                f"{', '.join(unknown_keys)}. Fix the spelling or remove unsupported keys."
            )

        self._validate_pm_risk_positive_values(risk_config)
        self._validate_pm_risk_threshold_order(risk_config)
        self._validate_pm_risk_boolean_keys(risk_config)

    def _validate_pm_risk_positive_values(self, risk_config: dict[str, Any]) -> None:
        for key in self._pm_risk_positive_number_keys:
            value = risk_config.get(key)
            if value is not None and float(value) <= 0:
                raise OperationalException(f"exchange.portfolio_margin_risk.{key} must be > 0.")

        for key in self._pm_risk_positive_integer_keys:
            value = risk_config.get(key)
            if value is not None and int(value) < 1:
                raise OperationalException(f"exchange.portfolio_margin_risk.{key} must be >= 1.")

        max_position_notional = risk_config.get("max_position_notional")
        if max_position_notional is not None:
            if not isinstance(max_position_notional, dict):
                raise OperationalException(
                    "exchange.portfolio_margin_risk.max_position_notional must be an object."
                )
            invalid_caps = [
                pair
                for pair, value in max_position_notional.items()
                if value is None or float(value) <= 0
            ]
            if invalid_caps:
                raise OperationalException(
                    "exchange.portfolio_margin_risk.max_position_notional values must be > 0 "
                    f"for: {', '.join(sorted(invalid_caps))}."
                )

    @staticmethod
    def _validate_pm_risk_threshold_order(risk_config: dict[str, Any]) -> None:
        warning_mmr = risk_config.get("warning_uni_mmr")
        min_mmr = risk_config.get("min_uni_mmr")
        emergency_mmr = risk_config.get("emergency_stop_uni_mmr")
        if warning_mmr is not None and min_mmr is not None and float(warning_mmr) < float(min_mmr):
            raise OperationalException(
                "exchange.portfolio_margin_risk.warning_uni_mmr must be >= min_uni_mmr."
            )
        if (
            emergency_mmr is not None
            and min_mmr is not None
            and float(emergency_mmr) > float(min_mmr)
        ):
            raise OperationalException(
                "exchange.portfolio_margin_risk.emergency_stop_uni_mmr must be <= min_uni_mmr."
            )

    @staticmethod
    def _validate_pm_risk_boolean_keys(risk_config: dict[str, Any]) -> None:
        for key in ("user_stream_enabled", "user_stream_recover_unmatched_orders"):
            if not isinstance(risk_config.get(key, True), bool):
                raise OperationalException(
                    f"exchange.portfolio_margin_risk.{key} must be a boolean."
                )

    @retrier
    def create_pm_listen_key(self) -> str:
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return ""
        try:
            response = self._papi_request("listenKey", "POST")
            self._log_exchange_response("papi_create_listen_key", response)
            return response.get("listenKey", "")
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not create Binance PM listenKey due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def keepalive_pm_listen_key(self, listen_key: str) -> None:
        if not self._is_portfolio_margin() or self._config["dry_run"] or not listen_key:
            return
        try:
            self._papi_request("listenKey", "PUT", {"listenKey": listen_key})
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not keepalive Binance PM listenKey due to {e.__class__.__name__}. "
                f"Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def delete_pm_listen_key(self, listen_key: str) -> None:
        if not self._is_portfolio_margin() or self._config["dry_run"] or not listen_key:
            return
        try:
            self._papi_request("listenKey", "DELETE", {"listenKey": listen_key})
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not delete Binance PM listenKey due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def start_pm_user_stream(self, listen_key: str) -> None:
        if (
            not self._is_portfolio_margin()
            or self._config["dry_run"]
            or not self._pm_user_stream_enabled()
            or not listen_key
        ):
            return

        with self._pm_user_stream_lock:
            if (
                self._pm_user_stream
                and self._pm_user_stream.listen_key == listen_key
                and self._pm_user_stream.stats().get("running")
            ):
                return

            if self._pm_user_stream:
                self._pm_user_stream.stop()
            self._pm_user_stream = BinancePMUserStream(listen_key)
            self._pm_user_stream.start()

    def stop_pm_user_stream(self) -> None:
        with self._pm_user_stream_lock:
            stream = self._pm_user_stream
            self._pm_user_stream = None
        if stream:
            stream.stop()

    def pop_pm_user_stream_events(self, max_events: int = 1000) -> list[dict[str, Any]]:
        with self._pm_user_stream_lock:
            stream = self._pm_user_stream
        if not stream:
            return []
        return stream.pop_events(max_events)

    def get_pm_user_stream_stats(self) -> dict[str, Any]:
        with self._pm_user_stream_lock:
            stream = self._pm_user_stream
        if stream:
            return stream.stats()
        return {
            "enabled": (
                self._is_portfolio_margin()
                and not self._config["dry_run"]
                and self._pm_user_stream_enabled()
            ),
            "running": False,
            "connected": False,
            "listen_key_set": False,
            "queued_events": 0,
            "events_received": 0,
            "events_dropped": 0,
            "reconnects": 0,
            "next_reconnect_delay": None,
            "last_reconnect_delay": None,
            "parse_errors": 0,
            "last_event_type": None,
            "last_event_time": None,
            "last_connected_at": None,
            "last_disconnected_at": None,
            "last_error": None,
        }

    def close(self):
        self.stop_pm_user_stream()
        super().close()

    def get_proxy_coin(self) -> str:
        """
        Get the proxy coin for the given coin
        Falls back to the stake currency if no proxy coin is found
        :return: Proxy coin or stake currency
        """
        if self.margin_mode == MarginMode.CROSS:
            return self._config.get(
                "proxy_coin",
                self._config["stake_currency"],
            )  # type: ignore[return-value]
        return self._config["stake_currency"]

    def market_is_future(self, market: dict[str, Any]) -> bool:
        if self._is_portfolio_margin():
            return (
                market.get(self._ft_has["ccxt_futures_name"], False) is True
                and market.get("type", False) == "swap"
                and market.get("linear", False) is True
                and market.get("settle") in {"USDT", "USDC"}
            )
        return super().market_is_future(market)

    def get_tickers(
        self,
        symbols: list[str] | None = None,
        *,
        cached: bool = False,
        market_type: TradingMode | None = None,
    ) -> Tickers:
        tickers = super().get_tickers(symbols=symbols, cached=cached, market_type=market_type)
        if self.trading_mode == TradingMode.FUTURES:
            # Binance's future result has no bid/ask values.
            # Therefore we must fetch that from fetch_bids_asks and combine the two results.
            bidsasks = self.fetch_bids_asks(symbols, cached=cached)
            tickers = deep_merge_dicts(bidsasks, tickers, allow_null_overrides=False)
        return tickers

    @retrier
    def additional_exchange_init(self) -> None:
        """
        Additional exchange initialization logic.
        .api will be available at this point.
        Must be overridden in child methods if required.
        """
        try:
            if (
                self.trading_mode == TradingMode.FUTURES
                and not self._config["dry_run"]
                and not self._is_portfolio_margin()
            ):
                position_side = self._api.fapiPrivateGetPositionSideDual()
                self._log_exchange_response("position_side_setting", position_side)
                assets_margin = self._api.fapiPrivateGetMultiAssetsMargin()
                self._log_exchange_response("multi_asset_margin", assets_margin)
                msg = ""
                if position_side.get("dualSidePosition") is True:
                    msg += (
                        "\nHedge Mode is not supported by freqtrade. "
                        "Please change 'Position Mode' on your binance futures account."
                    )
                if (
                    assets_margin.get("multiAssetsMargin") is True
                    and self.margin_mode != MarginMode.CROSS
                ):
                    msg += (
                        "\nMulti-Asset Mode is not supported by freqtrade. "
                        "Please change 'Asset Mode' on your binance futures account."
                    )
                if msg:
                    raise OperationalException(msg)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Error in additional_exchange_init due to {e.__class__.__name__}. Message: {e}"
            ) from e

        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def validate_config(self, config) -> None:
        super().validate_config(config)
        if not self._is_portfolio_margin():
            return
        self._validate_pm_risk_config(config)
        if self.trading_mode != TradingMode.FUTURES or self.margin_mode != MarginMode.CROSS:
            raise OperationalException(
                "Binance Portfolio Margin requires trading_mode='futures' and margin_mode='cross'."
            )
        invalid_pairs = [
            pair
            for pair in config.get("exchange", {}).get("pair_whitelist", [])
            if self.markets.get(pair, {}).get("inverse")
            or self.markets.get(pair, {}).get("settle") not in {"USDT", "USDC"}
        ]
        if invalid_pairs:
            raise OperationalException(
                "This Binance PM adapter supports only USDT/USDC linear perpetual contracts. "
                f"Blocked pairs: {', '.join(invalid_pairs)}"
            )

    def get_balances(self, params: dict | None = None) -> CcxtBalances:
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return super().get_balances(params)
        return self._get_pm_balances(params)

    @retrier
    def _get_pm_balances(self, params: dict | None = None) -> CcxtBalances:
        try:
            balances_raw = self._papi_request("balance", "GET", params)
            balances: CcxtBalances = {}
            for balance in balances_raw:
                currency = balance.get("asset")
                if not currency:
                    continue
                total = self._float_or_zero(balance.get("totalWalletBalance"))
                free = self._float_or_zero(balance.get("crossMarginFree"))
                used = (
                    self._float_or_zero(balance.get("crossMarginLocked"))
                    + self._float_or_zero(balance.get("crossMarginBorrowed"))
                    + self._float_or_zero(balance.get("crossMarginInterest"))
                )
                balances[currency] = {
                    "free": free,
                    "used": used,
                    "total": total,
                }
            self._log_exchange_response("papi_balance", balances, add_info=params)
            return balances
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get Binance PM balance due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def fetch_positions(
        self, pair: str | None = None, params: dict | None = None
    ) -> list[CcxtPosition]:
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return super().fetch_positions(pair, params)
        return self._fetch_pm_positions(pair, params)

    @retrier
    def _fetch_pm_positions(
        self, pair: str | None = None, params: dict | None = None
    ) -> list[CcxtPosition]:
        try:
            pairs = [pair] if pair else list(self.markets.keys())
            namespaces = {
                self._pm_namespace_for_pair(ft_pair)
                for ft_pair in pairs
                if ft_pair in self.markets and self.markets[ft_pair].get("swap")
            }
            positions: list[CcxtPosition] = []
            for namespace in sorted(namespaces):
                raw_positions = self._papi_request(f"{namespace}/positionRisk", "GET", params)
                for raw_position in raw_positions:
                    parsed = self._parse_pm_position(raw_position)
                    if parsed and (pair is None or parsed["symbol"] == pair):
                        positions.append(parsed)
            self._log_exchange_response("papi_positions", positions, add_info=params)
            return positions
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get Binance PM positions due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def create_order(
        self,
        *,
        pair: str,
        ordertype: str,
        side: str,
        amount: float,
        rate: float,
        leverage: float,
        time_in_force: str = "GTC",
        reduceOnly: bool = False,
        initial_order: bool = True,
        entry_mode: EntryExecuteMode = "initial",
    ) -> CcxtOrder:
        if not self._is_portfolio_margin() or self._config["dry_run"]:
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
                entry_mode=entry_mode,
            )

        params = self._get_params(side, ordertype, leverage, reduceOnly, time_in_force)
        try:
            amount_contracts = self.amount_to_precision(
                pair, self._amount_to_contracts(pair, amount)
            )
            needs_price = self._order_needs_price(side, ordertype)
            rate_for_order = self.price_to_precision(pair, rate) if needs_price else None
            if not reduceOnly:
                self.assert_pm_risk_allows_order(
                    pair=pair, amount=amount, leverage=leverage, entry_mode=entry_mode
                )
                self._lev_prep(pair, leverage, side, accept_fail=not initial_order)
            raw_order = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/order",
                "POST",
                self._pm_order_params(
                    pair, ordertype, side, amount_contracts, rate_for_order, params
                ),
            )
            self._log_exchange_response("papi_create_order", raw_order)
            return self._order_contracts_to_amount(self._parse_pm_order(raw_order, pair))
        except ccxt.InsufficientFunds as e:
            raise InsufficientFundsError(
                f"Insufficient funds to create {ordertype} {side} PM order on {pair}. "
                f"Tried amount {amount} at rate {rate}. Message: {e}"
            ) from e
        except ccxt.InvalidOrder as e:
            raise InvalidOrderException(
                f"Could not create {ordertype} {side} PM order on {pair}. "
                f"Tried amount {amount} at rate {rate}. Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not place Binance PM order due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier(retries=0)
    def fetch_order(self, order_id: str, pair: str, params: dict | None = None) -> CcxtOrder:
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return super().fetch_order(order_id, pair, params)
        try:
            request = {"symbol": self._pm_symbol_for_pair(pair), "orderId": order_id}
            request.update(params or {})
            raw_order = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/order", "GET", request
            )
            self._log_exchange_response("papi_fetch_order", raw_order)
            return self._order_contracts_to_amount(self._parse_pm_order(raw_order, pair))
        except ccxt.OrderNotFound as e:
            raise TemporaryError(
                f"Order not found (pair: {pair} id: {order_id}). Message: {e}"
            ) from e
        except ccxt.InvalidOrder as e:
            raise InvalidOrderException(
                f"Tried to get an invalid PM order (pair: {pair} id: {order_id}). Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get Binance PM order due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def cancel_order(self, order_id: str, pair: str, params: dict | None = None) -> dict[str, Any]:
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return super().cancel_order(order_id, pair, params)
        return self._cancel_pm_order(order_id, pair, params)

    @retrier
    def _cancel_pm_order(
        self, order_id: str, pair: str, params: dict | None = None
    ) -> dict[str, Any]:
        try:
            request = {"symbol": self._pm_symbol_for_pair(pair), "orderId": order_id}
            request.update(params or {})
            raw_order = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/order", "DELETE", request
            )
            self._log_exchange_response("papi_cancel_order", raw_order)
            return self._order_contracts_to_amount(self._parse_pm_order(raw_order, pair))
        except ccxt.InvalidOrder as e:
            raise InvalidOrderException(f"Could not cancel Binance PM order. Message: {e}") from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not cancel Binance PM order due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def _set_leverage(
        self,
        leverage: float,
        pair: str | None = None,
        accept_fail: bool = False,
    ):
        if not self._is_portfolio_margin():
            return super()._set_leverage(leverage, pair, accept_fail)
        return self._set_pm_leverage(leverage, pair, accept_fail)

    @retrier
    def _set_pm_leverage(
        self,
        leverage: float,
        pair: str | None = None,
        accept_fail: bool = False,
    ):
        if self._config["dry_run"] or pair is None:
            return
        if self._ft_has.get("floor_leverage", False) is True:
            leverage = int(leverage)
        try:
            res = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/leverage",
                "POST",
                {"symbol": self._pm_symbol_for_pair(pair), "leverage": leverage},
            )
            self._log_exchange_response("papi_set_leverage", res)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.BadRequest, ccxt.OperationRejected, ccxt.InsufficientFunds) as e:
            if not accept_fail:
                raise TemporaryError(
                    f"Could not set Binance PM leverage due to {e.__class__.__name__}. Message: {e}"
                ) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not set Binance PM leverage due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def get_historic_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since_ms: int,
        candle_type: CandleType,
        is_new_pair: bool = False,
        until_ms: int | None = None,
    ) -> DataFrame:
        """
        Overwrite to introduce "fast new pair" functionality by detecting the pair's listing date
        Does not work for other exchanges, which don't return the earliest data when called with "0"
        :param candle_type: Any of the enum CandleType (must match trading mode!)
        """
        if is_new_pair and candle_type in (CandleType.SPOT, CandleType.FUTURES, CandleType.MARK):
            with self._loop_lock:
                x = self.loop.run_until_complete(
                    self._async_get_candle_history(pair, timeframe, candle_type, 0)
                )
            if x and x[3] and x[3][0] and x[3][0][0] > since_ms:
                # Set starting date to first available candle.
                since_ms = x[3][0][0]
                logger.info(
                    f"Candle-data for {pair} available starting with "
                    f"{datetime.fromtimestamp(since_ms // 1000, tz=UTC).isoformat()}."
                )
                if until_ms and since_ms >= until_ms:
                    logger.warning(
                        f"No available candle-data for {pair} before "
                        f"{dt_from_ts(until_ms).isoformat()}"
                    )
                    return DataFrame(columns=DEFAULT_DATAFRAME_COLUMNS)

        if (
            not self._can_use_data_download_fast
            or self._config["exchange"].get("only_from_ccxt", False)
            or
            # only download timeframes with significant improvements,
            # otherwise fall back to rest API
            not (
                (candle_type == CandleType.SPOT and timeframe in ["1s", "1m", "3m", "5m"])
                or (
                    candle_type == CandleType.FUTURES
                    and timeframe in ["1m", "3m", "5m", "15m", "30m"]
                )
            )
        ):
            return super().get_historic_ohlcv(
                pair=pair,
                timeframe=timeframe,
                since_ms=since_ms,
                candle_type=candle_type,
                is_new_pair=is_new_pair,
                until_ms=until_ms,
            )
        else:
            # Download from data.binance.vision
            return self.get_historic_ohlcv_fast(
                pair=pair,
                timeframe=timeframe,
                since_ms=since_ms,
                candle_type=candle_type,
                is_new_pair=is_new_pair,
                until_ms=until_ms,
            )

    def get_historic_ohlcv_fast(
        self,
        pair: str,
        timeframe: str,
        since_ms: int,
        candle_type: CandleType,
        is_new_pair: bool = False,
        until_ms: int | None = None,
    ) -> DataFrame:
        """
        Fastly fetch OHLCV data by leveraging https://data.binance.vision.
        """
        with self._loop_lock:
            df = self.loop.run_until_complete(
                download_archive_ohlcv(
                    candle_type=candle_type,
                    pair=pair,
                    timeframe=timeframe,
                    since_ms=since_ms,
                    until_ms=until_ms,
                    markets=self.markets,
                )
            )

        # download the remaining data from rest API
        if df.empty:
            rest_since_ms = since_ms
        else:
            rest_since_ms = dt_ts(df.iloc[-1].date) + timeframe_to_msecs(timeframe)

        # make sure since <= until
        if until_ms and rest_since_ms > until_ms:
            rest_df = DataFrame()
        else:
            rest_df = super().get_historic_ohlcv(
                pair=pair,
                timeframe=timeframe,
                since_ms=rest_since_ms,
                candle_type=candle_type,
                is_new_pair=is_new_pair,
                until_ms=until_ms,
            )
        all_df = concat_safe([df, rest_df])
        return all_df

    def funding_fee_cutoff(self, open_date: datetime):
        """
        Funding fees are only charged at full hours (usually every 4-8h).
        Therefore a trade opening at 10:00:01 will not be charged a funding fee until the next hour.
        On binance, this cutoff is 15s.
        https://github.com/freqtrade/freqtrade/pull/5779#discussion_r740175931
        :param open_date: The open date for a trade
        :return: True if the date falls on a full hour, False otherwise
        """
        return open_date.minute == 0 and open_date.second < 15

    def fetch_funding_rates(self, symbols: list[str] | None = None) -> dict[str, dict[str, float]]:
        """
        Fetch funding rates for the given symbols.
        :param symbols: List of symbols to fetch funding rates for
        :return: Dict of funding rates for the given symbols
        """
        try:
            if self.trading_mode == TradingMode.FUTURES:
                rates = self._api.fetch_funding_rates(symbols)
                return rates
            return {}
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Error in additional_exchange_init due to {e.__class__.__name__}. Message: {e}"
            ) from e

        except ccxt.BaseError as e:
            raise OperationalException(e) from e

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
        Important: Must be fetching data from cached values as this is used by backtesting!
        MARGIN: https://www.binance.com/en/support/faq/f6b010588e55413aa58b7d63ee0125ed
        PERPETUAL: https://www.binance.com/en/support/faq/b3c689c1f50a44cabb3a84e663b81d93

        :param pair: Pair to calculate liquidation price for
        :param open_rate: Entry price of position
        :param is_short: True if the trade is a short, false otherwise
        :param amount: Absolute value of position size incl. leverage (in base currency)
        :param stake_amount: Stake amount - Collateral in settle currency.
        :param leverage: Leverage used for this position.
        :param wallet_balance: Amount of margin_mode in the wallet being used to trade
            Cross-Margin Mode: crossWalletBalance
            Isolated-Margin Mode: isolatedWalletBalance
        :param open_trades: List of open trades in the same wallet

        # * Only required for Cross
        :param mm_ex_1: (TMM)
            Cross-Margin Mode: Maintenance Margin of all other contracts, excluding Contract 1
            Isolated-Margin Mode: 0
        :param upnl_ex_1: (UPNL)
            Cross-Margin Mode: Unrealized PNL of all other contracts, excluding Contract 1.
            Isolated-Margin Mode: 0
        :param other
        """
        cross_vars: float = 0.0

        # mm_ratio: Binance's formula specifies maintenance margin rate which is mm_ratio * 100%
        # maintenance_amt: (CUM) Maintenance Amount of position
        mm_ratio, maintenance_amt = self.get_maintenance_ratio_and_amt(pair, stake_amount)

        if self.margin_mode == MarginMode.CROSS:
            mm_ex_1: float = 0.0
            upnl_ex_1: float = 0.0
            pairs = [trade.pair for trade in open_trades]
            if self._config["runmode"] in ("live", "dry_run"):
                funding_rates = self.fetch_funding_rates(pairs)
            for trade in open_trades:
                if trade.pair == pair:
                    # Only "other" trades are considered
                    continue
                if self._config["runmode"] in ("live", "dry_run"):
                    mark_price = funding_rates[trade.pair]["markPrice"]
                else:
                    # Fall back to open rate for backtesting
                    mark_price = trade.open_rate
                mm_ratio1, maint_amnt1 = self.get_maintenance_ratio_and_amt(
                    trade.pair, trade.stake_amount
                )
                maint_margin = trade.amount * mark_price * mm_ratio1 - maint_amnt1
                mm_ex_1 += maint_margin

                upnl_ex_1 += trade.amount * mark_price - trade.amount * trade.open_rate

            cross_vars = upnl_ex_1 - mm_ex_1

        side_1 = -1 if is_short else 1

        if maintenance_amt is None:
            raise OperationalException(
                "Parameter maintenance_amt is required by Binance.liquidation_price"
                f"for {self.trading_mode}"
            )

        if self.trading_mode == TradingMode.FUTURES:
            return (
                (wallet_balance + cross_vars + maintenance_amt) - (side_1 * amount * open_rate)
            ) / ((amount * mm_ratio) - (side_1 * amount))
        else:
            raise OperationalException(
                "Freqtrade only supports isolated futures for leverage trading"
            )

    def load_leverage_tiers(self) -> dict[str, list[dict]]:
        if self.trading_mode == TradingMode.FUTURES:
            if self._config["dry_run"]:
                leverage_tiers_path = Path(__file__).parent / "binance_leverage_tiers.json"
                with leverage_tiers_path.open() as json_file:
                    return json_load(json_file)
            else:
                return self.get_leverage_tiers()
        else:
            return {}

    async def _async_get_trade_history_id_startup(
        self, pair: str, since: int
    ) -> tuple[list[list], str]:
        """
        override for initial call

        Binance only provides a limited set of historic trades data.
        Using from_id=0, we can get the earliest available trades.
        So if we don't get any data with the provided "since", we can assume to
        download all available data.
        """
        t, from_id = await self._async_fetch_trades(pair, since=since)
        if not t:
            return [], "0"
        return t, from_id

    async def _async_get_trade_history_id(
        self, pair: str, until: int, since: int, from_id: str | None = None
    ) -> tuple[str, list[list]]:
        logger.info(f"Fetching trades for {pair} from Binance, {from_id=}, {since=}, {until=}")

        if (
            not self._config["exchange"].get("only_from_ccxt", False)
            and self._can_use_data_download_fast
        ):
            if from_id is None or not since:
                trades = await self._api_async.fetch_trades(
                    pair,
                    params={
                        self._ft_has["trades_pagination_arg"]: "0",
                    },
                    limit=5,
                )
                listing_date: int = trades[0]["timestamp"]
                since = max(since, listing_date)

            _, res = await download_archive_trades(
                CandleType.FUTURES if self.trading_mode == "futures" else CandleType.SPOT,
                pair,
                since_ms=since,
                until_ms=until,
                markets=self.markets,
            )

            if not res:
                end_time = since
                end_id = from_id
            else:
                end_time = res[-1][0]
                end_id = res[-1][1]

            if end_time and end_time >= until:
                return pair, res
            else:
                _, res2 = await super()._async_get_trade_history_id(
                    pair, until=until, since=end_time, from_id=end_id
                )
                res.extend(res2)
                return pair, res

        return await super()._async_get_trade_history_id(
            pair, until=until, since=since, from_id=from_id
        )

    def _check_delisting_futures(self, pair: str) -> datetime | None:
        delivery_time = self.markets.get(pair, {}).get("info", {}).get("deliveryDate", None)
        if delivery_time:
            if isinstance(delivery_time, str) and (delivery_time != ""):
                delivery_time = int(delivery_time)

            # Binance set a very high delivery time for all perpetuals.
            # We compare with delivery time of BTC/USDT:USDT which assumed to never be delisted
            btc_delivery_time = (
                self.markets.get("BTC/USDT:USDT", {}).get("info", {}).get("deliveryDate", None)
            )

            if delivery_time == btc_delivery_time:
                return None

            delivery_time = dt_from_ts(delivery_time)

        return delivery_time

    def check_delisting_time(self, pair: str) -> datetime | None:
        """
        Check if the pair gonna be delisted.
        By default, it returns None.
        :param pair: Market symbol
        :return: Datetime if the pair gonna be delisted, None otherwise
        """
        if self._config["runmode"] not in TRADE_MODES:
            return None

        if self.trading_mode == TradingMode.FUTURES:
            return self._check_delisting_futures(pair)
        return self._get_spot_pair_delist_time(pair, refresh=False)

    def _get_spot_delist_schedule(self):
        """
        Get the delisting schedule for spot pairs
        Only works in live mode as it requires API keys,
        Return sample:
        [{
            "delistTime": "1759114800000",
            "symbols": [
                "OMNIBTC",
                "OMNIFDUSD",
                "OMNITRY",
                "OMNIUSDC",
                "OMNIUSDT"
            ]
        }]
        """
        try:
            delist_schedule = self._api.sapi_get_spot_delist_schedule()
            return delist_schedule
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.NetworkError, ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get delist schedule {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def _get_spot_pair_delist_time(self, pair: str, refresh: bool = False) -> datetime | None:
        """
        Get the delisting time for a pair if it will be delisted
        :param pair: Pair to get the delisting time for
        :param refresh: true if you need fresh data
        :return: int: delisting time None if not delisting
        """

        if not pair or not self._config["runmode"] == RunMode.LIVE:
            # Endpoint only works in live mode as it requires API keys
            return None

        cache = self._spot_delist_schedule_cache

        if not refresh:
            if delist_time := cache.get(pair, None):
                return delist_time

        delist_schedule = self._get_spot_delist_schedule()

        if delist_schedule is None:
            return None

        for schedule in delist_schedule:
            delist_dt = dt_from_ts(int(schedule["delistTime"]))
            for symbol in schedule["symbols"]:
                ft_symbol = next(
                    (
                        pair
                        for pair, market in self.markets.items()
                        if market.get("id", None) == symbol
                    ),
                    None,
                )
                if ft_symbol is None:
                    continue

                cache[ft_symbol] = delist_dt

        return cache.get(pair, None)


class Binanceusdm(Binance):
    """Binacne USDM Exchange
    Same as Binance - only futures trading is supported (via ccxt).

    Not actually necessary, binance should be preferred.
    """

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.FUTURES, MarginMode.CROSS),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
    ]


class Binanceus(Binance):
    """Binance US exchange class.
    Minimal adjustment to disable futures trading for the US subsidiary of Binance
    """

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
    ]
    # binance vision does not have data for binanceus
    _can_use_data_download_fast = False
