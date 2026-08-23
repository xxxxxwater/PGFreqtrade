"""Binance exchange subclass"""

import hashlib
import hmac
import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Any

import ccxt
from pandas import DataFrame

from freqtrade.constants import DEFAULT_DATAFRAME_COLUMNS, BuySell, EntryExecuteMode
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
from freqtrade.exchange.exchange_utils import ROUND_DOWN, ROUND_UP
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
        "wallet_mode",
        "collateral_haircut",
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
        "risk_api_failure_action",
        "user_stream_fail_closed",
        "allow_degraded_rest_recovery",
        "max_daily_loss_include_unrealized",
        "startup_consistency_mode",
        "emergency_close_retries",
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
        "emergency_close_retries",
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

    async def _api_reload_markets(self, reload: bool = False) -> None:
        if not self._is_portfolio_margin():
            return await super()._api_reload_markets(reload=reload)

        async def _empty_list(*a, **kw) -> list:
            return []

        original_margin_all = getattr(self._api_async, "sapiGetMarginAllPairs", None)
        original_margin_iso = getattr(self._api_async, "sapiGetMarginIsolatedAllPairs", None)
        try:
            self._api_async.sapiGetMarginAllPairs = _empty_list
            self._api_async.sapiGetMarginIsolatedAllPairs = _empty_list
            await super()._api_reload_markets(reload=reload)
        except Exception:
            if self._api_async.markets:
                logger.warning(
                    "PM: load_markets had errors on SAPI endpoints; "
                    "futures markets populated successfully."
                )
            else:
                raise
        finally:
            if original_margin_all is not None:
                self._api_async.sapiGetMarginAllPairs = original_margin_all
            if original_margin_iso is not None:
                self._api_async.sapiGetMarginIsolatedAllPairs = original_margin_iso

    def _papi_request(
        self, path: str, method: str = "GET", params: dict[str, Any] | None = None
    ) -> Any:
        """
        Signed Binance Portfolio Margin request.

        ccxt exposes Binance PAPI under the ``papi`` namespace (not ``papiPrivate``).
        Use ccxt first so we keep its signing, throttling and error mapping. Keep a
        small raw-HTTP fallback for ccxt builds where PAPI is missing or incomplete.
        """
        path = self._normalize_papi_path(path)
        method = method.upper()
        request_params = dict(params or {})
        try:
            result = self._api.request(path, "papi", method, request_params)
        except (ccxt.AuthenticationError, ccxt.PermissionDenied, ccxt.OperationRejected) as e:
            if isinstance(e, ccxt.OperationRejected) and self._extract_binance_error_code(
                str(e)
            ) not in {-2015, -2014}:
                raise
            raise self._pm_auth_exception(path, method, str(e)) from e
        except ccxt.NotSupported:
            result = self._raw_papi_request(path, method, request_params)
        except ccxt.BaseError:
            raise

        # Track the last successful PAPI request for the API health endpoint.
        self._last_papi_success_time = datetime.now(UTC).isoformat()
        return result

    @staticmethod
    def _normalize_papi_path(path: str) -> str:
        path = path.strip().lstrip("/")
        for prefix in ("papi/v1/", "papi/"):
            if path.startswith(prefix):
                path = path[len(prefix) :]
        return path

    def _raw_papi_request(self, path: str, method: str, params: dict[str, Any]) -> Any:
        url_base = (
            self._config.get("exchange", {})
            .get("portfolio_margin_base_url", "https://papi.binance.com/papi/v1")
            .rstrip("/")
        )
        api_key = getattr(self._api, "apiKey", "")
        secret = getattr(self._api, "secret", "")
        if not api_key or not secret:
            raise OperationalException("Binance PM API key/secret not configured.")

        payload = {
            "recvWindow": self._pm_recv_window(),
            "timestamp": int(time.time() * 1000),
        }
        payload.update(params)
        query = urllib.parse.urlencode(payload)
        signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"{url_base}/{path}?{query}&signature={signature}"
        req = urllib.request.Request(  # noqa: S310 - fixed HTTPS Binance PM endpoint.
            url, headers={"X-MBX-APIKEY": api_key}, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310
                raw = resp.read().decode()
                if not raw:
                    return {}
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            # Never treat all 4xx as auth errors: classify by the Binance error code.
            raise self._papi_http_exception(path, method, body, e.code) from e
        except urllib.error.URLError as e:
            raise TemporaryError(f"Binance PM API network error on {method} {path}: {e}") from e

    @staticmethod
    def _binance_error_category(code: int | None) -> str:
        """Classify a Binance error code so raw HTTP failures map to the right exception."""
        if code is None:
            return "unknown"
        if code in {-1002, -1022, -2014, -2015}:
            return "auth"
        if code in {-1001, -1003, -1015}:
            return "rate_limit"
        if code in {-2018, -2019}:
            return "insufficient_funds"
        if code in {
            -2010,
            -2011,
            -2012,
            -2013,  # "No such order" in PM/futures.
            -2020,
            -2021,
            -2022,
            -2024,
            -2025,
            -2026,
            -2027,
            -2028,
        }:
            return "order_rejected"
        if code == -1021 or (-1131 <= code <= -1100):
            return "invalid_request"
        return "other"

    def _papi_http_exception(
        self, path: str, method: str, body: str, http_code: int
    ) -> Exception:
        code = self._extract_binance_error_code(body)
        category = self._binance_error_category(code)
        endpoint = f"{method} /papi/v1/{path}"
        snippet = body[:300]
        if category == "auth":
            return self._pm_auth_exception(path, method, body)
        if category == "rate_limit":
            return TemporaryError(f"Binance PM rate limit (code {code}) on {endpoint}: {snippet}")
        if category == "insufficient_funds":
            return InsufficientFundsError(
                f"Binance PM insufficient funds (code {code}) on {endpoint}: {snippet}"
            )
        if category == "order_rejected":
            return InvalidOrderException(
                f"Binance PM order rejected (code {code}) on {endpoint}: {snippet}"
            )
        if category == "invalid_request":
            return InvalidOrderException(
                f"Binance PM rejected request (code {code}) on {endpoint}: {snippet}"
            )
        # Unknown codes and generic HTTP errors must never be reported as auth failures.
        return OperationalException(
            f"Binance PM API error (code {code}) HTTP {http_code} on {endpoint}: {snippet}"
        )

    def _pm_recv_window(self) -> int:
        return int(
            self._config.get("exchange", {}).get(
                "portfolio_margin_recv_window",
                getattr(self._api, "options", {}).get("recvWindow", 10000),
            )
        )

    def _pm_auth_exception(self, path: str, method: str, message: str) -> OperationalException:
        code = self._extract_binance_error_code(message)
        request_ip = self._extract_binance_request_ip(message)
        endpoint = f"/papi/v1/{path}"
        hints = [
            "confirm the bot is using the intended Binance API key/secret",
            "confirm the key is enabled for Portfolio Margin PAPI endpoints",
            "confirm Binance API IP restrictions include this machine/container egress IP",
            "confirm this is a standard Portfolio Margin account; PM Pro account queries use "
            "Binance portfolio SAPI endpoints, while this adapter trades standard PM UM futures",
            "confirm Docker Compose is not overriding exchange.key/secret with empty "
            "FREQTRADE__EXCHANGE__KEY or FREQTRADE__EXCHANGE__SECRET values",
        ]
        if request_ip:
            hints.insert(2, f"Binance saw request IP {request_ip}")
        code_part = f" code {code}" if code is not None else ""
        return OperationalException(
            f"Binance PM authentication failed on {method} {endpoint}{code_part}: "
            f"{message[:300]}. Checks: {'; '.join(hints)}."
        )

    @staticmethod
    def _extract_binance_error_code(message: str) -> int | None:
        try:
            body = json.loads(message)
            code = body.get("code")
            return int(code) if code is not None else None
        except (TypeError, ValueError, json.JSONDecodeError):
            match = re.search(r'"code"\s*:\s*(-?\d+)|code(?:=|:)\s*(-?\d+)', message)
            if not match:
                return None
            return int(next(group for group in match.groups() if group is not None))

    @staticmethod
    def _extract_binance_request_ip(message: str) -> str | None:
        match = re.search(r"request ip:\s*([0-9a-fA-F:.]+)", message)
        if match:
            return match.group(1)
        return None

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

    def _parse_pm_trade(self, trade: dict[str, Any], pair: str | None = None) -> dict[str, Any]:
        """Convert a PAPI /um/userTrades entry into the ccxt trade shape freqtrade expects."""
        symbol_id = trade.get("symbol")
        symbol = pair or self._api.safe_symbol(symbol_id, None, None, "contract")
        price = self._float_or_none(trade.get("price"))
        amount = self._float_or_zero(trade.get("qty"))
        cost = self._float_or_none(trade.get("quoteQty"))
        commission = self._float_or_none(trade.get("commission"))
        commission_asset = trade.get("commissionAsset")
        timestamp = self._float_or_none(trade.get("time"))

        fee = None
        if commission is not None and commission_asset:
            fee = {"cost": commission, "currency": commission_asset}

        return {
            "id": str(trade.get("id") or trade.get("tradeId")),
            "order": str(trade.get("orderId")),
            "symbol": symbol,
            "side": str(trade.get("side", "")).lower() or None,
            "price": price,
            "amount": amount,
            "cost": cost,
            "fee": fee,
            "datetime": dt_from_ts(int(timestamp)) if timestamp else None,
            "timestamp": int(timestamp) if timestamp else None,
            "takerOrMaker": "maker" if trade.get("maker") else "taker",
            "info": trade,
        }

    @staticmethod
    def _pm_conditional_status(status: str | None) -> str | None:
        """
        Map Binance PM conditional order ``strategyStatus`` to a ccxt order status.

        IMPORTANT: TRIGGERED only means the strategy fired and a real order was
        submitted. It does NOT mean the real order filled. The strategy must stay
        ``open`` until the real order (``orderId`` in the conditional history
        response) is resolved - otherwise freqtrade would mark the trade closed
        while the actual position is still open.
        """
        if status is None:
            return None
        return {
            "NEW": "open",
            "TRIGGERED": "open",
            "CANCELLED": "canceled",
            "CANCELED": "canceled",
            "EXPIRED": "expired",
            "FINISHED": "closed",
        }.get(status.upper(), status.lower())

    def _parse_pm_conditional_order(
        self, order: dict[str, Any], pair: str | None = None
    ) -> CcxtOrder:
        """Convert a PAPI conditional (stoploss) order response into a ccxt order shape."""
        symbol_id = order.get("symbol")
        symbol = pair or self._api.safe_symbol(symbol_id, None, None, "contract")
        amount = self._float_or_none(order.get("quantity") or order.get("origQty"))
        price = self._float_or_none(order.get("price"))
        stop_price = self._float_or_none(order.get("stopPrice"))
        strategy_id = order.get("strategyId")
        client_strategy_id = order.get("newClientStrategyId")
        timestamp = self._float_or_none(order.get("updateTime") or order.get("bookTime"))

        # freqtrade stores order["id"] and uses it to fetch/cancel later. For conditional
        # orders we prefer the stable client strategy id we generated (queryable by
        # ``newClientStrategyId``); keep the exchange ``strategyId`` in ``info``.
        order_id = str(client_strategy_id or strategy_id or "")

        # Official field names (binance-connector-js QueryUmConditionalOrderHistoryResponse):
        # ``orderId``/``status``/``type``/``triggerTime`` are only present once the
        # strategy has been triggered and refer to the REAL order the exchange submitted.
        info = dict(order)
        info["strategy_status"] = order.get("strategyStatus")
        info["actual_order_id"] = order.get("orderId")
        info["actual_order_status"] = order.get("status")

        return {
            "id": order_id,
            "clientStrategyId": client_strategy_id,
            "timestamp": int(timestamp) if timestamp else None,
            "datetime": dt_from_ts(int(timestamp)) if timestamp else None,
            "lastTradeTimestamp": int(timestamp) if timestamp else None,
            "symbol": symbol,
            "type": "stoploss",
            "timeInForce": order.get("timeInForce"),
            "side": str(order.get("side", "")).lower() or None,
            "price": price,
            "average": None,
            "stopPrice": stop_price,
            "amount": amount,
            "filled": 0.0,
            "remaining": amount,
            "cost": 0.0,
            "status": self._pm_conditional_status(order.get("strategyStatus")),
            "fee": None,
            "trades": [],
            "info": info,
        }

    def _pm_resolve_conditional_actual_order(
        self, strategy_order: CcxtOrder, pair: str
    ) -> CcxtOrder:
        """
        Resolve the REAL order behind a triggered conditional strategy.

        A triggered PM conditional order reports ``orderId`` (the real order the
        exchange submitted) and ``status`` in the conditional history response. We
        fetch that real order via /um/order to obtain authoritative fill data
        (filled / average / cost / fee / trades), then merge it back into the
        strategy order shape:

        * ``id`` stays the strategy id so freqtrade keeps matching the local
          stoploss order.
        * The real order id is kept as ``id_stop`` (see
          ``exchange.fetch_stoploss_order`` algo-order pattern).
        * ``status_stop`` is set to "triggered" for diagnostics.

        Fails open: if the real order cannot be fetched yet (transient), we keep
        the strategy order open - never close, never fake a fill.
        """
        actual_id = strategy_order.get("info", {}).get("orderId")
        if not actual_id:
            return strategy_order
        try:
            actual_order = self.fetch_order(str(actual_id), pair)
        except InvalidOrderException:
            # Real order not (yet) visible. Stay open and retry on the next loop.
            logger.warning(
                "PM conditional %s TRIGGERED but real order %s not found yet; "
                "keeping strategy open.",
                strategy_order.get("id"),
                actual_id,
            )
            return strategy_order

        merged: CcxtOrder = dict(actual_order)
        merged["id"] = strategy_order["id"]
        merged["id_stop"] = str(actual_order.get("id") or actual_id)
        merged["stopPrice"] = strategy_order.get("stopPrice")
        merged["status_stop"] = "triggered"
        merged["type"] = "stoploss"
        info = dict(strategy_order.get("info") or {})
        info["actual_order"] = actual_order
        info["actual_order_id"] = actual_order.get("id") or actual_id
        info["actual_order_status"] = actual_order.get("status")
        merged["info"] = info
        return merged

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
        # Idempotency: every PM order must carry a unique, traceable client order id.
        # If a timeout/network error follows a successful placement, the caller can
        # resolve the order by this id instead of blindly resubmitting it.
        if "newClientOrderId" not in request:
            request["newClientOrderId"] = self._pm_new_client_order_id()
        return request

    @staticmethod
    def _pm_new_client_order_id() -> str:
        """
        Generate a unique, traceable client order id for a PM order.

        Binance PM UM `newClientOrderId` must match `^[.A-Z\\:/a-z0-9_-]{1,32}$`,
        so the id is capped at 32 characters.
        """
        return f"ft{uuid.uuid4().hex[:28]}"

    @staticmethod
    def _pm_new_client_strategy_id() -> str:
        """
        Generate a unique client strategy id for a PM conditional (stoploss) order.

        Binance PM UM conditional `newClientStrategyId` must match
        `^[.A-Z\\:/a-z0-9_-]{1,32}$`.
        """
        return f"st{uuid.uuid4().hex[:28]}"

    # ---- Persistent order intents (transactional DB, idempotency) ----
    #
    # Before ANY POST to /um/order or /um/conditional/order the order intent is
    # durably committed to the MAIN freqtrade database (pm_order_intents table,
    # created by ModelBase.metadata.create_all). On timeout/429/disconnect the
    # SAME client id is used for lookup/recovery - never a fresh id - so a process
    # restart or a later strategy loop can never submit a duplicate entry / DCA /
    # reduceOnly / stoploss order.
    #
    # The durable commit is a hard prerequisite for the POST: failing to write,
    # read, commit or parse the intent store raises (fail-closed) and the order
    # is NOT submitted.
    #
    # NOTE: the model is imported lazily inside each method to avoid a circular
    # import (freqtrade.exchange -> binance -> persistence -> trade_model ->
    # freqtrade.exchange).

    @staticmethod
    def _pm_intent_model():
        from freqtrade.persistence.pm_order_intent import PMOrderIntent

        return PMOrderIntent

    def _pm_intent_put(self, client_id: str, intent: dict[str, Any]) -> None:
        """
        Durably commit the order intent BEFORE the POST.

        Raises OperationalException on ANY failure - the caller must not submit
        the order when this raises.
        """
        PMOrderIntent = self._pm_intent_model()
        try:
            row = PMOrderIntent(
                client_id=client_id,
                kind=str(intent.get("kind")),
                pair=str(intent.get("pair")),
                side=intent.get("side"),
                order_type=intent.get("type"),
                amount=intent.get("amount"),
                price=intent.get("price"),
                stop_price=intent.get("stop_price"),
                reduce_only=bool(intent.get("reduce_only", False)),
                state="PENDING",
            )
            PMOrderIntent.session.add(row)
            PMOrderIntent.session.commit()
        except Exception as e:
            try:
                PMOrderIntent.session.rollback()
            except Exception:
                pass
            raise OperationalException(
                "PM order intent could not be durably committed to the database; "
                f"refusing to submit the order (fail-closed). Error: {e}"
            ) from e

    def _pm_intent_clear(self, client_id: str) -> None:
        """Delete a resolved intent. Raises on failure (fail-closed)."""
        PMOrderIntent = self._pm_intent_model()
        try:
            row = PMOrderIntent.get_by_client_id(client_id)
            if row is not None:
                PMOrderIntent.session.delete(row)
                PMOrderIntent.session.commit()
        except Exception as e:
            try:
                PMOrderIntent.session.rollback()
            except Exception:
                pass
            raise OperationalException(
                f"PM order intent cleanup failed for {client_id}: {e} (fail-closed)"
            ) from e

    def _pm_intent_mark_uncertain(self, client_id: str, error: str) -> None:
        """Transition an intent to UNKNOWN (durable). Raises on failure (fail-closed)."""
        PMOrderIntent = self._pm_intent_model()
        try:
            row = PMOrderIntent.get_by_client_id(client_id)
            if row is None:
                raise OperationalException(
                    f"PM order intent {client_id} not found in the store - the store is "
                    "inconsistent; refusing to continue."
                )
            row.state = "UNKNOWN"
            row.last_error = error[:250]
            PMOrderIntent.session.commit()
        except Exception as e:
            try:
                PMOrderIntent.session.rollback()
            except Exception:
                pass
            raise OperationalException(
                f"PM order intent could not be marked UNKNOWN for {client_id}: {e} "
                "(fail-closed)"
            ) from e

    def pm_intent_store_ok(self) -> bool:
        """Whether the intent store is readable (any failure => False => fail-closed)."""
        PMOrderIntent = self._pm_intent_model()
        try:
            PMOrderIntent.session.query(PMOrderIntent).limit(1).all()
            return True
        except Exception:
            return False

    def pm_has_unresolved_intents(self) -> bool:
        """
        Whether any intent is PENDING/UNKNOWN. Store errors count as unresolved
        (fail-closed: never submit when the store cannot be trusted).
        """
        PMOrderIntent = self._pm_intent_model()
        try:
            return PMOrderIntent.has_unresolved()
        except Exception:
            return True

    def pm_unresolved_intent_count(self) -> int:
        """Number of unresolved intents; -1 when the store is unreadable."""
        PMOrderIntent = self._pm_intent_model()
        try:
            return len(PMOrderIntent.get_unresolved())
        except Exception:
            return -1

    def list_pm_pending_intents(self) -> list[dict[str, Any]]:
        """
        All unresolved intents (PENDING/UNKNOWN) from the durable store.

        Raises OperationalException when the store cannot be read - callers must
        treat this as fail-closed, never as "no intents".
        """
        PMOrderIntent = self._pm_intent_model()
        try:
            return [row.to_dict() for row in PMOrderIntent.get_unresolved()]
        except Exception as e:
            raise OperationalException(
                f"Could not read PM order intents from the database: {e} (fail-closed)"
            ) from e

    def clear_pm_pending_intent(self, client_id: str) -> None:
        """Remove a persisted intent once it has been definitively resolved."""
        self._pm_intent_clear(client_id)

    def resolve_pm_pending_intent(self, intent: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve one persisted intent against the exchange.

        Returns a report with:
        * ``resolved`` False + ``uncertain`` True: the exchange could not be queried;
          the intent MUST stay pending and new orders must stay blocked.
        * ``resolved`` True + ``exists`` False: the exchange definitively has no such
          order; the intent can be cleared.
        * ``resolved`` True + ``exists`` True: the order exists on the exchange; the
          caller must reconcile it with the local database before clearing.
        """
        client_id = str(intent.get("client_id") or "")
        pair = intent.get("pair") or ""
        report: dict[str, Any] = {
            "client_id": client_id,
            "kind": intent.get("kind"),
            "pair": pair,
            "resolved": False,
            "uncertain": False,
            "exists": None,
            "order": None,
            "error": None,
        }
        if not client_id or not pair:
            report["resolved"] = True
            report["exists"] = False
            report["error"] = "intent is malformed (missing client_id/pair)"
            return report
        try:
            if intent.get("kind") == "conditional":
                order = self.fetch_stoploss_order(client_id, pair)
            else:
                order = self._pm_fetch_order_by_client_id(client_id, pair)
        except InvalidOrderException:
            report["resolved"] = True
            report["exists"] = False
            return report
        except Exception as e:
            report["uncertain"] = True
            report["error"] = f"{e.__class__.__name__}: {e}"
            return report
        report["resolved"] = True
        report["exists"] = order is not None
        report["order"] = order
        return report

    def _pm_fetch_order_by_client_id(self, client_order_id: str, pair: str) -> CcxtOrder | None:
        """
        Resolve a PM order by its client order id.

        Returns None only when the exchange definitively reports the order does not exist.
        Transient lookup failures (network/timeout) are propagated so the caller never
        mistakes "unknown" for "absent" and therefore never blindly resubmits.
        """
        request: dict[str, Any] = {
            "symbol": self._pm_symbol_for_pair(pair),
            "origClientOrderId": client_order_id,
        }
        try:
            raw_order = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/order", "GET", request
            )
            self._log_exchange_response("papi_fetch_order_by_client_id", raw_order)
            return self._order_contracts_to_amount(self._parse_pm_order(raw_order, pair))
        except ccxt.OrderNotFound:
            return None
        except (ccxt.InvalidOrder, InvalidOrderException) as e:
            # Definitive "order does not exist" -> absent; anything else propagates.
            if self._extract_binance_error_code(str(e)) in {-2011, -2012, -2013}:
                return None
            raise

    def _pm_place_order(
        self,
        pair: str,
        ordertype: str,
        side: str,
        amount_contracts: float,
        rate_for_order: float | None,
        params: dict[str, Any],
        *,
        log_tag: str,
    ) -> CcxtOrder:
        """
        Submit an order to the PM PAPI endpoint with idempotency guarantees.

        Every order carries a unique ``newClientOrderId`` and its intent is persisted
        BEFORE the POST. On a transient submit error (network timeout, 429, disconnect)
        the order is first resolved by that client order id; if the exchange actually
        accepted it, the existing order is returned instead of resubmitting, so retries
        can never duplicate an entry/adjust/exit. If both the submit and the lookup
        fail, the intent stays UNKNOWN in the persistent store and startup recovery
        re-resolves it after a process restart.
        """
        client_order_id = self._pm_new_client_order_id()
        merged = dict(params or {})
        merged["newClientOrderId"] = client_order_id
        request = self._pm_order_params(
            pair, ordertype, side, amount_contracts, rate_for_order, merged
        )
        self._pm_intent_put(
            client_order_id,
            {
                "kind": "order",
                "pair": pair,
                "side": side,
                "type": ordertype,
                "amount": amount_contracts,
                "price": rate_for_order,
                "ts": time.time(),
            },
        )
        try:
            raw_order = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/order", "POST", request
            )
            self._pm_intent_clear(client_order_id)
        except (TemporaryError, ccxt.ExchangeError) as e:
            try:
                existing = self._pm_fetch_order_by_client_id(client_order_id, pair)
            except (TemporaryError, ccxt.ExchangeError):
                # Both the submit AND the idempotency lookup failed transiently. The order
                # state is uncertain - never guess, and never silently resubmit.
                self._pm_intent_mark_uncertain(client_order_id, str(e))
                raise TemporaryError(
                    f"Binance PM order {client_order_id} submit failed and the idempotency "
                    "lookup also failed; order state is uncertain and the intent is "
                    "persisted as UNKNOWN. Startup recovery or /pm_recover will resolve it "
                    "before any new order is allowed."
                ) from e
            if existing is not None:
                self._pm_intent_clear(client_order_id)
                logger.warning(
                    f"Binance PM order {client_order_id} submit raised "
                    f"{e.__class__.__name__}; resolved the already-placed order "
                    f"{existing.get('id')} and returning it (no duplicate submitted)."
                )
                return existing
            self._pm_intent_clear(client_order_id)
            raise
        except ccxt.BaseError:
            # Definitive rejection (BadRequest/InsufficientFunds/InvalidOrder/...):
            # the exchange rejected the order, so it was never created.
            self._pm_intent_clear(client_order_id)
            raise
        self._log_exchange_response(log_tag, raw_order)
        return self._order_contracts_to_amount(self._parse_pm_order(raw_order, pair))

    @staticmethod
    def _pm_order_not_found(exc: Exception) -> bool:
        """True when Binance definitively reports the order does not exist."""
        if isinstance(exc, ccxt.OrderNotFound):
            return True
        return Binance._extract_binance_error_code(str(exc)) in {-2011, -2012, -2013, -4003}

    @staticmethod
    def _pm_raise_transient(operation: str, exc: Exception) -> None:
        if isinstance(exc, ccxt.DDoSProtection):
            raise DDosProtection(exc) from exc
        raise TemporaryError(
            f"Binance PM {operation} failed transiently: "
            f"{exc.__class__.__name__}. Message: {exc}"
        ) from exc

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
            "available_balance": self._pm_account_float(
                account,
                "availableBalance",
                "totalAvailableBalance",
                "totalCrossAvailableBalance",
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

    def _pm_entry_price_or_fail(self, pair: str) -> float:
        """
        Fetch a valid entry price for notional checks. Raises on missing/invalid price
        so notional caps can never be bypassed by a price of 0 (fail-closed).
        """
        try:
            price = self._float_or_none(self.get_rate(pair, side="entry", refresh=True))
        except Exception as e:
            raise OperationalException(
                f"Could not fetch entry price for {pair}; refusing to open a new order "
                f"(fail-closed). Error: {e}"
            ) from e
        if not price or price <= 0:
            raise OperationalException(
                f"Invalid entry price ({price}) for {pair}; refusing to open a new order "
                "(fail-closed)."
            )
        return price

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
        try:
            risk = self.get_pm_risk_summary()
        except Exception as e:
            raise OperationalException(
                "Binance PM risk check failed; refusing to open a new order (fail-closed). "
                f"Error: {e}"
            ) from e
        if not risk.get("enabled"):
            raise OperationalException(
                "Binance PM risk data unavailable; refusing to open a new order (fail-closed)."
            )

        account_status = risk.get("account_status")
        if not account_status:
            raise OperationalException(
                "Binance PM accountStatus is missing; refusing to open a new order (fail-closed)."
            )
        if account_status != "NORMAL":
            raise OperationalException(
                f"Binance PM account status is {account_status}; refusing to open a new order."
            )

        min_uni_mmr = risk_config.get("min_uni_mmr")
        uni_mmr = risk.get("uni_mmr")
        if min_uni_mmr is not None:
            if uni_mmr is None:
                raise OperationalException(
                    "Binance PM uniMMR is missing while min_uni_mmr is configured; "
                    "refusing to open a new order (fail-closed)."
                )
            if uni_mmr < float(min_uni_mmr):
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
            price = self._pm_entry_price_or_fail(pair)
            new_notional = amount * price
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
                price = self._pm_entry_price_or_fail(pair)
                new_notional = amount * price
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
        self._validate_pm_wallet_mode(risk_config)

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

    @staticmethod
    def _validate_pm_wallet_mode(risk_config: dict[str, Any]) -> None:
        wallet_mode = risk_config.get("wallet_mode", "USDT_ONLY")
        if wallet_mode not in {"USDT_ONLY", "PM_COLLATERAL_HAIRCUT"}:
            raise OperationalException(
                "exchange.portfolio_margin_risk.wallet_mode must be "
                "'USDT_ONLY' or 'PM_COLLATERAL_HAIRCUT'."
            )
        haircut = risk_config.get("collateral_haircut")
        if haircut is not None and not (0 < float(haircut) <= 1):
            raise OperationalException(
                "exchange.portfolio_margin_risk.collateral_haircut must be > 0 and <= 1."
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
        if not config.get("dry_run", True):
            # Live PM trading must fail fast on missing credentials or risk config.
            # NOTE: FreqtradeBot strips exchange.key/secret from the config before the
            # exchange is constructed, so credentials must be read from the ccxt API
            # object (which was built from the retained deep copy), not from config.
            exchange_conf = config.get("exchange", {})
            api_key = getattr(self._api, "apiKey", "") or ""
            api_secret = getattr(self._api, "secret", "") or ""
            if not api_key:
                raise OperationalException(
                    "Binance PM live trading requires exchange.key / exchange.secret "
                    "(or FREQTRADE__EXCHANGE__KEY / SECRET env vars). Refusing to start."
                )
            if not api_secret:
                raise OperationalException(
                    "Binance PM live trading requires exchange.secret. Refusing to start."
                )
            pm_risk = exchange_conf.get("portfolio_margin_risk")
            if not pm_risk or not isinstance(pm_risk, dict) or not pm_risk:
                raise OperationalException(
                    "Binance PM live trading requires a non-empty "
                    "exchange.portfolio_margin_risk config (at minimum min_uni_mmr, "
                    "monitor_interval_minutes). Refusing to start (fail-closed)."
                )
            if not config.get("db_url"):
                raise OperationalException(
                    "Binance PM live trading requires a persistent database (db_url) "
                    "for the durable order intent store (pm_order_intents table). "
                    "Refusing to start without database persistence (fail-closed)."
                )
        # Wallet model A: only USDT/USDC are usable as opening capital. BTC/ETH held as PM
        # collateral are deliberately NOT converted into available stake, so collateral can
        # never be double-counted into balance, position margin and available funds.
        stake_currency = config.get("stake_currency")
        if stake_currency not in {"USDT", "USDC"}:
            raise OperationalException(
                "Binance PM wallet model requires stake_currency to be USDT or USDC. "
                "BTC/ETH PM collateral is not used as available opening capital. "
                f"Current stake_currency: {stake_currency}."
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
        # Fail-closed idempotency gate: any unresolved intent in the durable store
        # blocks exposure-increasing orders. reduceOnly exits / emergency closes are
        # NEVER blocked by this gate.
        if not reduceOnly and self.pm_has_unresolved_intents():
            raise TemporaryError(
                "Binance PM has unresolved order intents in the database; refusing to "
                "place a new exposure-increasing order (fail-closed). Resolve the "
                "pending intents via /pm_recover."
            )
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
            return self._pm_place_order(
                pair,
                ordertype,
                side,
                amount_contracts,
                rate_for_order,
                params,
                log_tag="papi_create_order",
            )
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
            request: dict[str, Any] = {
                "symbol": self._pm_symbol_for_pair(pair),
                "orderId": order_id,
            }
            # Strip framework-only params (e.g. `stop` added by stoploss_query_requires_stop_flag)
            # that PAPI does not accept.
            pm_params = dict(params or {})
            pm_params.pop("stop", None)
            request.update(pm_params)
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
            # PAPI regular order cancel only accepts symbol/orderId|origClientOrderId/recvWindow.
            # Strip framework-only params (e.g. `stop` added for stoploss flows) so regular
            # cancels never fail; conditional stoploss cancels route through
            # cancel_stoploss_order instead.
            pm_params = dict(params or {})
            pm_params.pop("stop", None)
            request.update(pm_params)
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

    @retrier(retries=0)
    def _fetch_orders(
        self, pair: str, since: datetime, params: dict | None = None
    ) -> list[CcxtOrder]:
        """
        Fetch all orders for a pair "since" through the PM PAPI order history endpoint.

        PM mode must never fall back to the generic CCXT futures fetch_orders().
        :param pair: Pair for the query
        :param since: Starting time for the query
        """
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return super()._fetch_orders(pair, since, params)
        try:
            since_ms = int((since.timestamp() - 10) * 1000)
            request: dict[str, Any] = {
                "symbol": self._pm_symbol_for_pair(pair),
                "startTime": since_ms,
                "limit": 1000,
            }
            request.update(params or {})
            raw_orders = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/allOrders", "GET", request
            )
            self._log_exchange_response("papi_all_orders", raw_orders)
            orders = [
                self._order_contracts_to_amount(self._parse_pm_order(raw_order, pair))
                for raw_order in raw_orders
            ]
            return orders
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get Binance PM orders due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier(retries=0)
    def fetch_open_orders(
        self, pair: str | None = None, since: datetime | None = None, params: dict | None = None
    ) -> list[CcxtOrder]:
        """
        Fetch currently open orders through the PM PAPI endpoint.

        PM mode must never use the generic CCXT futures fetch_open_orders().
        :param pair: Pair for the query (None => all pairs)
        """
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return self._api.fetch_open_orders(pair, params=params or {})
        try:
            request: dict[str, Any] = dict(params or {})
            if pair is not None:
                request["symbol"] = self._pm_symbol_for_pair(pair)
            raw_orders = self._papi_request(
                f"{self._pm_namespace_for_pair(pair or list(self.markets.keys())[0])}/openOrders",
                "GET",
                request,
            )
            self._log_exchange_response("papi_open_orders", raw_orders)
            return [
                self._order_contracts_to_amount(self._parse_pm_order(raw_order, pair))
                for raw_order in raw_orders
            ]
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get Binance PM open orders due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def get_trades_for_order(
        self, order_id: str, pair: str, since: datetime, params: dict | None = None
    ) -> list:
        """
        Fetch executed trades for an order through the PM PAPI userTrades endpoint.

        PM mode must never use the generic CCXT futures fetch_my_trades() (FAPI).
        """
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return super().get_trades_for_order(order_id, pair, since, params)
        try:
            since_ms = int((since.replace(tzinfo=UTC).timestamp() - 5) * 1000)
            request: dict[str, Any] = {
                "symbol": self._pm_symbol_for_pair(pair),
                "startTime": since_ms,
                "limit": 1000,
            }
            request.update(params or {})
            raw_trades = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/userTrades", "GET", request
            )
            self._log_exchange_response("papi_user_trades", raw_trades)
            trades = [self._parse_pm_trade(raw_trade, pair) for raw_trade in raw_trades]
            matched_trades = [trade for trade in trades if trade["order"] == order_id]
            return self._trades_contracts_to_amount(matched_trades)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get Binance PM trades due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def _pm_conditional_lookup_params(self, order_id: str) -> dict[str, Any]:
        """
        Build the lookup params for a PM conditional order.

        freqtrade stores ``order["id"]`` and passes it back for fetch/cancel. Conditional
        orders are identified by either the exchange ``strategyId`` (numeric) or the client
        ``newClientStrategyId`` we generated at creation time.
        """
        if str(order_id).isdigit():
            return {"strategyId": int(order_id)}
        return {"newClientStrategyId": order_id}

    def create_stoploss(
        self,
        pair: str,
        amount: float,
        stop_price: float,
        order_types: dict,
        side: BuySell,
        leverage: float,
    ) -> CcxtOrder:
        """
        Creates a stoploss order through the PM PAPI conditional order endpoint.

        Binance PM defines STOP/STOP_MARKET as conditional orders with their own lifecycle
        (``strategyType`` + ``newClientStrategyId``) on ``/papi/v1/um/conditional/order``,
        NOT as regular ``/papi/v1/um/order`` orders.
        """
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return super().create_stoploss(
                pair=pair,
                amount=amount,
                stop_price=stop_price,
                order_types=order_types,
                side=side,
                leverage=leverage,
            )

        user_order_type = order_types.get("stoploss", "market")
        ordertype, user_order_type = self._get_stop_order_type(user_order_type)
        # freqtrade futures mapping: market -> "stop_market", limit -> "stop".
        strategy_type = "STOP_MARKET" if ordertype == "stop_market" else "STOP"

        # Fail-closed idempotency gate: with unresolved intents a second strategy id
        # must never be generated (a previous stoploss POST may have succeeded).
        if self.pm_has_unresolved_intents():
            raise TemporaryError(
                "Binance PM has unresolved order intents in the database; refusing to "
                "generate a new client strategy id / place a stoploss order "
                "(fail-closed). Resolve the pending intents via /pm_recover."
            )

        round_mode = ROUND_DOWN if side == "buy" else ROUND_UP
        stop_price_norm = self.price_to_precision(pair, stop_price, rounding_mode=round_mode)
        limit_rate = None
        if user_order_type == "limit":
            limit_rate = self._get_stop_limit_rate(stop_price, order_types, side)
            limit_rate = self.price_to_precision(pair, limit_rate, rounding_mode=round_mode)

        working_type = "CONTRACT_PRICE"
        if "stoploss_price_type" in order_types and "stop_price_type_field" in self._ft_has:
            working_type = self._ft_has["stop_price_type_value_mapping"][
                order_types.get("stoploss_price_type", PriceType.LAST)
            ]

        client_strategy_id = self._pm_new_client_strategy_id()
        request: dict[str, Any] = {
            "symbol": self._pm_symbol_for_pair(pair),
            "side": side.upper(),
            "strategyType": strategy_type,
            "reduceOnly": "true",
            "stopPrice": stop_price_norm,
            "workingType": working_type,
            "newClientStrategyId": client_strategy_id,
        }
        if limit_rate is not None:
            request["price"] = limit_rate

        # Persist the intent BEFORE the POST so a timeout/restart can never create a
        # duplicate conditional stoploss for the same position.
        self._pm_intent_put(
            client_strategy_id,
            {
                "kind": "conditional",
                "pair": pair,
                "side": side,
                "type": strategy_type,
                "amount": amount,
                "stop_price": stop_price_norm,
                "price": limit_rate,
                "reduce_only": True,
                "ts": time.time(),
            },
        )
        try:
            amount_contracts = self.amount_to_precision(
                pair, self._amount_to_contracts(pair, amount)
            )
            request["quantity"] = amount_contracts
            self._lev_prep(pair, leverage, side, accept_fail=True)
            raw_order = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/conditional/order", "POST", request
            )
            self._pm_intent_clear(client_strategy_id)
            self._log_exchange_response("papi_create_stoploss", raw_order)
            order = self._parse_pm_conditional_order(raw_order, pair)
            logger.info(
                f"stoploss {user_order_type} ({strategy_type}) order added for {pair} "
                f"through PM PAPI conditional endpoint. stop price: {stop_price}. "
                f"limit: {limit_rate}."
            )
            return order
        except ccxt.InsufficientFunds as e:
            self._pm_intent_clear(client_strategy_id)
            raise InsufficientFundsError(
                f"Insufficient funds to create {strategy_type} {side} PM stoploss order on "
                f"{pair}. Tried to {side} amount {amount} at rate {limit_rate} with "
                f"stop-price {stop_price_norm}. Message: {e}"
            ) from e
        except (ccxt.InvalidOrder, ccxt.BadRequest, ccxt.OperationRejected) as e:
            self._pm_intent_clear(client_strategy_id)
            raise InvalidOrderException(
                f"Could not create {strategy_type} {side} PM stoploss order on market {pair}. "
                f"Tried to {side} amount {amount} at rate {limit_rate} with "
                f"stop-price {stop_price_norm}. Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            existing = self._pm_stoploss_resolve_after_transient(client_strategy_id, pair, str(e))
            if existing is not None:
                return existing
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            existing = self._pm_stoploss_resolve_after_transient(client_strategy_id, pair, str(e))
            if existing is not None:
                return existing
            raise TemporaryError(
                f"Could not place Binance PM stoploss order due to {e.__class__.__name__}. "
                f"Message: {e}. The intent is persisted as UNKNOWN and will be resolved "
                "before any new order is allowed."
            ) from e
        except ccxt.BaseError as e:
            self._pm_intent_mark_uncertain(client_strategy_id, str(e))
            raise OperationalException(e) from e

    def _pm_stoploss_resolve_after_transient(
        self, client_strategy_id: str, pair: str, error: str
    ) -> CcxtOrder | None:
        """
        Resolve a conditional stoploss after a transient POST failure using the SAME
        ``newClientStrategyId`` (openOrder -> orderHistory -> triggered real order).

        * Order exists on the exchange -> clear the intent and RETURN it (the caller
          writes it into the local trade). No duplicate strategy id is ever created.
        * Definitively absent -> clear the intent (safe to retry on a later loop).
        * Lookup also failed -> the intent stays UNKNOWN (block remains) and None is
          returned so the caller raises. Never guesses.
        """
        try:
            existing = self.fetch_stoploss_order(client_strategy_id, pair)
        except InvalidOrderException:
            # Exchange definitively has no such strategy -> it was never placed.
            self._pm_intent_clear(client_strategy_id)
            return None
        except Exception as lookup_error:
            # Uncertain: keep the UNKNOWN intent (block) - never resubmit.
            self._pm_intent_mark_uncertain(client_strategy_id, error)
            logger.warning(
                f"Binance PM stoploss {client_strategy_id} POST raised {error} and the "
                f"same-id lookup also failed ({lookup_error}); intent kept UNKNOWN."
            )
            return None
        self._pm_intent_clear(client_strategy_id)
        logger.warning(
            f"Binance PM stoploss {client_strategy_id} POST raised {error}; resolved the "
            f"already-placed conditional order {existing.get('id')} and returning it "
            "(no duplicate submitted)."
        )
        return existing

    @retrier(retries=0)
    def fetch_stoploss_order(
        self, order_id: str, pair: str, params: dict | None = None
    ) -> CcxtOrder:
        """
        Fetch a PM conditional (stoploss) order with full lifecycle resolution.

        * ``NEW`` strategies live on ``/um/conditional/openOrder``.
        * Triggered/cancelled/expired strategies fall back to
          ``/um/conditional/orderHistory``.
        * A TRIGGERED strategy reports the REAL order id in ``orderId``; the real
          order is fetched through ``/um/order`` so filled/average/cost/fee/trades
          are authoritative - the strategy stays ``open`` until the real order
          actually fills. No fill data is ever fabricated from the strategy record.

        Fail-closed: transient failures propagate (TemporaryError) so the caller
        aborts the iteration instead of assuming the stoploss is gone and placing a
        duplicate one. InvalidOrderException is raised ONLY when the exchange
        definitively reports the strategy no longer exists.
        """
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return super().fetch_stoploss_order(order_id, pair, params)
        request: dict[str, Any] = {"symbol": self._pm_symbol_for_pair(pair)}
        request.update(self._pm_conditional_lookup_params(order_id))
        request.update(params or {})
        try:
            raw_order = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/conditional/openOrder", "GET", request
            )
            self._log_exchange_response("papi_fetch_stoploss_order", raw_order)
            return self._parse_pm_conditional_order(raw_order, pair)
        except ccxt.BaseError as e:
            if not self._pm_order_not_found(e):
                # Transient error -> propagate; the caller must not treat the
                # stoploss as absent and must not create a duplicate.
                self._pm_raise_transient("fetch_stoploss_order", e)
            # Definitive "not open": fall back to the conditional history.
            try:
                raw_order = self._papi_request(
                    f"{self._pm_namespace_for_pair(pair)}/conditional/orderHistory",
                    "GET",
                    request,
                )
                self._log_exchange_response("papi_fetch_stoploss_history", raw_order)
            except ccxt.BaseError as history_exc:
                if self._pm_order_not_found(history_exc):
                    raise InvalidOrderException(
                        f"Binance PM conditional order {order_id} on {pair} does not exist "
                        "(not open, not in history)."
                    ) from history_exc
                self._pm_raise_transient("fetch_stoploss_history", history_exc)
            order = self._parse_pm_conditional_order(raw_order, pair)
            # TRIGGERED/FINISHED with a real order id -> resolve the real order.
            strategy_status = str(order.get("info", {}).get("strategyStatus") or "").upper()
            if order.get("info", {}).get("orderId") and strategy_status in {
                "TRIGGERED",
                "FINISHED",
            }:
                return self._pm_resolve_conditional_actual_order(order, pair)
            return order

    @retrier(retries=0)
    def fetch_open_conditional_orders(self, pair: str | None = None) -> list[CcxtOrder]:
        """
        Fetch currently open PM conditional (stoploss) orders via
        ``/um/conditional/openOrders``. Used by the startup consistency check.
        """
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return []
        request: dict[str, Any] = {}
        if pair is not None:
            request["symbol"] = self._pm_symbol_for_pair(pair)
        try:
            raw_orders = self._papi_request(
                f"{self._pm_namespace_for_pair(pair or list(self.markets.keys())[0])}"
                "/conditional/openOrders",
                "GET",
                request,
            )
            self._log_exchange_response("papi_open_conditional_orders", raw_orders)
            return [
                self._parse_pm_conditional_order(raw_order, pair)
                for raw_order in raw_orders
            ]
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get Binance PM open conditional orders due to "
                f"{e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def cancel_stoploss_order(
        self, order_id: str, pair: str, params: dict | None = None
    ) -> dict:
        """
        Cancel a PM conditional (stoploss) order via ``/um/conditional/order`` DELETE.
        """
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return super().cancel_stoploss_order(order_id, pair, params)
        request: dict[str, Any] = {"symbol": self._pm_symbol_for_pair(pair)}
        request.update(self._pm_conditional_lookup_params(order_id))
        request.update(params or {})
        try:
            raw_order = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/conditional/order", "DELETE", request
            )
            self._log_exchange_response("papi_cancel_stoploss", raw_order)
            return self._parse_pm_conditional_order(raw_order, pair)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not cancel Binance PM stoploss order due to {e.__class__.__name__}. "
                f"Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def _get_funding_fees_from_exchange(self, pair: str, since: datetime | int) -> float:
        """
        Sum funding fees for a PM pair through the PAPI ``/um/income`` endpoint.

        PM private flows must not use the generic ccxt FAPI ``fetch_funding_history()``.
        """
        if not self._is_portfolio_margin() or self._config["dry_run"]:
            return super()._get_funding_fees_from_exchange(pair, since)

        if type(since) is datetime:
            since = dt_ts(since)
        try:
            raw_income = self._papi_request(
                f"{self._pm_namespace_for_pair(pair)}/income",
                "GET",
                {
                    "symbol": self._pm_symbol_for_pair(pair),
                    "incomeType": "FUNDING_FEE",
                    "startTime": int(since),
                    "limit": 1000,
                },
            )
            self._log_exchange_response(
                "papi_funding_fees", raw_income, add_info=f"pair: {pair}, since: {since}"
            )
            return sum(self._float_or_zero(entry.get("income")) for entry in raw_income)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get Binance PM funding fees due to {e.__class__.__name__}. Message: {e}"
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
            if self._config["dry_run"] or self._is_portfolio_margin():
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
