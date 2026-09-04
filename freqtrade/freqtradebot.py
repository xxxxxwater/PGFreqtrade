"""
Freqtrade is the main module of this bot. It contains the FreqtradeBot class.
"""

import logging
import traceback
from hashlib import sha256
from copy import deepcopy
from datetime import UTC, datetime, time, timedelta
from math import isclose
from threading import Lock
from time import sleep
from typing import Any

from schedule import Scheduler
from sqlalchemy import func, select

import pandas as pd
from pandas import DataFrame

from freqtrade import constants
from freqtrade.configuration import remove_exchange_credentials, validate_config_consistency
from freqtrade.constants import BuySell, Config, EntryExecuteMode, ExchangeConfig, LongShort
from freqtrade.data.converter import order_book_to_dataframe
from freqtrade.data.dataprovider import DataProvider
from freqtrade.enums import (
    ExitCheckTuple,
    ExitType,
    MarginMode,
    RPCMessageType,
    SignalDirection,
    State,
    TradingMode,
)
from freqtrade.exceptions import (
    DependencyException,
    ExchangeError,
    InsufficientFundsError,
    InvalidOrderException,
    OperationalException,
    PricingError,
)
from freqtrade.exchange import (
    ROUND_DOWN,
    ROUND_UP,
    timeframe_to_minutes,
    timeframe_to_next_date,
    timeframe_to_prev_date,
    timeframe_to_seconds,
)
from freqtrade.exchange.exchange_types import CcxtOrder
from freqtrade.leverage.liquidation_price import update_liquidation_prices
from freqtrade.misc import safe_value_fallback, safe_value_fallback2
from freqtrade.mixins import LoggingMixin
from freqtrade.persistence import Order, PairLocks, Trade, init_db
from freqtrade.persistence.key_value_store import set_startup_time
from freqtrade.persistence.pm_order_intent import PMOrderIntent
from freqtrade.plugins.pairlistmanager import PairListManager
from freqtrade.plugins.protectionmanager import ProtectionManager
from freqtrade.resolvers import ExchangeResolver, StrategyResolver
from freqtrade.rpc import RPCManager
from freqtrade.rpc.external_message_consumer import ExternalMessageConsumer
from freqtrade.rpc.rpc_types import (
    ProfitLossStr,
    RPCCancelMsg,
    RPCEntryMsg,
    RPCExitCancelMsg,
    RPCExitMsg,
    RPCProtectionMsg,
)
from freqtrade.strategy.interface import IStrategy
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.util import FtPrecise, MeasureTime, PeriodicCache, dt_from_ts, dt_now
from freqtrade.util.migrations import migrate_live_content
from freqtrade.wallets import Wallets


logger = logging.getLogger(__name__)


class FreqtradeBot(LoggingMixin):
    """
    Freqtrade is the main class of the bot.
    This is from here the bot start its logic.
    """

    def __init__(self, config: Config) -> None:
        """
        Init all variables and objects the bot needs to work
        :param config: configuration dict, you can use Configuration.get_config()
        to get the config dict.
        """
        self.active_pair_whitelist: list[str] = []

        # Init bot state
        self.state = State.STOPPED

        # Init objects
        self.config = config
        exchange_config: ExchangeConfig = deepcopy(config["exchange"])
        # Remove credentials from original exchange config to avoid accidental credential exposure
        remove_exchange_credentials(config["exchange"], True)

        self.exchange = ExchangeResolver.load_exchange(
            self.config, exchange_config=exchange_config, load_leverage_tiers=True
        )

        self.strategy: IStrategy = StrategyResolver.load_strategy(self.config)

        # Check config consistency here since strategies can set certain options
        validate_config_consistency(config)
        # Re-validate exchange compatibility
        self.exchange.validate_config(self.config)

        init_db(self.config["db_url"])

        self.wallets = Wallets(self.config, self.exchange)

        PairLocks.timeframe = self.config["timeframe"]

        self.trading_mode: TradingMode = self.config.get("trading_mode", TradingMode.SPOT)
        self.margin_mode: MarginMode = self.config.get("margin_mode", MarginMode.NONE)
        self.last_process: datetime | None = None

        # RPC runs in separate threads, can start handling external commands just after
        # initialization, even before Freqtradebot has a chance to start its throttling,
        # so anything in the Freqtradebot instance should be ready (initialized), including
        # the initial state of the bot.
        # Keep this at the end of this initialization method.
        self.rpc: RPCManager = RPCManager(self)

        self.dataprovider = DataProvider(self.config, self.exchange, rpc=self.rpc)
        self.pairlists = PairListManager(self.exchange, self.config, self.dataprovider)

        self.dataprovider.add_pairlisthandler(self.pairlists)

        # Attach Dataprovider to strategy instance
        self.strategy.dp = self.dataprovider
        # Attach Wallets to strategy instance
        self.strategy.wallets = self.wallets

        # Init ExternalMessageConsumer if enabled
        self.emc = (
            ExternalMessageConsumer(self.config, self.dataprovider)
            if self.config.get("external_message_consumer", {}).get("enabled", False)
            else None
        )

        logger.info("Starting initial pairlist refresh")
        with MeasureTime(
            lambda duration, _: logger.info(f"Initial Pairlist refresh took {duration:.2f}s"), 0
        ):
            self.active_pair_whitelist = self._refresh_active_whitelist()

        # Set initial bot state from config
        initial_state = self.config.get("initial_state")
        self.state = State[initial_state.upper()] if initial_state else State.STOPPED

        # Protect exit-logic from forcesell and vice versa
        self._exit_lock = Lock()
        timeframe_secs = timeframe_to_seconds(self.strategy.timeframe)
        self._exit_reason_cache = PeriodicCache(100, ttl=timeframe_secs)
        LoggingMixin.__init__(self, logger, timeframe_secs)

        self._schedule = Scheduler()

        if self.trading_mode == TradingMode.FUTURES:

            def update():
                self.update_funding_fees()
                self.update_all_liquidation_prices()
                self.wallets.update()

            # This would be more efficient if scheduled in utc time, and performed at each
            # funding interval, specified by funding_fee_times on the exchange classes
            # However, this reduces the precision - and might therefore lead to problems.
            for time_slot in range(0, 24):
                for minutes in [1, 31]:
                    t = str(time(time_slot, minutes, 2))
                    self._schedule.every().day.at(t).do(update)

        self._schedule.every().day.at("00:02").do(self.exchange.ws_connection_reset)
        if hasattr(self.wallets, "record_wallet_state"):
            self._schedule.every().day.at("00:07").do(self.wallets.record_wallet_state)

        if (
            self.trading_mode == TradingMode.FUTURES
            and not self.config["dry_run"]
            and getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
        ):
            self._pm_listen_key: str = ""
            self._pm_init_user_stream_state()
            self._init_pm_schedule()

        self.strategy.ft_bot_start()
        # Initialize protections AFTER bot start - otherwise parameters are not loaded.
        self.protections = ProtectionManager(self.config, self.strategy.protections)

        def log_took_too_long(duration: float, time_limit: float):
            logger.warning(
                f"Strategy analysis took {duration:.2f}s, more than 25% of the timeframe "
                f"({time_limit:.2f}s). This can lead to delayed orders and missed signals."
                "Consider either reducing the amount of work your strategy performs "
                "or reduce the amount of pairs in the Pairlist."
            )

        self._measure_execution = MeasureTime(log_took_too_long, timeframe_secs * 0.25)

    def notify_status(self, msg: str, msg_type=RPCMessageType.STATUS) -> None:
        """
        Public method for users of this class (worker, etc.) to send notifications
        via RPC about changes in the bot status.
        """
        self.rpc.send_msg({"type": msg_type, "status": msg})

    def _init_pm_schedule(self) -> None:
        if not hasattr(self, "_pm_listen_key"):
            self._pm_listen_key = ""
        self._pm_create_listen_key()

        pm_risk_cfg = self.config.get("exchange", {}).get("portfolio_margin_risk", {})
        # Binance recommends renewing a listenKey about every 30 minutes.  Keep a
        # little operational margin so one delayed worker loop cannot put a live
        # stream close to its expiry window.
        keepalive_interval = int(
            pm_risk_cfg.get("user_stream_listen_key_keepalive_minutes", 20)
        )
        self._schedule.every(keepalive_interval).minutes.do(self._pm_keepalive_listen_key)

        risk_interval = pm_risk_cfg.get("monitor_interval_minutes", 5)
        self._schedule.every(risk_interval).minutes.do(self._pm_risk_monitor)

        recovery_interval = pm_risk_cfg.get("order_recovery_interval_minutes", 5)
        self._schedule.every(recovery_interval).minutes.do(self._pm_order_recovery)

        health_interval = pm_risk_cfg.get("user_stream_health_interval_minutes", 1)
        self._schedule.every(health_interval).minutes.do(self._pm_user_stream_health_monitor)

        logger.info(
            f"Binance PM scheduled tasks registered: listenKey keepalive "
            f"({keepalive_interval}min), "
            f"risk monitor ({risk_interval}min), "
            f"order recovery ({recovery_interval}min), "
            f"user stream health ({health_interval}min)"
        )

    def _pm_create_listen_key(self) -> None:
        try:
            if hasattr(self.exchange, "create_pm_listen_key"):
                self._pm_listen_key = self.exchange.create_pm_listen_key()
                if self._pm_listen_key:
                    logger.info("Binance PM listenKey created successfully.")
                    self._pm_unblock_orders("listen_key_failed")
                    if hasattr(self.exchange, "start_pm_user_stream"):
                        self.exchange.start_pm_user_stream(self._pm_listen_key)
                else:
                    logger.warning("Binance PM listenKey creation returned an empty key.")
                    if self._pm_stream_fail_closed():
                        self._pm_block_orders("listen_key_failed")
                        self.rpc.send_msg(
                            {
                                "type": RPCMessageType.WARNING,
                                "status": (
                                    "PM FAIL-CLOSED: listenKey creation returned no key. "
                                    "New orders BLOCKED until the user stream is restored."
                                ),
                            }
                        )
        except Exception as e:
            logger.warning(f"Failed to create Binance PM listenKey: {e}")
            if self._pm_stream_fail_closed():
                self._pm_block_orders("listen_key_failed")
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM FAIL-CLOSED: listenKey creation failed: {e}. "
                            "New orders BLOCKED until the user stream is restored."
                        ),
                    }
                )

    def _pm_stream_fail_closed(self) -> bool:
        """Whether a broken user stream should block new orders (fail-closed)."""
        risk_cfg = self.config.get("exchange", {}).get("portfolio_margin_risk", {})
        if risk_cfg.get("user_stream_fail_closed", True):
            return True
        # Only a non-fail-closed config with explicit degraded REST mode allows trading.
        return not risk_cfg.get("allow_degraded_rest_recovery", False)

    def _pm_keepalive_listen_key(self) -> None:
        if not self._pm_listen_key:
            return
        try:
            if hasattr(self.exchange, "keepalive_pm_listen_key"):
                self.exchange.keepalive_pm_listen_key(self._pm_listen_key)
                self._pm_last_listen_key_keepalive_at = datetime.now(UTC)
                logger.info("Binance PM listenKey keepalive succeeded.")
        except Exception as e:
            logger.warning(f"Failed to keepalive Binance PM listenKey: {e}")
            if hasattr(self.exchange, "stop_pm_user_stream"):
                self.exchange.stop_pm_user_stream()
            self._pm_listen_key = ""
            self._pm_create_listen_key()

    def _pm_init_user_stream_state(self) -> None:
        if not hasattr(self, "_pm_user_stream_restarts"):
            self._pm_user_stream_restarts: list[datetime] = []
        if not hasattr(self, "_pm_last_listen_key_rebuild_at"):
            self._pm_last_listen_key_rebuild_at: datetime | None = None
        if not hasattr(self, "_pm_last_listen_key_keepalive_at"):
            self._pm_last_listen_key_keepalive_at: datetime | None = None
        if not hasattr(self, "_pm_user_stream_last_events_dropped"):
            self._pm_user_stream_last_events_dropped = 0
        if not hasattr(self, "_pm_user_stream_last_parse_errors"):
            self._pm_user_stream_last_parse_errors = 0
        if not hasattr(self, "_pm_user_stream_queue_warning_active"):
            self._pm_user_stream_queue_warning_active = False
        if not hasattr(self, "_pm_user_stream_restart_limit_warning_sent"):
            self._pm_user_stream_restart_limit_warning_sent = False
        if not hasattr(self, "_pm_unmatched_stream_order_ids"):
            self._pm_unmatched_stream_order_ids: set[str] = set()
        if not hasattr(self, "_pm_foreign_stream_order_events"):
            # Foreign exchange orders remain outside the strategy database.  Keep a
            # bounded id/status cache solely to avoid flooding logs when Binance
            # retransmits an external manual-order event.
            self._pm_foreign_stream_order_events: set[str] = set()
        if not hasattr(self, "_pm_actual_order_map"):
            # Real order id (after conditional trigger) -> local stoploss order id.
            # Populated whenever fetch_stoploss_order resolves an actual order.
            self._pm_actual_order_map: dict[str, str] = {}
        if not hasattr(self, "_pm_orders_blocked_reasons"):
            self._pm_orders_blocked_reasons: list[str] = []
        if not hasattr(self, "_pm_unresolved_alert_sent"):
            self._pm_unresolved_alert_sent = False
        if not hasattr(self, "_pm_last_success_reconcile_time"):
            self._pm_last_success_reconcile_time: datetime | None = None
        if not hasattr(self, "_pm_last_reconcile_result"):
            self._pm_last_reconcile_result: dict[str, Any] | None = None
        if not hasattr(self, "_pm_risk_api_failure_sent"):
            self._pm_risk_api_failure_sent = False
        if not hasattr(self, "_pm_user_stream_state"):
            # SYNCING -> HEALTHY | DEGRADED | FAILED
            self._pm_user_stream_state = "SYNCING"
            # Fail-closed from the very first moment: block new orders until the first
            # health evaluation confirms the stream is HEALTHY (not just "not yet checked").
            if self._pm_stream_fail_closed() and not self.config.get("dry_run", True):
                if "user_stream_unavailable" not in self._pm_orders_blocked_reasons:
                    self._pm_orders_blocked_reasons.append("user_stream_unavailable")

    def _pm_get_user_stream_state(self) -> str:
        """Explicit PM user stream health state (SYNCING/HEALTHY/DEGRADED/FAILED)."""
        return getattr(self, "_pm_user_stream_state", "SYNCING")

    def _pm_block_orders(self, reason: str) -> None:
        """Block new non-reduce-only PM orders for a given reason (fail-closed)."""
        self._pm_init_user_stream_state()
        if reason not in self._pm_orders_blocked_reasons:
            self._pm_orders_blocked_reasons.append(reason)
            logger.warning("PM new-order gate set: reason=%s.", reason)

    def _pm_unblock_orders(self, reason: str) -> None:
        """Remove a previously set order-blocking reason."""
        self._pm_init_user_stream_state()
        if reason in self._pm_orders_blocked_reasons:
            self._pm_orders_blocked_reasons.remove(reason)
            logger.info("PM new-order gate cleared: reason=%s.", reason)

    def _pm_blocked_order_reasons(self) -> list[str]:
        """
        Reasons that currently block opening new PM orders (empty list = allowed).

        In live PM mode this ALWAYS consults the authoritative durable intent store:
        any PENDING/UNKNOWN intent blocks entries (the memory list alone is not
        sufficient). An unreadable store is itself a blocking reason (fail-closed).
        """
        reasons = list(getattr(self, "_pm_orders_blocked_reasons", []))
        if self._pm_db_gate_active():
            self._pm_init_user_stream_state()
            try:
                if PMOrderIntent.has_unresolved():
                    if "unresolved_intent" not in reasons:
                        reasons.append("unresolved_intent")
                    if not self._pm_unresolved_alert_sent:
                        self._pm_unresolved_alert_sent = True
                        try:
                            self.rpc.send_msg(
                                {
                                    "type": RPCMessageType.WARNING,
                                    "status": (
                                        "PM FAIL-CLOSED: unresolved order intents exist in "
                                        "the database. New exposure-increasing orders are "
                                        "BLOCKED until /pm_recover resolves them."
                                    ),
                                }
                            )
                        except Exception:
                            pass
                else:
                    self._pm_unresolved_alert_sent = False
            except Exception:
                if "intent_store_unavailable" not in reasons:
                    reasons.append("intent_store_unavailable")
        return reasons

    def _pm_db_gate_active(self) -> bool:
        """Whether the durable-intent DB gate applies (live PM only)."""
        return (
            getattr(self, "trading_mode", None) == TradingMode.FUTURES
            and not self.config.get("dry_run", True)
            and getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
        )

    def _pm_has_unresolved_intents(self) -> bool:
        """Authoritative unresolved-intent check (store failure => True, fail-closed)."""
        try:
            return PMOrderIntent.has_unresolved()
        except Exception:
            return True

    def _pm_unresolved_intent_count(self) -> int:
        try:
            return len(PMOrderIntent.get_unresolved())
        except Exception:
            return -1

    def _pm_risk_failure_action(self) -> str:
        """Configured action when the PM risk API fails: warn (default) | pause | stop."""
        action = (
            self.config.get("exchange", {})
            .get("portfolio_margin_risk", {})
            .get("risk_api_failure_action", "warn")
        )
        return str(action).lower() if action in {"warn", "pause", "stop"} else "warn"

    def _pm_pair_from_exchange_symbol(self, symbol_id: str | None) -> str | None:
        if not symbol_id:
            return None

        for pair, market in self.exchange.markets.items():
            if market.get("id") == symbol_id:
                return pair

        api = getattr(self.exchange, "_api", None)
        if api and hasattr(api, "safe_symbol"):
            try:
                pair = api.safe_symbol(symbol_id, None, None, "contract")
                if pair in self.exchange.markets:
                    return pair
            except Exception:
                logger.debug(f"Could not map PM stream symbol {symbol_id} to a freqtrade pair.")
        return None

    def _pm_handle_order_trade_update(
        self, event: dict[str, Any], order_index: dict[str, tuple[Any, Any]]
    ) -> bool:
        order_data = event.get("o", {})
        order_id = str(order_data.get("i") or order_data.get("orderId") or "")
        client_order_id = str(order_data.get("c") or "")
        event_order_id = order_id or client_order_id
        if not event_order_id:
            return False

        pair = self._pm_pair_from_exchange_symbol(order_data.get("s"))
        if not pair:
            logger.debug(f"Skipping PM order event for unknown symbol {order_data.get('s')}.")
            return False

        # A PM conditional order may announce its generated real ``orderId`` in
        # ``i`` while keeping our ``newClientStrategyId`` in ``c``.  Stoploss
        # orders are stored locally under the strategy id, so consult both ids
        # before treating the event as an unmatched order.
        entry = order_index.get(order_id) or order_index.get(client_order_id)
        if entry is None:
            return self._pm_handle_unmatched_order_trade_update(event_order_id, order_data, pair)

        trade, order = entry
        # Stoploss orders are PM conditional orders: their lifecycle must be resolved
        # through the conditional endpoints (and the triggered real order), never
        # through a plain /um/order fetch with the strategy id.
        if order.ft_order_side == "stoploss":
            with self._exit_lock:
                strategy_id = str(order.order_id)
                try:
                    exchange_order = self.exchange.fetch_stoploss_order(strategy_id, trade.pair)
                except InvalidOrderException:
                    # The conditional is definitively gone (triggered or
                    # canceled). Mark it canceled locally - never re-raise:
                    # a redelivered event must not fail the batch repeatedly.
                    logger.warning(
                        f"PM user stream: stoploss {strategy_id} on {trade.pair} no longer "
                        "exists on the exchange; marking canceled locally."
                    )
                    order.ft_is_open = False
                    order.status = "canceled"
                    return True
                self._pm_record_actual_order(exchange_order, strategy_id)
                self.update_trade_state(
                    trade,
                    strategy_id,
                    exchange_order,
                    stoploss_order=True,
                )
        else:
            with self._exit_lock:
                exchange_order = self.exchange.fetch_order(order_id, trade.pair)
                self.update_trade_state(
                    trade,
                    order_id,
                    exchange_order,
                    stoploss_order=False,
                )
        logger.info(
            f"PM user stream reconciled order {event_order_id} on {trade.pair}: "
            f"{exchange_order.get('status')}."
        )
        return True

    def _pm_record_actual_order(self, exchange_order: CcxtOrder | dict, strategy_id: str) -> None:
        """Remember the real order id behind a triggered conditional strategy."""
        self._pm_init_user_stream_state()
        actual_id = exchange_order.get("id_stop")
        if actual_id:
            self._pm_actual_order_map[str(actual_id)] = strategy_id

    def _pm_handle_unmatched_order_trade_update(
        self, order_id: str, order_data: dict[str, Any], pair: str
    ) -> bool:
        """
        Handle a user-stream order event that did not match a local open order id.

        Fail-closed resolution order:
        1. The real order of a triggered local conditional stoploss (actualOrderId map)
           -> reconcile through the conditional lifecycle.
        2. An order carrying one of OUR client ids (ft*/st* prefixes) -> we own it but
           lost track -> block new orders + full recovery + alert.
        3. A foreign order (no client id, e.g. placed manually) -> account refresh
           only; it is never imported into or managed by the strategy.
        """
        self._pm_init_user_stream_state()
        strategy_id = self._pm_actual_order_map.get(order_id)
        if strategy_id is not None:
            # Real order event for a locally known conditional stoploss.
            for trade in Trade.get_open_trades():
                if any(sl.order_id == strategy_id for sl in trade.open_sl_orders):
                    with self._exit_lock:
                        exchange_order = self.exchange.fetch_stoploss_order(
                            strategy_id, trade.pair
                        )
                        self._pm_record_actual_order(exchange_order, strategy_id)
                        self.update_trade_state(
                            trade, strategy_id, exchange_order, stoploss_order=True
                        )
                    logger.info(
                        f"PM user stream reconciled triggered stoploss {strategy_id} "
                        f"via real order {order_id} on {trade.pair}."
                    )
                    return True
            logger.warning(
                f"PM user stream: actual order {order_id} maps to strategy {strategy_id} "
                "but no local stoploss order matches; keeping the order block active."
            )
            return False

        client_order_id = str(order_data.get("c") or "")

        # P0-5: the in-memory actual-order map may not be rebuilt yet (e.g. right after
        # a restart). Before declaring this event foreign, query the conditional
        # history of every local open stoploss on the same pair - a triggered child
        # order reports its real ``orderId`` there. A failed lookup is fail-closed:
        # we can never silently ignore an order that may belong to this bot.
        child_lookup_failed = False
        for trade in Trade.get_open_trades():
            if trade.pair != pair:
                continue
            for sl in trade.open_sl_orders:
                try:
                    exchange_order = self.exchange.fetch_stoploss_order(sl.order_id, pair)
                except Exception as e:
                    child_lookup_failed = True
                    logger.warning(
                        f"PM user stream: could not resolve stoploss {sl.order_id} "
                        f"({pair}) while classifying order event {order_id}: {e}"
                    )
                    continue
                self._pm_record_actual_order(exchange_order, sl.order_id)
                if str(exchange_order.get("id_stop") or "") == order_id:
                    # The event IS the real child order of a local conditional stoploss.
                    with self._exit_lock:
                        self.update_trade_state(
                            trade, sl.order_id, exchange_order, stoploss_order=True
                        )
                    logger.info(
                        f"PM user stream: classified order event {order_id} as the real "
                        f"order of local stoploss {sl.order_id} on {pair} (post-restart "
                        "rebuild)."
                    )
                    return True
        if child_lookup_failed:
            # Fail-closed: we could not rule out that this order belongs to this bot.
            if order_id not in self._pm_unmatched_stream_order_ids:
                self._pm_unmatched_stream_order_ids.add(order_id)
            self._pm_block_orders("reconciliation_incomplete")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM User Stream FAIL-CLOSED: order event {order_id} on {pair} "
                        "could not be classified because a conditional-history lookup "
                        "failed. New orders BLOCKED until reconciliation succeeds."
                    ),
                }
            )
            return False

        if client_order_id.startswith(("ft", "st")):
            # Ours, but not in the local index -> we lost track of it.
            if len(self._pm_unmatched_stream_order_ids) > 1000:
                self._pm_unmatched_stream_order_ids.clear()
            if order_id not in self._pm_unmatched_stream_order_ids:
                self._pm_unmatched_stream_order_ids.add(order_id)
            self._pm_block_orders("unmatched_stream_order")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM User Stream FAIL-CLOSED: order event {order_id} "
                        f"(clientId {client_order_id}) on {pair} did not match a local "
                        "open order. New orders are BLOCKED until /pm_recover succeeds."
                    ),
                }
            )
            if (
                self.config.get("exchange", {})
                .get("portfolio_margin_risk", {})
                .get("user_stream_recover_unmatched_orders", True)
            ):
                self._pm_order_recovery()
            return False

        event_state = str(order_data.get("X") or order_data.get("x") or "unknown")
        event_key = f"{order_id}:{event_state}"
        if len(self._pm_foreign_stream_order_events) > 1000:
            self._pm_foreign_stream_order_events.clear()
        if event_key not in self._pm_foreign_stream_order_events:
            self._pm_foreign_stream_order_events.add(event_key)
            logger.info(
                f"PM user stream: observed foreign order event {order_id} on {pair} "
                f"(state={event_state}); read-only account refresh only, not managed by this bot."
            )
        return False

    @staticmethod
    def _pm_stream_time(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _pm_restart_user_stream(self, reason: str) -> None:
        self._pm_init_user_stream_state()
        risk_cfg = self.config.get("exchange", {}).get("portfolio_margin_risk", {})
        max_restarts = int(risk_cfg.get("user_stream_max_restarts_per_hour", 3))
        now = datetime.now(UTC)
        self._pm_user_stream_restarts = [
            restart
            for restart in self._pm_user_stream_restarts
            if restart > now - timedelta(hours=1)
        ]
        if len(self._pm_user_stream_restarts) >= max_restarts:
            if not self._pm_user_stream_restart_limit_warning_sent:
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            "PM User Stream CRITICAL: restart limit reached "
                            f"({max_restarts}/hour). Reason: {reason}. "
                            "REST recovery remains active, but live event sync is degraded."
                        ),
                    }
                )
                self._pm_user_stream_restart_limit_warning_sent = True
            return

        self._pm_user_stream_restart_limit_warning_sent = False
        self._pm_user_stream_restarts.append(now)
        self.rpc.send_msg(
            {
                "type": RPCMessageType.WARNING,
                "status": f"PM User Stream restart: {reason}",
            }
        )
        if hasattr(self.exchange, "stop_pm_user_stream"):
            self.exchange.stop_pm_user_stream()
        if self._pm_listen_key and hasattr(self.exchange, "start_pm_user_stream"):
            self.exchange.start_pm_user_stream(self._pm_listen_key)
        else:
            self._pm_create_listen_key()
        self._pm_order_recovery()

    def _pm_user_stream_health_monitor(self) -> None:
        try:
            self._pm_init_user_stream_state()
            if (
                self.config["dry_run"]
                or not hasattr(self.exchange, "get_pm_user_stream_stats")
                or not getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
            ):
                return
            stats = self.exchange.get_pm_user_stream_stats()
            if not stats.get("enabled"):
                return

            risk_cfg = self.config.get("exchange", {}).get("portfolio_margin_risk", {})
            queued = int(stats.get("queued_events") or 0)
            queue_warning_size = int(risk_cfg.get("user_stream_queue_warning_size", 500))
            if queued >= queue_warning_size and not self._pm_user_stream_queue_warning_active:
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM User Stream WARNING: {queued} queued events are waiting "
                            f"(threshold={queue_warning_size})."
                        ),
                    }
                )
                self._pm_user_stream_queue_warning_active = True
            elif queued < queue_warning_size:
                self._pm_user_stream_queue_warning_active = False

            dropped = int(stats.get("events_dropped") or 0)
            if dropped > self._pm_user_stream_last_events_dropped:
                # Dropped events mean the local view may be stale: go DEGRADED immediately,
                # block new orders and force a full reconciliation.
                self._pm_user_stream_state = "DEGRADED"
                self._pm_block_orders("user_stream_events_dropped")
                self._pm_user_stream_last_events_dropped = dropped
                self._pm_order_recovery()
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM User Stream CRITICAL: dropped events increased from "
                            f"{self._pm_user_stream_last_events_dropped} to {dropped}. "
                            "Stream state=DEGRADED; new orders BLOCKED; running order recovery."
                        ),
                    }
                )

            parse_errors = int(stats.get("parse_errors") or 0)
            if parse_errors > self._pm_user_stream_last_parse_errors:
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM User Stream WARNING: parse errors increased from "
                            f"{self._pm_user_stream_last_parse_errors} to {parse_errors}."
                        ),
                    }
                )
                self._pm_user_stream_last_parse_errors = parse_errors

            # --- Fail-closed stream health gate ---
            stream_healthy = True
            restart_limit_reached = False
            if not stats.get("running"):
                stream_healthy = False
                self._pm_restart_user_stream("stream thread is not running")
            elif not stats.get("connected"):
                disconnected_at = self._pm_stream_time(stats.get("last_disconnected_at"))
                max_disconnected_seconds = int(
                    risk_cfg.get("user_stream_disconnected_restart_seconds", 120)
                )
                if (
                    disconnected_at
                    and datetime.now(UTC) - disconnected_at
                    > timedelta(seconds=max_disconnected_seconds)
                ):
                    stream_healthy = False
                    self._pm_restart_user_stream(
                        "stream has been disconnected for "
                        f">{max_disconnected_seconds}s; last_error={stats.get('last_error')}"
                    )

            if queued >= queue_warning_size:
                # A growing backlog means events may be delayed or dropped - treat as unhealthy.
                stream_healthy = False

            # Restart-limit reached means the stream can no longer self-heal.
            now = datetime.now(UTC)
            one_hour_ago = now - timedelta(hours=1)
            recent_restarts = [
                t for t in self._pm_user_stream_restarts if t > one_hour_ago
            ]
            restart_limit = int(risk_cfg.get("user_stream_max_restarts_per_hour", 3))
            restart_limit_reached = len(recent_restarts) >= restart_limit

            if restart_limit_reached and not stream_healthy:
                self._pm_user_stream_state = "FAILED"
            elif stream_healthy:
                self._pm_user_stream_state = "HEALTHY"
            else:
                self._pm_user_stream_state = "DEGRADED"

            if self._pm_stream_fail_closed():
                if stream_healthy:
                    # Stream restored: unblocking requires a SUCCESSFUL reconciliation
                    # (no errors, all local open orders resolved) AND no unresolved
                    # order intents. A connected WebSocket alone never unblocks.
                    result = self._pm_reconcile_open_orders()
                    if not result["errors"] and not self._pm_has_unresolved_intents():
                        self._pm_unblock_orders("user_stream_unavailable")
                        self._pm_unblock_orders("user_stream_events_dropped")
                        self._pm_unblock_orders("reconciliation_incomplete")
                    else:
                        self._pm_block_orders("reconciliation_incomplete")
                        self.rpc.send_msg(
                            {
                                "type": RPCMessageType.WARNING,
                                "status": (
                                    "PM FAIL-CLOSED: user stream is connected but "
                                    "reconciliation did not complete cleanly "
                                    f"({len(result['errors'])} error(s), unresolved "
                                    f"intents={self._pm_unresolved_intent_count()}). "
                                    "New orders stay BLOCKED (reconciliation_incomplete)."
                                ),
                            }
                        )
                else:
                    self._pm_block_orders("user_stream_unavailable")
                    self.rpc.send_msg(
                        {
                            "type": RPCMessageType.WARNING,
                            "status": (
                                f"PM FAIL-CLOSED: user stream unhealthy "
                                f"(state={self._pm_user_stream_state}, "
                                f"running={stats.get('running')}, "
                                f"connected={stats.get('connected')}, "
                                f"queued={queued}). New orders BLOCKED until the stream "
                                "recovers and reconciliation completes."
                            ),
                        }
                    )
        except Exception as e:
            logger.warning(f"PM user stream health check failed: {e}")
            self._pm_user_stream_state = "DEGRADED"
            # Fail-closed: an unreadable health status must block new orders, not just
            # degrade the state silently.
            if self._pm_stream_fail_closed():
                self._pm_block_orders("user_stream_unavailable")

    def _pm_rebuild_user_stream(self) -> None:
        self._pm_init_user_stream_state()
        now = datetime.now(UTC)
        # Binance can deliver duplicate listenKeyExpired events in one drained batch.
        # Once a replacement key exists, rebuilding again only tears down the fresh
        # stream and duplicates the operator warning.  Do not suppress retries when
        # the preceding key creation failed (the key remains empty in that case).
        if (
            self._pm_listen_key
            and self._pm_last_listen_key_rebuild_at
            and now - self._pm_last_listen_key_rebuild_at < timedelta(seconds=30)
        ):
            logger.info("Ignoring duplicate Binance PM listenKeyExpired event after rebuild.")
            return

        self._pm_last_listen_key_rebuild_at = now
        # This is a recoverable Binance stream event.  Block new exposure until
        # the next health check reconciles the replacement stream, but do not
        # send a Telegram warning before we know recovery has failed.
        logger.warning("PM user data stream listenKey expired; rebuilding stream.")
        if self._pm_stream_fail_closed():
            self._pm_block_orders("user_stream_unavailable")
        if hasattr(self.exchange, "stop_pm_user_stream"):
            self.exchange.stop_pm_user_stream()
        self._pm_listen_key = ""
        self._pm_create_listen_key()
        if self._pm_listen_key:
            logger.info(
                "Binance PM replacement listenKey created; new orders remain blocked "
                "until stream health reconciliation succeeds."
            )

    def _pm_process_user_stream_event(
        self, event: dict[str, Any], order_index: dict[str, tuple[Any, Any]]
    ) -> tuple[bool, bool, bool]:
        event_type = event.get("e")
        if event_type == "ORDER_TRADE_UPDATE":
            order_updated = self._pm_handle_order_trade_update(event, order_index)
            # An external/manual order must refresh the PM account view promptly,
            # but it must never become a local Trade or participate in strategy
            # lifecycle actions.  The refresh is read-only and coalesced per batch.
            return order_updated, True, True
        if event_type == "ACCOUNT_UPDATE":
            return False, True, True
        if event_type == "riskLevelChange":
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM Risk Stream ALERT: state={event.get('s')}, "
                        f"uniMMR={event.get('u')}, equity={event.get('eq')}."
                    ),
                }
            )
            return False, True, True
        if event_type in {
            "ACCOUNT_CONFIG_UPDATE",
            "balanceUpdate",
            "outboundAccountPosition",
            "liabilityChange",
            "openOrderLoss",
            "MARGIN_CALL",
            "POSITION_HISTORY_UPDATE",
        }:
            return False, True, True
        if event_type == "listenKeyExpired":
            self._pm_rebuild_user_stream()
            return False, False, False

        logger.debug(f"Unhandled Binance PM user stream event type: {event_type}")
        return False, False, False

    def _pm_consume_user_stream_events(self) -> None:
        if (
            self.trading_mode != TradingMode.FUTURES
            or self.config["dry_run"]
            or not hasattr(self.exchange, "pop_pm_user_stream_events")
        ):
            return

        try:
            events = self.exchange.pop_pm_user_stream_events()
        except Exception as e:
            logger.warning(f"Could not read Binance PM user stream events: {e}")
            return

        if not events:
            return

        order_index: dict[str, tuple[Any, Any]] = {}
        for trade in Trade.get_open_trades():
            for order in trade.open_orders:
                order_index[str(order.order_id)] = (trade, order)
            # ``open_orders`` intentionally excludes stoploss orders.  PM
            # conditionals are identified by their local client strategy id,
            # which Binance can send in ORDER_TRADE_UPDATE.o.c before trigger.
            for order in trade.open_sl_orders:
                order_index[str(order.order_id)] = (trade, order)

        order_updates = 0
        account_updates = 0
        errors = 0
        needs_wallet_update = False
        needs_risk_check = False

        for event in events:
            event_type = event.get("e")
            try:
                order_updated, wallet_update, risk_check = self._pm_process_user_stream_event(
                    event, order_index
                )
                order_updates += int(order_updated)
                account_updates += int(event_type == "ACCOUNT_UPDATE")
                needs_wallet_update = needs_wallet_update or wallet_update
                needs_risk_check = needs_risk_check or risk_check
            except Exception as e:
                errors += 1
                logger.warning(f"Failed to process Binance PM user stream event {event_type}: {e}")

        if needs_wallet_update:
            self.wallets.update(require_update=True)
        if needs_risk_check:
            self._pm_risk_monitor()
        Trade.commit()

        logger.info(
            f"Processed {len(events)} Binance PM stream event(s): "
            f"orders={order_updates}, account={account_updates}, errors={errors}."
        )

    def _pm_risk_monitor(self) -> None:
        try:
            if not hasattr(self.exchange, "get_pm_risk_summary"):
                return
            risk = self.exchange.get_pm_risk_summary()
            if not risk.get("enabled"):
                self._pm_unblock_orders("risk_check_failed")
                return

            # Risk data is available - clear any previous risk-API failure block.
            self._pm_unblock_orders("risk_check_failed")
            self._pm_risk_api_failure_sent = False

            risk_cfg = self.config.get("exchange", {}).get("portfolio_margin_risk", {})
            uni_mmr = risk.get("uni_mmr")
            account_status = risk.get("account_status")

            # Missing critical risk data while configured => fail-closed
            if risk_cfg.get("min_uni_mmr") is not None and uni_mmr is None:
                self._pm_block_orders("risk_data_missing")
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            "PM Risk FAIL-CLOSED: uniMMR missing while min_uni_mmr is "
                            "configured. New orders BLOCKED."
                        ),
                    }
                )
            else:
                self._pm_unblock_orders("risk_data_missing")

            if account_status and account_status != "NORMAL":
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM Risk ALERT: account status is {account_status}. "
                            f"uniMMR={uni_mmr}, equity={risk.get('account_equity')}. "
                            "Bot will refuse new orders."
                        ),
                    }
                )

            warning_mmr = risk_cfg.get("warning_uni_mmr")
            if warning_mmr is not None and uni_mmr is not None and uni_mmr < float(warning_mmr):
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM Risk WARNING: uniMMR {uni_mmr} is below warning threshold "
                            f"{warning_mmr}. Equity={risk.get('account_equity')}, "
                            f"Maintenance margin={risk.get('maintenance_margin')}."
                        ),
                    }
                )

            critical_mmr = risk_cfg.get("min_uni_mmr")
            if critical_mmr is not None and uni_mmr is not None and uni_mmr < float(critical_mmr):
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM Risk CRITICAL: uniMMR {uni_mmr} is below min_uni_mmr "
                            f"{critical_mmr}. New orders are BLOCKED. "
                            f"Equity={risk.get('account_equity')}."
                        ),
                    }
                )

            emergency_stop_mmr = risk_cfg.get("emergency_stop_uni_mmr")
            if (
                emergency_stop_mmr is not None
                and uni_mmr is not None
                and uni_mmr < float(emergency_stop_mmr)
            ):
                # Stop BEFORE attempting REST exits.  A close failure, a database
                # error or a later recovery in this loop must never permit an
                # automatic re-entry after an emergency threshold breach.
                self.state = State.STOPPED
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM EMERGENCY STOP: uniMMR {uni_mmr} is below emergency stop "
                            f"{emergency_stop_mmr}. FORCE-CLOSING ALL POSITIONS. "
                            f"Equity={risk.get('account_equity')}."
                        ),
                    }
                )
                self._pm_emergency_close_all()

            max_daily_loss = risk_cfg.get("max_daily_loss")
            if max_daily_loss is not None:
                today_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
                try:
                    daily_pnl = (
                        Trade.session.execute(
                            select(func.sum(Trade.close_profit_abs)).filter(
                                Trade.is_open.is_(False), Trade.close_date >= today_start
                            )
                        ).scalar_one()
                        or 0.0
                    )
                    if risk_cfg.get("max_daily_loss_include_unrealized", False):
                        daily_pnl += self._pm_unrealized_pnl()
                except Exception as e:  # noqa: BLE001 - any accounting error must fail closed
                    # A failed PnL calculation must not pretend the account has
                    # made no loss.  Keep exits available, but refuse all new
                    # exposure until the accounting path becomes readable again.
                    self._pm_block_orders("daily_loss_check_failed")
                    self.rpc.send_msg(
                        {
                            "type": RPCMessageType.WARNING,
                            "status": (
                                f"PM Daily-loss FAIL-CLOSED: could not calculate UTC daily "
                                f"PnL ({e}). New orders are BLOCKED until the next successful "
                                "daily-loss check."
                            ),
                        }
                    )
                    return
                self._pm_unblock_orders("daily_loss_check_failed")
                if daily_pnl < -float(max_daily_loss):
                    self.rpc.send_msg(
                        {
                            "type": RPCMessageType.WARNING,
                            "status": (
                                f"PM MAX DAILY LOSS EXCEEDED: daily PnL={daily_pnl:.2f} "
                                f"exceeds max_daily_loss={max_daily_loss}. "
                                "Stopping trader and closing all positions."
                            ),
                        }
                    )
                    self._pm_emergency_close_all()
                    self.state = State.STOPPED

        except Exception as e:
            logger.warning(f"PM risk monitor check failed: {e}")
            # Fail-closed: block new orders on any risk API exception.
            self._pm_block_orders("risk_check_failed")
            action = self._pm_risk_failure_action()
            if action == "stop":
                self.state = State.STOPPED
            elif action == "pause":
                self.state = State.PAUSED
            if not self._pm_risk_api_failure_sent:
                self._pm_risk_api_failure_sent = True
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM Risk monitor FAILED (fail-closed): {e}. "
                            f"New orders BLOCKED. Failure action: {action}."
                        ),
                    }
                )

    def _pm_unrealized_pnl(self) -> float:
        """Sum of unrealized PnL across open PM trades (in stake currency)."""
        total = 0.0
        for trade in Trade.get_open_trades():
            try:
                current_rate = self.exchange.get_rate(
                    trade.pair, side="exit", is_short=trade.is_short, refresh=False
                )
                total += trade.calc_profit(current_rate)
            except Exception as e:
                # The daily-loss guard must not silently understate loss when an
                # open trade cannot be valued.  The caller turns this into a
                # fail-closed block with an operator-visible alert.
                raise OperationalException(
                    f"Could not calculate unrealized PM PnL for {trade.pair}: {e}"
                ) from e
        return total

    def _pm_emergency_close_all(self) -> None:
        """
        Force-close EVERY open PM position on the account - including manual /
        external positions with no local trade - and CONFIRM each one is flat on
        the exchange before counting it closed.

        Creating an exit order is NOT proof of safety: after each attempt the
        position is re-fetched per-symbol (fresh, bypassing the account-wide
        snapshot cache) and only a zero contract count counts as closed.
        """
        risk_cfg = self.config.get("exchange", {}).get("portfolio_margin_risk", {})
        max_retries = int(risk_cfg.get("emergency_close_retries", 3))
        try:
            all_positions = [
                p
                for p in self.exchange.fetch_positions()
                if float(p.get("contracts", 0) or 0) != 0
            ]
        except Exception as e:
            logger.error(f"PM emergency close: could not enumerate exchange positions: {e}")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM EMERGENCY CLOSE FAILED: exchange positions could not be "
                        f"enumerated ({e.__class__.__name__}). Manual intervention "
                        "required."
                    ),
                }
            )
            return
        if not all_positions:
            logger.info("PM emergency close: account is already flat.")
            return
        closed = 0
        failed = 0
        failed_details: list[str] = []
        with self._exit_lock:
            for position in all_positions:
                symbol = str(position.get("symbol") or "")
                side = str(position.get("side") or "")
                contracts = abs(float(position.get("contracts") or 0))
                trade = self._pm_open_trade_for_position(symbol, side)
                success = False
                last_error = "unknown"
                for attempt in range(1, max_retries + 1):
                    try:
                        if trade is not None and trade.is_open and trade.has_open_position:
                            if not self._safe_force_exit(trade):
                                last_error = f"attempt {attempt}: exit order not confirmed"
                        else:
                            if not self._pm_force_close_foreign_position(symbol, side, contracts):
                                last_error = f"attempt {attempt}: foreign close failed"
                    except Exception as e:
                        last_error = f"attempt {attempt}: {e}"
                    # Creating an order is NOT proof of safety: verify FLAT with a
                    # fresh per-symbol fetch (the account-wide snapshot cache must
                    # never mask an unfilled exit) - even after a "successful" exit.
                    try:
                        fresh = self.exchange.fetch_positions(pair=symbol)
                        remaining = sum(
                            float(p.get("contracts") or 0)
                            for p in fresh
                            if p.get("symbol") == symbol
                        )
                        if remaining < 1e-9:
                            success = True
                            break
                        last_error = f"attempt {attempt}: {remaining} contracts still open"
                    except Exception as e:
                        last_error = f"attempt {attempt}: verification failed ({e})"
                if success:
                    closed += 1
                    logger.warning(f"PM emergency close: {symbol} confirmed FLAT.")
                else:
                    failed += 1
                    failed_details.append(f"{symbol} ({side}): {last_error}")
        Trade.commit()
        logger.warning(
            f"PM emergency close: {closed} closed, {failed} failed, "
            f"out of {len(all_positions)} total."
        )
        if failed:
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM EMERGENCY CLOSE PARTIAL FAILURE: {failed} position(s) could "
                        f"NOT be confirmed flat and remain OPEN: "
                        f"{', '.join(failed_details[:5])}. Manual intervention is required."
                    ),
                }
            )

    def _pm_open_trade_for_position(self, pair: str, side: str | None) -> Trade | None:
        """The local open trade for an exchange position (pair+side), if any."""
        for trade in Trade.get_open_trades():
            if trade.pair != pair or not trade.is_open:
                continue
            if side is None:
                return trade
            if trade.is_short == (side == "short"):
                return trade
        return None

    def _pm_force_close_foreign_position(self, pair: str, side: str, contracts: float) -> bool:
        """
        Close a position that has NO local trade (manual/external exposure) with
        a reduce-only market order. Returns True when the order was created -
        the caller still verifies the position goes flat.
        """
        try:
            rate = self.exchange.get_rate(
                pair, side="exit", is_short=(side == "short"), refresh=True
            )
            amount = self.exchange._contracts_to_amount(pair, contracts)
            opposite = "sell" if side == "long" else "buy"
            order = self.exchange.create_order(
                pair=pair,
                ordertype=self.strategy.order_types.get("emergency_exit", "market"),
                side=opposite,
                amount=amount,
                rate=rate,
                leverage=1,
                reduceOnly=True,
                initial_order=False,
            )
            logger.warning(
                f"PM emergency close: placed reduce-only close for FOREIGN position "
                f"{pair} ({side}, {contracts} contracts); order id={order.get('id')}."
            )
            return True
        except Exception as e:
            logger.warning(f"PM emergency close: foreign close failed for {pair}: {e}")
            return False

    def _safe_force_exit(self, trade) -> bool:
        """
        Submit a reduceOnly emergency market exit for a trade. Raises on failure so the
        caller can retry and report accurately (never falsely report as closed).
        """
        current_rate = self.exchange.get_rate(
            trade.pair, side="exit", is_short=trade.is_short, refresh=True
        )
        return self.execute_trade_exit(
            trade,
            limit=current_rate,
            exit_check=ExitCheckTuple(exit_type=ExitType.EMERGENCY_EXIT),
            ordertype=self.strategy.order_types.get("emergency_exit", "market"),
        )

    def _pm_reconcile_open_orders(self) -> dict[str, Any]:
        """
        Reconcile all open orders for open PM trades against the exchange (PAPI).

        Uses the same full state-update path as normal fills (update_trade_state), so a fill
        updates the Trade lifecycle (open/closed), order status, filled amount, fees, realized
        PnL, exit time, wallet, notifications and protections.

        Idempotent: once an order is closed locally it is removed from ``trade.open_orders``,
        so repeated recovery does not re-process it. Shared by the user-stream event path,
        the scheduled recovery and the Telegram/API manual recovery.
        """
        result: dict[str, Any] = {
            "open_trades": 0,
            "checked": 0,
            "reconciled": 0,
            "mismatches": [],
            "errors": [],
        }
        try:
            if (
                not hasattr(self.exchange, "_is_portfolio_margin")
                or not self.exchange._is_portfolio_margin()
            ):
                return result
            open_trades = Trade.get_open_trades()
            result["open_trades"] = len(open_trades)
            for trade in open_trades:
                # ``trade.open_orders`` excludes stoploss orders; reconcile both.
                reconcile_orders = list(trade.open_orders) + list(trade.open_sl_orders)
                for order in reconcile_orders:
                    result["checked"] += 1
                    try:
                        if order.ft_order_side == "stoploss":
                            # Conditional stoploss: resolve strategy status AND the
                            # triggered real order (filled/average/cost/fee/trades).
                            exchange_order = self.exchange.fetch_stoploss_order(
                                order.order_id, trade.pair
                            )
                            self._pm_record_actual_order(exchange_order, order.order_id)
                        else:
                            exchange_order = self.exchange.fetch_order(order.order_id, trade.pair)
                        if exchange_order.get("status") != order.status:
                            mismatch_detail = (
                                f"{order.order_id}({trade.pair}): DB={order.status} "
                                f"Exchange={exchange_order.get('status')}"
                            )
                            result["mismatches"].append(mismatch_detail)
                            logger.warning(
                                f"PM order state mismatch for {order.order_id} "
                                f"({trade.pair}): DB={order.status}, "
                                f"Exchange={exchange_order.get('status')}"
                            )
                        # Full lifecycle update - same path as a normal fill.
                        self.update_trade_state(
                            trade,
                            order.order_id,
                            action_order=exchange_order,
                            stoploss_order=order.ft_order_side == "stoploss",
                            send_msg=True,
                        )
                        result["reconciled"] += 1
                    except Exception as e:
                        result["errors"].append(f"{order.order_id}: {e}")
                        logger.debug(f"PM order recovery check failed for {order.order_id}: {e}")
            Trade.commit()
            # Reconcile LINKED intents: tombstone only the ones the exchange confirms.
            result["intents"] = self._pm_reconcile_linked_intents()
        except Exception as e:
            logger.warning(f"PM order recovery check failed: {e}")
            result["errors"].append(f"recovery: {e}")
        result["unresolved_intents"] = self._pm_unresolved_intent_count()
        if not result["errors"]:
            self._pm_init_user_stream_state()
            self._pm_last_success_reconcile_time = datetime.now(UTC)
            result["success"] = True
        else:
            result["success"] = False
        self._pm_last_reconcile_result = result
        return result

    def _pm_reconcile_linked_intents(self) -> dict[str, Any]:
        """
        Verify LINKED intents against the exchange and tombstone (RECONCILED) the
        ones the exchange confirms. Never deletes an intent the exchange does not
        confirm.
        """
        report: dict[str, Any] = {"checked": 0, "reconciled": 0, "kept": 0, "errors": []}
        if not hasattr(self.exchange, "list_pm_linked_intents"):
            return report
        try:
            intents = self.exchange.list_pm_linked_intents()
        except Exception as e:
            report["errors"].append(f"{e.__class__.__name__}: {e}")
            return report
        for intent in intents:
            report["checked"] += 1
            client_id = str(intent.get("client_id") or "")
            exchange_id = str(intent.get("exchange_order_id") or intent.get("linked_order_id") or "")
            pair = intent.get("pair") or ""
            try:
                if intent.get("kind") == "conditional":
                    order = self.exchange.fetch_stoploss_order(client_id, pair)
                else:
                    order = self.exchange._pm_fetch_order_by_exchange_id(exchange_id, pair)
            except InvalidOrderException:
                order = None
            except Exception as e:
                report["errors"].append(f"{client_id}: {e}")
                report["kept"] += 1
                continue
            if order is None:
                # The linked order no longer exists on the exchange (filled long
                # ago / purged) while the local record still expects it: keep the
                # intent and surface it for manual review - never guess.
                report["kept"] += 1
                logger.warning(
                    f"PM LINKED intent {client_id}: exchange order {exchange_id} no "
                    "longer exists; keeping the intent for manual review."
                )
                continue
            try:
                self.exchange.pm_tombstone_reconciled_intent(client_id)
                report["reconciled"] += 1
            except Exception as e:
                report["errors"].append(f"{client_id}: {e}")
                report["kept"] += 1
        return report

    def _pm_order_recovery(self, clear_unmatched_block: bool = False) -> None:
        result = self._pm_reconcile_open_orders()
        if result["mismatches"]:
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM Order Recovery: {len(result['mismatches'])} order state "
                        f"mismatch(es) reconciled across {result['open_trades']} open trades."
                    ),
                }
            )
        if result["errors"]:
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM Order Recovery: {len(result['errors'])} order(s) could NOT be "
                        "reconciled. New orders stay BLOCKED (fail-closed). "
                        f"{'; '.join(result['errors'][:3])}"
                    ),
                }
            )
        if clear_unmatched_block and not result["errors"]:
            # Explicit manual recovery that completed cleanly -> clear the
            # unmatched-stream-order block.
            self._pm_unblock_orders("unmatched_stream_order")

    def _pm_has_open_trade_for(self, pair: str, side: str | None) -> bool:
        """Whether a local open trade exists for an exchange position (pair+side)."""
        for trade in Trade.get_open_trades():
            if trade.pair != pair or not trade.is_open:
                continue
            if side is None:
                return True
            if trade.is_short == (side == "short"):
                return True
        return False

    def _pm_link_intent_for_order(
        self, exchange_order: dict, trade: Trade, local_order_id: str
    ) -> None:
        """
        Mark the PM order intent LINKED inside the CURRENT (uncommitted) session.

        No-op for non-PM / dry-run / missing client id. Raises OperationalException
        when the intent is UNKNOWN (fail-closed - never link uncertain state).
        """
        if self.trading_mode != TradingMode.FUTURES:
            return
        if not getattr(self.exchange, "_is_portfolio_margin", lambda: False)():
            return
        if not exchange_order:
            return
        client_id = exchange_order.get("clientOrderId") or exchange_order.get("clientAlgoId")
        if not client_id:
            return
        try:
            Trade.session.flush()
        except Exception:
            pass
        self.exchange.pm_link_intent_in_session(client_id, str(local_order_id), trade.id)

    def _pm_find_local_order(self, exchange_id: str, client_id: str) -> dict[str, Any] | None:
        """
        Find the local Order row for a resolved exchange order.

        Regular PM orders store the exchange orderId locally; conditional
        (stoploss) orders store the client algo id. Either identifier may match.
        """
        candidates = {str(exchange_id), str(client_id)} - {"", "None"}
        if not candidates:
            return None
        for order in Order.session.query(Order).filter(Order.order_id.in_(candidates)).all():
            return {"order_id": str(order.order_id), "trade_id": order.ft_trade_id}
        return None

    # ---- Signal decision ledger + unified data-failure policy ----

    def _pm_ledger_enabled(self) -> bool:
        return (
            self.trading_mode == TradingMode.FUTURES
            and not self.config.get("dry_run", True)
            and getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
        )

    def _pm_ledger_snapshot(self, pair: str) -> tuple[dict | None, str | None]:
        """
        Build the durable per-candle signal snapshot for ``pair``.

        Returns (snapshot, block_reason): block_reason is None when the factors
        are fresh and contiguous; otherwise the reason the data is not usable
        (unified fail-closed input - a strategy signal may never bypass it).
        """
        if not self._pm_ledger_enabled():
            return None, None
        snapshot_fn = getattr(self.strategy, "pm_signal_snapshot", None)
        if snapshot_fn is None:
            return None, None
        try:
            dataframe, _ = self.dataprovider.get_analyzed_dataframe(pair, self.config["timeframe"])
            snapshot = snapshot_fn(pair, dataframe)
        except Exception as e:
            return None, f"snapshot_unavailable: {e.__class__.__name__}"
        if snapshot is None:
            return None, "no_closed_candle_data"
        # Durable gap latch: a previously persisted data discontinuity keeps
        # blocking until the bot has seen a complete contiguous recovery (the
        # missing candle is backfilled). Never expires implicitly.
        try:
            from freqtrade.persistence.pm_candle_watermark import PMCandleWatermark

            watermark = PMCandleWatermark.get(pair, self.config["timeframe"])
            if watermark is not None and watermark.gap_active:
                if not self._pm_dataframe_has_candle(
                    pair, self.config["timeframe"], watermark.gap_expected_open_time
                ):
                    return snapshot, f"candle_gap_unrecovered: {watermark.gap_reason or 'unknown'}"
                # else: the hole was backfilled - recovery proceeds and the
                # decision path clears the latch via mark_entry_decision.
        except Exception:  # defensive - ledger snapshot must never raise
            pass
        # Contiguity: the last two closed candles must not have a gap.
        try:
            if dataframe is not None and len(dataframe) >= 2 and "date" in dataframe:
                last_ts = pd.to_datetime(dataframe["date"].iat[-1], utc=True)
                prev_ts = pd.to_datetime(dataframe["date"].iat[-2], utc=True)
                tf_s = timeframe_to_seconds(self.config["timeframe"])
                if (last_ts - prev_ts).total_seconds() > tf_s * 1.5:
                    return snapshot, "candle_gap"
        except Exception:
            pass
        if not snapshot.get("data_fresh"):
            return snapshot, f"data_unhealthy: {snapshot.get('freshness_detail') or 'unknown'}"
        return snapshot, None

    @staticmethod
    def _pm_utc_naive(value: datetime | None) -> datetime | None:
        """Normalize a timestamp to the PM ledger's UTC-naive representation."""
        if value is None:
            return None
        if value.tzinfo is not None:
            return value.astimezone(UTC).replace(tzinfo=None)
        return value

    def _pm_dataframe_has_candle(
        self, pair: str, timeframe: str, open_time: datetime | None
    ) -> bool:
        """True when the analyzed dataframe contains a candle opened at ``open_time``."""
        if open_time is None:
            return False
        try:
            dataframe, _ = self.dataprovider.get_analyzed_dataframe(pair, timeframe)
            if dataframe is None or len(dataframe) == 0 or "date" not in dataframe:
                return False
            target = pd.to_datetime(open_time, utc=True)
            dates = pd.to_datetime(dataframe["date"], utc=True)
            return bool((dates == target).any())
        except Exception:
            return False

    def _pm_detect_persisted_candle_gap(
        self,
        pair: str,
        timeframe: str,
        cursor: datetime | None,
        candle_open: datetime,
    ) -> tuple[datetime, datetime] | None:
        """
        Detect a data discontinuity between the durable watermark cursor and the
        candle being decided.

        Returns ``(expected_open_time, observed_open_time)`` when the candle
        immediately preceding ``candle_open`` is MISSING from the dataframe -
        i.e. the series jumped, so factors may have been computed over a hole.
        A skip caused by downtime whose data was backfilled (the preceding
        candle IS present) is not a gap and does not block.
        """
        if cursor is None:
            return None
        candle = self._pm_utc_naive(candle_open)
        if candle is None or candle <= cursor:
            return None
        prev_candle = candle - timedelta(seconds=timeframe_to_seconds(timeframe))
        if not self._pm_dataframe_has_candle(pair, timeframe, prev_candle):
            return prev_candle, candle
        return None

    def _pm_record_signal_decision(
        self,
        pair: str,
        decision: str,
        *,
        reason: str | None = None,
        order_client_id: str | None = None,
        decision_scope: str = "entry",
    ) -> bool:
        """
        Write-once decision row for the current closed candle of ``pair``.

        The first decision for a candle wins (factors are evaluated at most once
        per closed, contiguous candle). Entry-scope rows additionally advance
        the durable :class:`PMCandleWatermark` cursor in the SAME transaction;
        a data discontinuity latches the watermark instead, so the pair stays
        fail-closed until the missing candle is backfilled. Returns True when a
        NEW row was recorded.
        """
        if not self._pm_ledger_enabled():
            return False
        try:
            return self._pm_record_signal_decision_inner(
                pair,
                decision,
                reason=reason,
                order_client_id=order_client_id,
                decision_scope=decision_scope,
            )
        except Exception:
            # The ledger/watermark is an audit + exactly-once cursor, NOT the
            # order-consistency source of truth (intent/outbox is). A ledger
            # failure must never break an order that was already placed, nor
            # an entry flow; the order path itself stays fail-closed.
            logger.exception("PM signal ledger write failed for %s (%s).", pair, decision)
            return False

    def _pm_record_signal_decision_inner(
        self,
        pair: str,
        decision: str,
        *,
        reason: str | None = None,
        order_client_id: str | None = None,
        decision_scope: str = "entry",
    ) -> bool:
        snapshot, block_reason = self._pm_ledger_snapshot(pair)
        if snapshot is None:
            return False
        from freqtrade.persistence.pm_candle_watermark import PMCandleWatermark
        from freqtrade.persistence.pm_signal_ledger import PMSignalLedger

        timeframe = self.config["timeframe"]
        candle_open = self._pm_utc_naive(snapshot.get("candle_open_time")) or (
            self._pm_utc_naive(timeframe_to_prev_date(timeframe, datetime.now(UTC)))
        )
        watermark: PMCandleWatermark | None = None
        gap_latched_now = False
        recovered_gap = False
        if decision_scope == "entry":
            # A decision evaluated on unhealthy / non-contiguous data is ALWAYS
            # recorded as blocked_data - a no_signal or entry_submitted must
            # never be attributed to a gapped candle.
            if block_reason is not None and decision not in ("blocked", "blocked_data"):
                decision = "blocked_data"
                reason = block_reason
            watermark = PMCandleWatermark.get_or_create(pair, timeframe)
            if watermark.gap_active:
                # Recovery check: the latched gap heals only when the missing
                # candle is actually present in the dataframe again.
                if self._pm_dataframe_has_candle(
                    pair, timeframe, watermark.gap_expected_open_time
                ):
                    recovered_gap = True
                else:
                    decision = "blocked_data"
                    reason = f"candle_gap_unrecovered: {watermark.gap_reason or 'unknown'}"
                    gap_latched_now = True
            elif not recovered_gap:
                gap = self._pm_detect_persisted_candle_gap(
                    pair, timeframe, watermark.last_decision_candle_open_time, candle_open
                )
                if gap is not None:
                    expected_open, observed_open = gap
                    watermark.mark_gap(
                        expected_open_time=expected_open,
                        observed_open_time=observed_open,
                        reason="missing_closed_candle",
                    )
                    decision = "blocked_data"
                    reason = (
                        f"candle_gap: expected {expected_open.isoformat()} "
                        f"before {observed_open.isoformat()} missing"
                    )
                    gap_latched_now = True

        strategy_version = str(
            getattr(self.strategy, "strategy_version", None)
            or self.strategy.get_strategy_name()
        )
        row = PMSignalLedger.record_once(
            pair=pair,
            timeframe=timeframe,
            candle_open_time=candle_open,
            strategy_version=strategy_version,
            factor_hash=str(snapshot.get("factor_hash") or ""),
            data_fresh=bool(snapshot.get("data_fresh")),
            freshness_detail=snapshot.get("freshness_detail"),
            signal_tag=snapshot.get("signal_tag"),
            decision=decision,
            decision_reason=reason,
            order_client_id=order_client_id,
            decision_scope=decision_scope,
        )
        if (
            decision_scope == "entry"
            and watermark is not None
            and not gap_latched_now
            and row.decision == decision
        ):
            watermark.mark_entry_decision(candle_open, recovered_gap=recovered_gap)
        Trade.session.commit()
        if decision_scope == "entry" and row.decision != decision and decision == "entry_submitted":
            logger.warning(
                f"PM signal ledger: candle {candle_open} for {pair} already has decision "
                f"'{row.decision}' - skipping duplicate decision '{decision}'."
            )
        return row.decision == decision

    def _pm_rebuild_actual_order_map(self) -> None:
        """
        Rebuild the real-order-id -> strategy-id map from PERSISTENT local data.

        For every local open stoploss order, resolve the conditional lifecycle; a
        triggered strategy reports its real ``orderId`` (id_stop) which is then
        mapped. This runs at startup BEFORE the user stream is released, so a child
        ORDER_TRADE_UPDATE arriving right after restart is never classified as
        foreign. Lookup failures block new orders (fail-closed).
        """
        self._pm_init_user_stream_state()
        failures = 0
        for trade in Trade.get_open_trades():
            for sl in trade.open_sl_orders:
                try:
                    exchange_order = self.exchange.fetch_stoploss_order(sl.order_id, trade.pair)
                    self._pm_record_actual_order(exchange_order, sl.order_id)
                except Exception as e:
                    failures += 1
                    logger.warning(
                        f"PM startup: could not resolve stoploss {sl.order_id} "
                        f"({trade.pair}) to rebuild the actual-order map: {e}"
                    )
        if failures:
            self._pm_block_orders("reconciliation_incomplete")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM FAIL-CLOSED: {failures} stoploss order(s) could not be "
                        "resolved at startup to rebuild the actual-order map. New "
                        "orders BLOCKED until reconciliation succeeds."
                    ),
                }
            )
        else:
            self._pm_unblock_orders("reconciliation_incomplete")

    def _pm_recover_pending_intents(self) -> dict[str, Any]:
        """
        Crash/restart recovery for the PM order pipeline.

        1. Relay undispatched outbox rows (orders whose POST never happened) -
           idempotent thanks to the persisted client ids (advisory-locked).
        2. Resolve unresolved intents against the exchange:
           * definitively absent -> tombstone intent, outbox REJECTED (audit);
           * exists -> ensure the ACK evidence is persisted, then LINK the intent
             to the local order when the local database already has it (normal
             post-restart state); otherwise keep the intent (blocking) and report
             the orphan - the crash window between exchange ACK and local commit.
        * Exchange not queryable -> keep the intent and BLOCK new orders (fail-closed).
        * Intent store unreadable -> PAUSE the bot and BLOCK (fail-closed).

        Returns the recovery report.
        """
        report: dict[str, Any] = {
            "checked": 0,
            "cleared": 0,
            "linked": 0,
            "orphaned": 0,
            "unresolved": 0,
            "outbox": None,
            "store_error": None,
        }
        if not hasattr(self.exchange, "list_pm_pending_intents"):
            return report
        # 0) Relay undispatched outbox rows first (advisory-locked, idempotent).
        if hasattr(self.exchange, "pm_drain_outbox"):
            try:
                report["outbox"] = self.exchange.pm_drain_outbox()
            except Exception as e:
                logger.warning(f"PM outbox relay failed at startup: {e}")
                report["outbox"] = {"errors": [f"{e.__class__.__name__}: {e}"]}
        try:
            intents = self.exchange.list_pm_pending_intents()
        except Exception as e:
            # Fail-closed: an unreadable intent store must never be treated as empty.
            logger.warning(f"Could not read PM pending order intents: {e}")
            report["store_error"] = f"{e.__class__.__name__}: {e}"
            self.state = State.PAUSED
            self._pm_block_orders("intent_store_unavailable")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM FAIL-CLOSED: the order intent store could not be read at "
                        f"startup ({e}). Bot PAUSED; new orders BLOCKED."
                    ),
                }
            )
            return report
        if not intents:
            return report

        for intent in intents:
            report["checked"] += 1
            client_id = str(intent.get("client_id") or "")
            try:
                result = self.exchange.resolve_pm_pending_intent(intent)
            except Exception as e:
                result = {
                    "resolved": False,
                    "uncertain": True,
                    "client_id": client_id,
                    "error": f"{e.__class__.__name__}: {e}",
                }
            if result.get("uncertain"):
                report["unresolved"] += 1
                continue
            if result.get("exists") is False:
                try:
                    self.exchange.clear_pm_pending_intent(client_id)
                except Exception as e:
                    logger.warning(f"Could not clear resolved PM intent {client_id}: {e}")
                    report["unresolved"] += 1
                    continue
                report["cleared"] += 1
                logger.info(
                    f"PM pending intent {client_id} resolved: order does not exist on the "
                    "exchange (it was never placed). Intent cleared."
                )
                continue
            # The order exists on the exchange. Try to link it to the local database.
            exchange_id = str(
                (result.get("order") or {}).get("id")
                or intent.get("exchange_order_id")
                or ""
            )
            local = self._pm_find_local_order(exchange_id, client_id)
            if local is not None:
                try:
                    self.exchange.pm_mark_intent_linked(
                        client_id, local["order_id"], local["trade_id"]
                    )
                    report["linked"] += 1
                    logger.info(
                        f"PM pending intent {client_id} LINKED to local order "
                        f"{local['order_id']} (trade {local['trade_id']})."
                    )
                except Exception as e:
                    logger.warning(f"Could not link PM intent {client_id}: {e}")
                    report["unresolved"] += 1
                continue
            report["orphaned"] += 1
            logger.warning(
                f"PM pending intent {client_id} resolved: the order EXISTS on the "
                f"exchange (id={exchange_id}) but has no local Trade/Order record - "
                "the crash window between exchange ACK and local commit was hit. "
                "The intent stays ACKED (evidence preserved) and new exposure stays "
                "blocked; manual reconciliation is required."
            )

        if report["orphaned"]:
            self._pm_block_orders("pending_intent_unresolved")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM FAIL-CLOSED: {report['orphaned']} order(s) exist on the "
                        "exchange with no local Trade/Order record (ACK->commit crash "
                        "window). Evidence is preserved on the ACKED intents. New orders "
                        "are BLOCKED until /pm_recover succeeds or manual reconciliation."
                    ),
                }
            )
        if report["unresolved"]:
            self._pm_block_orders("pending_intent_unresolved")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM FAIL-CLOSED: {report['unresolved']} persisted order "
                        "intent(s) could NOT be resolved against the exchange after "
                        "restart. New orders are BLOCKED until /pm_recover succeeds."
                    ),
                }
            )
        if not report["orphaned"] and not report["unresolved"]:
            self._pm_unblock_orders("pending_intent_unresolved")
        return report

    def _pm_startup_consistency_check(self) -> dict[str, Any]:
        """
        Compare exchange (PAPI) positions, normal open orders AND conditional
        (stoploss) open orders against the local database.

        Never mutates the exchange or the database; it only reports mismatches. The caller
        decides what to do based on ``startup_consistency_mode``.
        """
        result: dict[str, Any] = {
            "status": "consistent",
            "exchange_positions": [],
            "exchange_open_orders": [],
            "exchange_conditional_orders": [],
            "unknown_positions": [],
            "unknown_orders": [],
            "unknown_conditional_orders": [],
            "local_open_trades_flat_on_exchange": [],
            "local_open_orders_missing_on_exchange": [],
            "recent_closed_order_mismatches": [],
        }
        try:
            positions = self.exchange.fetch_positions()
            for position in positions:
                contracts = position.get("contracts", 0) or 0
                pair = position.get("symbol")
                if not contracts or not pair:
                    continue
                result["exchange_positions"].append(
                    {"pair": pair, "side": position.get("side"), "contracts": contracts}
                )
                if not self._pm_has_open_trade_for(pair, position.get("side")):
                    result["unknown_positions"].append(
                        {"pair": pair, "side": position.get("side"), "contracts": contracts}
                    )

            open_trades = Trade.get_open_trades()
            known_order_ids = {
                order.order_id
                for trade in open_trades
                for order in trade.open_orders
            }
            known_strategy_ids = {
                order.order_id
                for trade in open_trades
                for order in trade.open_sl_orders
            }
            open_orders = self.exchange.fetch_open_orders()
            for order in open_orders:
                order_id = str(order.get("id") or "")
                symbol = order.get("symbol")
                if not order_id:
                    continue
                result["exchange_open_orders"].append(
                    {"order_id": order_id, "symbol": symbol}
                )
                if order_id not in known_order_ids and order_id not in known_strategy_ids:
                    result["unknown_orders"].append(
                        {"order_id": order_id, "symbol": symbol}
                    )

            # Conditional (stoploss) open orders live on a separate PAPI endpoint and
            # are identified by strategy ids - never mix them with normal order ids.
            conditional_orders = self.exchange.fetch_open_conditional_orders()
            for order in conditional_orders:
                strategy_id = str(order.get("id") or "")
                symbol = order.get("symbol")
                if not strategy_id:
                    continue
                result["exchange_conditional_orders"].append(
                    {"order_id": strategy_id, "symbol": symbol}
                )
                if strategy_id not in known_strategy_ids:
                    result["unknown_conditional_orders"].append(
                        {"order_id": strategy_id, "symbol": symbol}
                    )

            # --- Reverse direction: local records the exchange does NOT confirm ---
            # (one-way mode, so position matching is side-agnostic)
            exchange_position_pairs = {
                pos["pair"]
                for pos in result["exchange_positions"]
                if float(pos.get("contracts") or 0) != 0
            }
            exchange_order_ids = {o["order_id"] for o in result["exchange_open_orders"]}
            exchange_conditional_ids = {o["order_id"] for o in result["exchange_conditional_orders"]}
            for trade in open_trades:
                if not trade.is_open:
                    continue
                local_open_ids = {str(o.order_id) for o in trade.open_orders}
                local_sl_ids = {str(o.order_id) for o in trade.open_sl_orders}
                if trade.pair not in exchange_position_pairs:
                    working_entry = bool(local_open_ids & exchange_order_ids)
                    working_stop = bool(local_sl_ids & exchange_conditional_ids)
                    if not working_entry and not working_stop:
                        result["local_open_trades_flat_on_exchange"].append(
                            {
                                "trade_id": trade.id,
                                "pair": trade.pair,
                                "amount": trade.amount,
                            }
                        )
                missing_ids = sorted(
                    (local_open_ids - exchange_order_ids)
                    | (local_sl_ids - exchange_conditional_ids)
                )
                if missing_ids:
                    result["local_open_orders_missing_on_exchange"].append(
                        {"trade_id": trade.id, "pair": trade.pair, "order_ids": missing_ids}
                    )

            # --- Recent terminal orders: the local DB claims a terminal state -
            # verify the exchange agrees (bounded to the newest 50 within 2h) ---
            recent_cutoff = datetime.now(UTC) - timedelta(hours=2)
            recent_orders = (
                Order.session.query(Order)
                .filter(Order.order_filled_date >= recent_cutoff)
                .order_by(Order.order_filled_date.desc())
                .limit(50)
                .all()
            )
            for local_order in recent_orders:
                order_id = str(local_order.order_id)
                if order_id in exchange_order_ids:
                    continue  # still open on the exchange - covered above
                try:
                    ex_order = self.exchange.fetch_order(order_id, local_order.pair)
                except InvalidOrderException:
                    result["recent_closed_order_mismatches"].append(
                        {
                            "order_id": order_id,
                            "pair": local_order.pair,
                            "local_status": local_order.status,
                            "exchange_status": "absent",
                        }
                    )
                    continue
                except Exception as e:
                    logger.debug(f"PM recent-order verification failed for {order_id}: {e}")
                    continue
                ex_status = str((ex_order or {}).get("status") or "")
                local_terminal = {
                    "closed": "closed",
                    "canceled": "canceled",
                    "cancelled": "canceled",
                    "expired": "expired",
                }.get(str(local_order.status).lower(), str(local_order.status).lower())
                if ex_status and local_terminal != str(ex_status).lower():
                    result["recent_closed_order_mismatches"].append(
                        {
                            "order_id": order_id,
                            "pair": local_order.pair,
                            "local_status": local_order.status,
                            "exchange_status": ex_status,
                        }
                    )
        except Exception as e:
            logger.warning(f"PM startup consistency check failed: {e}")
            result["status"] = "error"
            result["error"] = f"{e.__class__.__name__}: {e}"
            return result

        if (
            result["unknown_positions"]
            or result["unknown_orders"]
            or result["unknown_conditional_orders"]
            or result["local_open_trades_flat_on_exchange"]
            or result["local_open_orders_missing_on_exchange"]
            or result["recent_closed_order_mismatches"]
        ):
            result["status"] = "mismatch"
        return result

    def _pm_apply_startup_consistency(self, result: dict[str, Any]) -> None:
        """Apply the configured startup_consistency_mode to a consistency report."""
        if result["status"] == "error":
            # The consistency check itself failed - never start trading blind.
            self.state = State.PAUSED
            self._pm_block_orders("startup_consistency_error")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM FAIL-CLOSED: startup consistency check could not complete "
                        f"({result.get('error')}). Bot PAUSED; new orders BLOCKED."
                    ),
                }
            )
            return
        if result["status"] != "mismatch":
            return

        risk_cfg = self.config.get("exchange", {}).get("portfolio_margin_risk", {})
        mode = str(risk_cfg.get("startup_consistency_mode", "pause")).lower()
        unknown_positions = result.get("unknown_positions", [])
        unknown_orders = result.get("unknown_orders", [])
        unknown_conditional_orders = result.get("unknown_conditional_orders", [])
        local_flat_trades = result.get("local_open_trades_flat_on_exchange", [])
        local_missing_orders = result.get("local_open_orders_missing_on_exchange", [])

        self.rpc.send_msg(
            {
                "type": RPCMessageType.WARNING,
                "status": (
                    "PM STARTUP CONSISTENCY MISMATCH: "
                    f"{len(unknown_positions)} exchange position(s), "
                    f"{len(unknown_orders)} open order(s) and "
                    f"{len(unknown_conditional_orders)} conditional order(s) have no "
                    f"local database match; {len(local_flat_trades)} local open trade(s) "
                    f"are flat on the exchange and {len(local_missing_orders)} local open "
                    f"order(s) are missing on the exchange. Mode={mode}."
                ),
            }
        )

        if mode == "cancel":
            cancelled = 0
            failed = 0
            for order in unknown_orders:
                pair = order.get("symbol")
                order_id = order.get("order_id")
                if not pair or not order_id:
                    failed += 1
                    continue
                try:
                    result = self.exchange.cancel_order(order_id, pair)
                    if isinstance(result, dict) and str(result.get("status") or "").lower() not in {
                        "canceled",
                        "closed",
                    }:
                        # Non-terminal result: the order may still be live -> unsafe.
                        failed += 1
                        logger.warning(
                            f"PM startup consistency: cancel of order {order_id} returned "
                            f"non-terminal status {result.get('status')}."
                        )
                    else:
                        cancelled += 1
                except Exception as e:
                    failed += 1
                    logger.warning(f"Could not cancel unmatched PM order {order_id}: {e}")
            for order in unknown_conditional_orders:
                pair = order.get("symbol")
                strategy_id = order.get("order_id")
                if not pair or not strategy_id:
                    failed += 1
                    continue
                try:
                    # Conditional orders MUST be cancelled through the conditional
                    # DELETE endpoint - the strategy id is not a valid /um/order id.
                    result = self.exchange.cancel_stoploss_order(strategy_id, pair)
                    if isinstance(result, dict) and str(result.get("status") or "").lower() not in {
                        "canceled",
                        "closed",
                    }:
                        failed += 1
                        logger.warning(
                            f"PM startup consistency: cancel of conditional {strategy_id} "
                            f"returned non-terminal status {result.get('status')}."
                        )
                    else:
                        cancelled += 1
                except Exception as e:
                    failed += 1
                    logger.warning(
                        f"Could not cancel unmatched PM conditional order {strategy_id}: {e}"
                    )
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM startup consistency: cancelled {cancelled} unmatched open "
                        f"order(s) ({failed} failures)."
                    ),
                }
            )

            # P0-4: issuing the cancel request is NOT proof of safety. Re-query the
            # exchange: only a clean second check may allow the bot to continue.
            verify = self._pm_startup_consistency_check()
            if verify["status"] == "consistent" and failed == 0:
                logger.info(
                    "PM startup consistency (cancel mode): second check clean; "
                    "proceeding without blocking."
                )
            else:
                self.state = State.PAUSED
                self._pm_block_orders("startup_consistency_mismatch")
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            "PM FAIL-CLOSED (startup cancel mode): "
                            f"{failed} cancel failure(s); second consistency check "
                            f"status={verify['status']}. Bot PAUSED; new orders BLOCKED "
                            "until unknown orders are gone on the exchange."
                        ),
                    }
                )

        if mode in {"pause", "cancel"} and unknown_positions:
            # Unmatched exchange positions cannot be auto-resolved safely - pause.
            self.state = State.PAUSED
            self._pm_block_orders("startup_consistency_mismatch")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM FAIL-CLOSED: exchange positions exist without local trade "
                        "records. Bot PAUSED; new orders BLOCKED. Manual reconciliation "
                        "required before resuming."
                    ),
                }
            )
        elif mode == "pause" and (unknown_orders or unknown_conditional_orders):
            self.state = State.PAUSED
            self._pm_block_orders("startup_consistency_mismatch")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM FAIL-CLOSED: exchange open orders (normal or conditional) "
                        "exist without local order records. Bot PAUSED; new orders "
                        "BLOCKED. Manual reconciliation required before resuming."
                    ),
                }
            )

        if mode == "pause" and (local_flat_trades or local_missing_orders):
            # Reverse direction: the local database claims state the exchange does
            # not confirm. Never auto-mutate - pause and require manual review.
            self.state = State.PAUSED
            self._pm_block_orders("startup_consistency_mismatch")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM FAIL-CLOSED: local database records that the exchange does "
                        "NOT confirm (open trade flat on the exchange / open order "
                        "missing on the exchange). Bot PAUSED; new orders BLOCKED. "
                        "Manual reconciliation required before resuming."
                    ),
                }
            )

    def _pm_cleanup_stream_and_listen_key(self) -> None:
        if hasattr(self.exchange, "stop_pm_user_stream"):
            self.exchange.stop_pm_user_stream()
        if not getattr(self, "_pm_listen_key", ""):
            return
        try:
            if hasattr(self.exchange, "delete_pm_listen_key"):
                self.exchange.delete_pm_listen_key(self._pm_listen_key)
                logger.info("Binance PM listenKey deleted.")
        except Exception as e:
            logger.warning(f"Failed to delete Binance PM listenKey: {e}")

    def cleanup(self) -> None:
        """
        Cleanup pending resources on an already stopped bot
        :return: None
        """
        logger.info("Cleaning up modules ...")
        try:
            # Wrap db activities in shutdown to avoid problems if database is gone,
            # and raises further exceptions.
            if self.config["cancel_open_orders_on_exit"]:
                self.cancel_all_open_orders()

            self.check_for_open_trades()
        except Exception as e:
            logger.warning(f"Exception during cleanup: {e.__class__.__name__} {e}")

        finally:
            self.strategy.ft_bot_cleanup()

        self.rpc.cleanup()
        if self.emc:
            self.emc.shutdown()
        if getattr(self, "exchange", None):
            self._pm_cleanup_stream_and_listen_key()
            self.exchange.close()
        try:
            Trade.commit()
        except Exception:
            # Exceptions here will be happening if the db disappeared.
            # At which point we can no longer commit anyway.
            logger.exception("Error during cleanup")

    def startup(self) -> None:
        """
        Called on startup and after reloading the bot - triggers notifications and
        performs startup tasks
        """
        migrate_live_content(self.config, self.exchange)
        set_startup_time()

        self.rpc.startup_messages(self.config, self.pairlists, self.protections)
        # Update older trades with precision and precision mode
        self.startup_backpopulate_precision()
        # Adjust stoploss if it was changed
        Trade.stoploss_reinitialization(self.strategy.stoploss)

        # Only update open orders on startup
        # This will update the database after the initial migration
        self.startup_update_open_orders()
        self.update_all_liquidation_prices()
        self.update_funding_fees()

        if (
            self.trading_mode == TradingMode.FUTURES
            and not self.config["dry_run"]
            and getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
        ):
            # 1) Resolve persisted order intents from before a crash/restart so a
            #    timeout-uncertain order can never be resubmitted twice.
            self._pm_recover_pending_intents()
            # 2) Rebuild the real-order-id -> strategy-id map from the persistent
            #    local stoploss orders BEFORE the listenKey/user stream may deliver
            #    child-order events (post-restart child events must never be
            #    classified as foreign).
            self._pm_rebuild_actual_order_map()
            # 3) Fail-closed startup consistency check: exchange (PAPI) positions / open
            #    orders / conditional stoploss orders must match the local database
            #    before the bot may open new orders.
            consistency = self._pm_startup_consistency_check()
            self._pm_apply_startup_consistency(consistency)

        if (
            self.trading_mode == TradingMode.FUTURES
            and not self.config["dry_run"]
            and getattr(self, "_pm_listen_key", None) is not None
            and not self._pm_listen_key
        ):
            self._pm_create_listen_key()

    def process(self) -> None:
        """
        Queries the persistence layer for open trades and handles them,
        otherwise a new trade is created.
        :return: True if one or more trades has been created or closed, False otherwise
        """

        # Check whether markets have to be reloaded and reload them when it's needed
        self.exchange.reload_markets()
        self._pm_consume_user_stream_events()

        self.update_trades_without_assigned_fees()

        # Query trades from persistence layer
        trades: list[Trade] = Trade.get_open_trades()

        self.active_pair_whitelist = self._refresh_active_whitelist(trades)

        # Refreshing candles
        self.dataprovider.refresh(
            self.pairlists.create_pair_list(self.active_pair_whitelist),
            self.strategy.gather_informative_pairs(),
        )

        strategy_safe_wrapper(self.strategy.bot_loop_start, supress_error=True)(
            current_time=datetime.now(UTC)
        )

        with self._measure_execution:
            self.strategy.analyze(self.active_pair_whitelist)

        with self._exit_lock:
            # Check for exchange cancellations, timeouts and user requested replace
            self.manage_open_orders()

        # Protect from collisions with force_exit.
        # Without this, freqtrade may try to recreate stoploss_on_exchange orders
        # while exiting is in process, since telegram messages arrive in an different thread.
        with self._exit_lock:
            trades = Trade.get_open_trades()
            # First process current opened trades (positions)
            self.exit_positions(trades)
            Trade.commit()

        # Check if we need to adjust our current positions before attempting to enter new trades.
        if self.strategy.position_adjustment_enable:
            with self._exit_lock:
                self.process_open_trade_positions()

        # Then looking for entry opportunities
        if self.state == State.RUNNING and self.get_free_open_trades():
            self.enter_positions()
        self._schedule.run_pending()
        Trade.commit()
        self.rpc.process_msg_queue(self.dataprovider._msg_queue)
        self.last_process = datetime.now(UTC)

    def process_stopped(self) -> None:
        """
        Close all orders that were left open
        """
        if self.config["cancel_open_orders_on_exit"]:
            self.cancel_all_open_orders()

    def check_for_open_trades(self):
        """
        Notify the user when the bot is stopped (not reloaded)
        and there are still open trades active.
        """
        open_trades = Trade.get_open_trades()

        if len(open_trades) != 0 and self.state != State.RELOAD_CONFIG:
            msg = {
                "type": RPCMessageType.WARNING,
                "status": f"{len(open_trades)} open trades active.\n\n"
                f"Handle these trades manually on {self.exchange.name}, "
                f"or '/start' the bot again and use '/stopentry' "
                f"to handle open trades gracefully. \n"
                f"{'Note: Trades are simulated (dry run).' if self.config['dry_run'] else ''}",
            }
            self.rpc.send_msg(msg)

    def _refresh_active_whitelist(self, trades: list[Trade] | None = None) -> list[str]:
        """
        Refresh active whitelist from pairlist and extend it with
        pairs that have open trades.
        """
        # Refresh whitelist
        _prev_whitelist = self.pairlists.whitelist
        self.pairlists.refresh_pairlist()
        _whitelist = self.pairlists.whitelist

        if trades:
            # Extend active-pair whitelist with pairs of open trades
            # It ensures that candle (OHLCV) data are downloaded for open trades as well
            _whitelist.extend([trade.pair for trade in trades if trade.pair not in _whitelist])

        # Called last to include the included pairs
        if _prev_whitelist != _whitelist:
            self.rpc.send_msg({"type": RPCMessageType.WHITELIST, "data": _whitelist})

        return _whitelist

    def get_free_open_trades(self) -> int:
        """
        Return the number of free open trades slots or 0 if
        max number of open trades reached
        """
        open_trades = Trade.get_open_trade_count()
        return max(0, self.config["max_open_trades"] - open_trades)

    def update_all_liquidation_prices(self) -> None:
        if self.trading_mode == TradingMode.FUTURES and self.margin_mode == MarginMode.CROSS:
            # Update liquidation prices for all trades in cross margin mode
            update_liquidation_prices(
                exchange=self.exchange,
                wallets=self.wallets,
                stake_currency=self.config["stake_currency"],
                dry_run=self.config["dry_run"],
            )

    def update_funding_fees(self) -> None:
        if self.trading_mode == TradingMode.FUTURES:
            trades: list[Trade] = Trade.get_open_trades()
            for trade in trades:
                trade.set_funding_fees(
                    self.exchange.get_funding_fees(
                        pair=trade.pair,
                        amount=trade.amount,
                        is_short=trade.is_short,
                        open_date=trade.date_last_filled_utc,
                    )
                )

    def startup_backpopulate_precision(self) -> None:
        trades = Trade.get_trades([Trade.contract_size.is_(None)])
        for trade in trades:
            if trade.exchange != self.exchange.id:
                continue
            trade.precision_mode = self.exchange.precisionMode
            trade.precision_mode_price = self.exchange.precision_mode_price
            trade.amount_precision = self.exchange.get_precision_amount(trade.pair)
            trade.price_precision = self.exchange.get_precision_price(trade.pair)
            trade.contract_size = self.exchange.get_contract_size(trade.pair)
        Trade.commit()

    def startup_update_open_orders(self):
        """
        Updates open orders based on order list kept in the database.
        Mainly updates the state of orders - but may also close trades
        """
        if self.config["dry_run"] or self.config["exchange"].get("skip_open_order_update", False):
            # Updating open orders in dry-run does not make sense and will fail.
            return

        orders = Order.get_open_orders()
        logger.info(f"Updating {len(orders)} open orders.")
        for order in orders:
            try:
                fo = self.exchange.fetch_order_or_stoploss_order(
                    order.order_id, order.ft_pair, order.ft_order_side == "stoploss"
                )
                if not order.trade:
                    # This should not happen, but it does if trades were deleted manually.
                    # This can only incur on sqlite, which doesn't enforce foreign constraints.
                    logger.warning(
                        f"Order {order.order_id} has no trade attached. "
                        "This may suggest a database corruption. "
                        f"The expected trade ID is {order.ft_trade_id}. Ignoring this order."
                    )
                    continue
                self.update_trade_state(
                    order.trade,
                    order.order_id,
                    fo,
                    stoploss_order=(order.ft_order_side == "stoploss"),
                )

            except InvalidOrderException as e:
                logger.warning(f"Error updating Order {order.order_id} due to {e}.")
                if order.order_date_utc - timedelta(days=5) < datetime.now(UTC):
                    logger.warning(
                        "Order is older than 5 days. Assuming order was fully cancelled."
                    )
                    fo = order.to_ccxt_object()
                    fo["status"] = "canceled"
                    self.handle_cancel_order(
                        fo, order, order.trade, constants.CANCEL_REASON["TIMEOUT"]
                    )

            except ExchangeError as e:
                logger.warning(f"Error updating Order {order.order_id} due to {e}")

    def update_trades_without_assigned_fees(self) -> None:
        """
        Update closed trades without close fees assigned.
        Only acts when Orders are in the database, otherwise the last order-id is unknown.
        """
        if self.config["dry_run"]:
            # Updating open orders in dry-run does not make sense and will fail.
            return

        trades: list[Trade] = Trade.get_closed_trades_without_assigned_fees()
        for trade in trades:
            if not trade.is_open and not trade.fee_updated(trade.exit_side):
                # Get sell fee
                order = trade.select_order(trade.exit_side, False, only_filled=True)
                if not order:
                    order = trade.select_order("stoploss", False)
                if order:
                    logger.info(
                        f"Updating {trade.exit_side}-fee on trade {trade} "
                        f"for order {order.order_id}."
                    )
                    self.update_trade_state(
                        trade,
                        order.order_id,
                        stoploss_order=order.ft_order_side == "stoploss",
                        send_msg=False,
                    )

        trades = Trade.get_open_trades_without_assigned_fees()
        for trade in trades:
            with self._exit_lock:
                if trade.is_open and not trade.fee_updated(trade.entry_side):
                    order = trade.select_order(trade.entry_side, False, only_filled=True)
                    open_order = trade.select_order(trade.entry_side, True)
                    if order and open_order is None:
                        logger.info(
                            f"Updating {trade.entry_side}-fee on trade {trade} "
                            f"for order {order.order_id}."
                        )
                        self.update_trade_state(trade, order.order_id, send_msg=False)

    def handle_insufficient_funds(self, trade: Trade):
        """
        Try refinding a lost trade.
        Only used when InsufficientFunds appears on exit orders (stoploss or long sell/short buy).
        Tries to walk the stored orders and updates the trade state if necessary.
        """
        logger.info(f"Trying to refind lost order for {trade}")
        for order in trade.orders:
            logger.info(f"Trying to refind {order}")
            fo = None
            if not order.ft_is_open:
                logger.debug(f"Order {order} is no longer open.")
                continue
            try:
                fo = self.exchange.fetch_order_or_stoploss_order(
                    order.order_id, order.ft_pair, order.ft_order_side == "stoploss"
                )
                if fo:
                    logger.info(f"Found {order} for trade {trade}.")
                    self.update_trade_state(
                        trade, order.order_id, fo, stoploss_order=order.ft_order_side == "stoploss"
                    )

            except ExchangeError:
                logger.warning(f"Error updating {order.order_id}.")

    def handle_onexchange_order(self, trade: Trade) -> bool:
        """
        Try refinding a order that is not in the database.
        Only used balance disappeared, which would make exiting impossible.
        :return: True if the trade was deleted, False otherwise
        """
        try:
            orders = self.exchange.fetch_orders(
                trade.pair, trade.open_date_utc - timedelta(seconds=10)
            )
            prev_exit_reason = trade.exit_reason
            prev_trade_state = trade.is_open
            prev_trade_amount = trade.amount
            for order in orders:
                trade_order = [o for o in trade.orders if o.order_id == order["id"]]

                if trade_order:
                    # We knew this order, but didn't have it updated properly
                    order_obj = trade_order[0]
                else:
                    logger.info(f"Found previously unknown order {order['id']} for {trade.pair}.")

                    order_obj = Order.parse_from_ccxt_object(order, trade.pair, order["side"])
                    order_obj.order_filled_date = dt_from_ts(
                        safe_value_fallback(order, "lastTradeTimestamp", "timestamp")
                    )
                    trade.orders.append(order_obj)
                    Trade.commit()
                    trade.exit_reason = ExitType.SOLD_ON_EXCHANGE.value

                self.update_trade_state(trade, order["id"], order, send_msg=False)

                logger.info(f"handled order {order['id']}")

            # Refresh trade from database
            Trade.session.refresh(trade)
            if not trade.is_open:
                # Trade was just closed
                trade.close_date = trade.date_last_filled_utc
                self.order_close_notify(
                    trade,
                    order_obj,
                    order_obj.ft_order_side == "stoploss",
                    send_msg=prev_trade_state != trade.is_open,
                )
            else:
                trade.exit_reason = prev_exit_reason
                total = (
                    self.wallets.get_owned(trade.pair, trade.base_currency)
                    if trade.base_currency
                    else 0
                )
                if total < trade.amount:
                    if trade.fully_canceled_entry_order_count == len(trade.orders):
                        logger.warning(
                            f"Trade only had fully canceled entry orders. "
                            f"Removing {trade} from database."
                        )

                        self._notify_enter_cancel(
                            trade,
                            order_type=self.strategy.order_types["entry"],
                            reason=constants.CANCEL_REASON["FULLY_CANCELLED"],
                        )
                        trade.delete()
                        return True
                    if total > trade.amount * 0.98:
                        logger.warning(
                            f"{trade} has a total of {trade.amount} {trade.base_currency}, "
                            f"but the Wallet shows a total of {total} {trade.base_currency}. "
                            f"Adjusting trade amount to {total}. "
                            "This may however lead to further issues."
                        )
                        trade.amount = total
                    else:
                        logger.warning(
                            f"{trade} has a total of {trade.amount} {trade.base_currency}, "
                            f"but the Wallet shows a total of {total} {trade.base_currency}. "
                            "Refusing to adjust as the difference is too large. "
                            "This may however lead to further issues."
                        )
                if prev_trade_amount != trade.amount:
                    # Cancel stoploss on exchange if the amount changed
                    trade = self.cancel_stoploss_on_exchange(trade)
            Trade.commit()

        except ExchangeError:
            logger.warning("Error finding onexchange order.")
        except Exception:
            # catching https://github.com/freqtrade/freqtrade/issues/9025
            logger.warning("Error finding onexchange order", exc_info=True)
        return False

    #
    # enter positions / open trades logic and methods
    #

    def enter_positions(self) -> int:
        """
        Tries to execute entry orders for new trades (positions)
        """
        trades_created = 0

        if self._pm_blocked_order_reasons():
            self.log_once(
                "Not creating new trades. PM orders blocked: "
                + ", ".join(self._pm_blocked_order_reasons()),
                logger.info,
            )
            return trades_created

        whitelist = deepcopy(self.active_pair_whitelist)
        if not whitelist:
            self.log_once("Active pair whitelist is empty.", logger.info)
            return trades_created
        # Remove pairs for currently opened trades from the whitelist
        for trade in Trade.get_open_trades():
            if trade.pair in whitelist:
                whitelist.remove(trade.pair)
                logger.debug("Ignoring %s in pair whitelist", trade.pair)

        if not whitelist:
            self.log_once(
                "No currency pair in active pair whitelist, but checking to exit open trades.",
                logger.info,
            )
            return trades_created
        if PairLocks.is_global_lock(side="*"):
            # This only checks for total locks (both sides).
            # per-side locks will be evaluated by `is_pair_locked` within create_trade,
            # once the direction for the trade is clear.
            lock = PairLocks.get_pair_longest_lock("*")
            if lock:
                self.log_once(
                    f"Global pairlock active until "
                    f"{lock.lock_end_time.strftime(constants.DATETIME_PRINT_FORMAT)}. "
                    f"Not creating new trades, reason: {lock.reason}.",
                    logger.info,
                )
            else:
                self.log_once("Global pairlock active. Not creating new trades.", logger.info)
            return trades_created
        # Create entity and execute trade for each pair from whitelist
        for pair in whitelist:
            try:
                with self._exit_lock:
                    trades_created += self.create_trade(pair)
            except DependencyException as exception:
                logger.warning("Unable to create trade for %s: %s", pair, exception)

        if not trades_created:
            logger.debug("Found no enter signals for whitelisted currencies. Trying again...")

        return trades_created

    def create_trade(self, pair: str) -> bool:
        """
        Check the implemented trading strategy for entry signals.

        If the pair triggers the enter signal a new trade record gets created
        and the entry-order opening the trade gets issued towards the exchange.

        :return: True if a trade has been created.
        """
        logger.debug(f"create_trade for pair {pair}")

        analyzed_df, _ = self.dataprovider.get_analyzed_dataframe(pair, self.strategy.timeframe)
        nowtime = analyzed_df.iloc[-1]["date"] if len(analyzed_df) > 0 else None

        # get_free_open_trades is checked before create_trade is called
        # but it is still used here to prevent opening too many trades within one iteration
        if not self.get_free_open_trades():
            logger.debug(f"Can't open a new trade for {pair}: max number of trades is reached.")
            return False

        # running get_signal on historical data fetched
        (signal, enter_tag) = self.strategy.get_entry_signal(
            pair, self.strategy.timeframe, analyzed_df
        )

        if signal:
            if self.strategy.is_pair_locked(pair, candle_date=nowtime, side=signal):
                lock = PairLocks.get_pair_longest_lock(pair, nowtime, signal)
                if lock:
                    self.log_once(
                        f"Pair {pair} {lock.side} is locked until "
                        f"{lock.lock_end_time.strftime(constants.DATETIME_PRINT_FORMAT)} "
                        f"due to {lock.reason}.",
                        logger.info,
                    )
                else:
                    self.log_once(f"Pair {pair} is currently locked.", logger.info)
                # PM signal decision ledger: a locked entry is a blocked
                # decision for this closed candle (write-once).
                self._pm_record_signal_decision(pair, decision="blocked", reason="pair_locked")
                return False
            stake_amount = self.wallets.get_trade_stake_amount(pair, self.config["max_open_trades"])

            bid_check_dom = self.config.get("entry_pricing", {}).get("check_depth_of_market", {})
            if (bid_check_dom.get("enabled", False)) and (
                bid_check_dom.get("bids_to_ask_delta", 0) > 0
            ):
                if self._check_depth_of_market(pair, bid_check_dom, side=signal):
                    return self.execute_entry(
                        pair,
                        stake_amount,
                        enter_tag=enter_tag,
                        is_short=(signal == SignalDirection.SHORT),
                    )
                else:
                    return False

            return self.execute_entry(
                pair, stake_amount, enter_tag=enter_tag, is_short=(signal == SignalDirection.SHORT)
            )
        else:
            # PM signal decision ledger: record the no-signal decision for this
            # closed candle (write-once per candle per pair) so the ledger
            # covers ALL four decision classes - no_signal / blocked /
            # submitted / exit - and restarts are idempotent.
            self._pm_record_signal_decision(pair, decision="no_signal", reason="no_entry_signal")
            return False

    #
    # Modify positions / DCA logic and methods
    #
    def process_open_trade_positions(self):
        """
        Tries to execute additional buy or sell orders for open trades (positions)
        """
        # Walk through each pair and check if it needs changes
        for trade in Trade.get_open_trades():
            # If there is any open orders, wait for them to finish.
            # TODO Remove to allow mul open orders
            if trade.has_open_position or trade.has_open_orders:
                # Do a wallets update (will be ratelimited to once per hour)
                self.wallets.update(False)
                try:
                    self.check_and_call_adjust_trade_position(trade)
                except DependencyException as exception:
                    logger.warning(
                        f"Unable to adjust position of trade for {trade.pair}: {exception}"
                    )

    def check_and_call_adjust_trade_position(self, trade: Trade):
        """
        Check the implemented trading strategy for adjustment command.
        If the strategy triggers the adjustment, a new order gets issued.
        Once that completes, the existing trade is modified to match new data.
        """
        current_entry_rate, current_exit_rate = self.exchange.get_rates(
            trade.pair, True, trade.is_short
        )

        current_entry_profit = trade.calc_profit_ratio(current_entry_rate)
        current_exit_profit = trade.calc_profit_ratio(current_exit_rate)

        min_entry_stake = self.exchange.get_min_pair_stake_amount(
            trade.pair, current_entry_rate, 0.0, trade.leverage
        )
        min_exit_stake = self.exchange.get_min_pair_stake_amount(
            trade.pair, current_exit_rate, self.strategy.stoploss, trade.leverage
        )
        max_entry_stake = self.exchange.get_max_pair_stake_amount(
            trade.pair, current_entry_rate, trade.leverage
        )
        stake_available = self.wallets.get_available_stake_amount()
        logger.debug(f"Calling adjust_trade_position for pair {trade.pair}")
        stake_amount, order_tag = self.strategy._adjust_trade_position_internal(
            trade=trade,
            current_time=datetime.now(UTC),
            current_rate=current_entry_rate,
            current_profit=current_entry_profit,
            min_stake=min_entry_stake,
            max_stake=min(max_entry_stake, stake_available),
            current_entry_rate=current_entry_rate,
            current_exit_rate=current_exit_rate,
            current_entry_profit=current_entry_profit,
            current_exit_profit=current_exit_profit,
        )

        if stake_amount is not None and stake_amount > 0.0:
            if self.state == State.PAUSED:
                logger.debug("Position adjustment aborted because the bot is in PAUSED state")
                return

            # We should increase our position
            if self.strategy.max_entry_position_adjustment > -1:
                count_of_entries = trade.nr_of_successful_entries
                if count_of_entries > self.strategy.max_entry_position_adjustment:
                    logger.debug(f"Max adjustment entries for {trade.pair} has been reached.")
                    return
                else:
                    logger.debug("Max adjustment entries is set to unlimited.")

            self.execute_entry(
                trade.pair,
                stake_amount,
                price=current_entry_rate,
                trade=trade,
                is_short=trade.is_short,
                mode="pos_adjust",
                enter_tag=order_tag,
            )

        if stake_amount is not None and stake_amount < 0.0:
            # We should decrease our position
            amount = self.exchange.amount_to_contract_precision(
                trade.pair,
                abs(
                    float(
                        FtPrecise(stake_amount)
                        * FtPrecise(trade.amount)
                        / FtPrecise(trade.stake_amount)
                    )
                ),
            )

            if amount == 0.0:
                logger.info(
                    f"Wanted to exit of {stake_amount} amount, "
                    "but exit amount is now 0.0 due to exchange limits - not exiting."
                )
                return

            remaining = (trade.amount - amount) * current_exit_rate
            if min_exit_stake and remaining != 0 and remaining < min_exit_stake:
                logger.info(
                    f"Remaining amount of {remaining} would be smaller "
                    f"than the minimum of {min_exit_stake}."
                )
                return

            self.execute_trade_exit(
                trade,
                current_exit_rate,
                exit_check=ExitCheckTuple(exit_type=ExitType.PARTIAL_EXIT),
                sub_trade_amt=amount,
                exit_tag=order_tag,
            )

    def _check_depth_of_market(self, pair: str, conf: dict, side: SignalDirection) -> bool:
        """
        Checks depth of market before executing an entry
        """
        conf_bids_to_ask_delta = conf.get("bids_to_ask_delta", 0)
        logger.info(f"Checking depth of market for {pair} ...")
        order_book = self.exchange.fetch_l2_order_book(pair, 1000)
        order_book_data_frame = order_book_to_dataframe(order_book["bids"], order_book["asks"])
        order_book_bids = order_book_data_frame["b_size"].sum()
        order_book_asks = order_book_data_frame["a_size"].sum()

        entry_side = order_book_bids if side == SignalDirection.LONG else order_book_asks
        exit_side = order_book_asks if side == SignalDirection.LONG else order_book_bids
        bids_ask_delta = entry_side / exit_side

        bids = f"Bids: {order_book_bids}"
        asks = f"Asks: {order_book_asks}"
        delta = f"Delta: {bids_ask_delta}"

        logger.info(
            f"{bids}, {asks}, {delta}, Direction: {side.value} "
            f"Bid Price: {order_book['bids'][0][0]}, Ask Price: {order_book['asks'][0][0]}, "
            f"Immediate Bid Quantity: {order_book['bids'][0][1]}, "
            f"Immediate Ask Quantity: {order_book['asks'][0][1]}."
        )
        if bids_ask_delta >= conf_bids_to_ask_delta:
            logger.info(f"Bids to asks delta for {pair} DOES satisfy condition.")
            return True
        else:
            logger.info(f"Bids to asks delta for {pair} does not satisfy condition.")
            return False

    def execute_entry(
        self,
        pair: str,
        stake_amount: float,
        price: float | None = None,
        *,
        is_short: bool = False,
        ordertype: str | None = None,
        enter_tag: str | None = None,
        trade: Trade | None = None,
        mode: EntryExecuteMode = "initial",
        leverage_: float | None = None,
    ) -> bool:
        """
        Executes an entry for the given pair
        :param pair: pair for which we want to create a LIMIT order
        :param stake_amount: amount of stake-currency for the pair
        :return: True if an entry order is created, False if it fails.
        :raise: DependencyException or it's subclasses like ExchangeError.
        """
        # Fail-closed PM gate - single unified entry for strategy entries, DCA / position
        # adjustments, Telegram/API force-entry and recovery re-entries. Never increase
        # risk exposure while the PM user stream is unavailable, the startup consistency
        # check failed, or any other configured order-block reason is active.
        if self._pm_blocked_order_reasons():
            self.log_once(
                "Refusing to open a new entry. PM orders blocked: "
                + ", ".join(self._pm_blocked_order_reasons()),
                logger.warning,
            )
            if mode == "initial":
                self._pm_record_signal_decision(
                    pair, decision="blocked", reason="; ".join(self._pm_blocked_order_reasons())
                )
            return False

        time_in_force = self.strategy.order_time_in_force["entry"]

        side: BuySell = "sell" if is_short else "buy"
        name = "Short" if is_short else "Long"
        trade_side: LongShort = "short" if is_short else "long"
        pos_adjust = trade is not None

        enter_limit_requested, stake_amount, leverage = self.get_valid_enter_price_and_stake(
            pair, price, stake_amount, trade_side, enter_tag, trade, mode, leverage_
        )

        if not stake_amount:
            return False

        msg = (
            f"Position adjust: about to create a new order for {pair} with stake_amount: "
            f"{stake_amount} and price: {enter_limit_requested} for {trade}"
            if mode == "pos_adjust"
            else (
                f"Replacing {side} order: about create a new order for {pair} with stake_amount: "
                f"{stake_amount} and price: {enter_limit_requested} ..."
                if mode == "replace"
                else f"{name} signal found: about create a new trade for {pair} with stake_amount: "
                f"{stake_amount} and price: {enter_limit_requested} ..."
            )
        )
        logger.info(msg)
        amount = (stake_amount / enter_limit_requested) * leverage
        order_type = ordertype or self.strategy.order_types["entry"]

        if mode == "initial" and not strategy_safe_wrapper(
            self.strategy.confirm_trade_entry, default_retval=True
        )(
            pair=pair,
            order_type=order_type,
            amount=amount,
            rate=enter_limit_requested,
            time_in_force=time_in_force,
            current_time=datetime.now(UTC),
            entry_tag=enter_tag,
            side=trade_side,
        ):
            logger.info(f"User denied entry for {pair}.")
            return False

        if trade and self.handle_similar_open_order(trade, enter_limit_requested, amount, side):
            return False

        # PM unified data-failure policy: a signal evaluated on unhealthy or
        # non-contiguous data must never reach the exchange (fail-closed). The
        # decision is recorded in the signal ledger for audit.
        if mode == "initial":
            _snapshot, _block_reason = self._pm_ledger_snapshot(pair)
            if _block_reason:
                self._pm_record_signal_decision(
                    pair, decision="blocked_data", reason=_block_reason
                )
                logger.warning(
                    f"Refusing PM entry for {pair}: data not usable ({_block_reason}); "
                    "unified fail-closed policy."
                )
                return False

        order = self.exchange.create_order(
            pair=pair,
            ordertype=order_type,
            side=side,
            amount=amount,
            rate=enter_limit_requested,
            reduceOnly=False,
            time_in_force=time_in_force,
            leverage=leverage,
            initial_order=trade is None,
            entry_mode=mode,
        )
        order_obj = Order.parse_from_ccxt_object(order, pair, side, amount, enter_limit_requested)
        order_obj.ft_order_tag = enter_tag
        order_id = order["id"]
        order_status = order.get("status")
        logger.info(f"Order {order_id} was created for {pair} and status is {order_status}.")

        # PM signal decision ledger: record the submitted decision for this
        # closed candle (write-once) with the exchange client id.
        if mode == "initial":
            self._pm_record_signal_decision(
                pair,
                decision="entry_submitted",
                reason=f"order_id={order_id}",
                order_client_id=str(order.get("clientOrderId") or ""),
            )

        # we assume the order is executed at the price requested
        enter_limit_filled_price = enter_limit_requested
        amount_requested = amount

        if order_status == "expired" or order_status == "rejected":
            # return false if the order is not filled
            if float(order["filled"]) == 0:
                logger.warning(
                    f"{name} {time_in_force} order with time in force {order_type} "
                    f"for {pair} is {order_status} by {self.exchange.name}."
                    " zero amount is fulfilled."
                )
                return False
            else:
                # the order is partially fulfilled
                # in case of IOC orders we can check immediately
                # if the order is fulfilled fully or partially
                logger.warning(
                    "%s %s order with time in force %s for %s is %s by %s."
                    " %s amount fulfilled out of %s (%s remaining which is canceled).",
                    name,
                    time_in_force,
                    order_type,
                    pair,
                    order_status,
                    self.exchange.name,
                    order["filled"],
                    order["amount"],
                    order["remaining"],
                )
                amount = safe_value_fallback(order, "filled", "amount", amount)
                enter_limit_filled_price = safe_value_fallback(
                    order, "average", "price", enter_limit_filled_price
                )

        # in case of FOK the order may be filled immediately and fully
        elif order_status == "closed":
            amount = safe_value_fallback(order, "filled", "amount", amount)
            enter_limit_filled_price = safe_value_fallback(
                order, "average", "price", enter_limit_requested
            )

        # Fee is applied twice because we make a LIMIT_BUY and LIMIT_SELL
        fee = self.exchange.get_fee(symbol=pair, taker_or_maker="maker")
        base_currency = self.exchange.get_pair_base_currency(pair)
        open_date = datetime.now(UTC)

        funding_fees = self.exchange.get_funding_fees(
            pair=pair,
            amount=amount + trade.amount if trade else amount,
            is_short=is_short,
            open_date=trade.date_last_filled_utc if trade else open_date,
        )

        # This is a new trade
        if trade is None:
            trade = Trade(
                pair=pair,
                base_currency=base_currency,
                stake_currency=self.config["stake_currency"],
                stake_amount=stake_amount,
                amount=0,
                is_open=True,
                amount_requested=amount_requested,
                fee_open=fee,
                fee_close=fee,
                open_rate=enter_limit_filled_price,
                open_rate_requested=enter_limit_requested,
                open_date=open_date,
                exchange=self.exchange.id,
                strategy=self.strategy.get_strategy_name(),
                enter_tag=enter_tag,
                timeframe=timeframe_to_minutes(self.config["timeframe"]),
                leverage=leverage,
                is_short=is_short,
                trading_mode=self.trading_mode,
                funding_fees=funding_fees,
                amount_precision=self.exchange.get_precision_amount(pair),
                price_precision=self.exchange.get_precision_price(pair),
                precision_mode=self.exchange.precisionMode,
                precision_mode_price=self.exchange.precision_mode_price,
                contract_size=self.exchange.get_contract_size(pair),
            )
            stoploss = self.strategy.stoploss
            trade.adjust_stop_loss(trade.open_rate, stoploss, initial=True)

        else:
            trade.is_open = True
            trade.set_funding_fees(funding_fees)

        trade.orders.append(order_obj)
        trade.recalc_trade_from_orders()
        Trade.session.add(trade)
        # PM order-consistency: mark the intent LINKED in the SAME transaction as
        # the local Trade/Order commit, so the intent can never be tombstoned
        # before the local rows exist (crash between exchange ACK and this commit
        # leaves the intent ACKED with full evidence and blocks new exposure).
        self._pm_link_intent_for_order(order, trade, str(order_obj.order_id))
        Trade.commit()

        # Updating wallets
        self.wallets.update()

        self._notify_enter(trade, order_obj, order_type, sub_trade=pos_adjust)

        if pos_adjust:
            if order_status == "closed":
                logger.info(f"DCA order closed, trade should be up to date: {trade}")
                trade = self.cancel_stoploss_on_exchange(trade)
            else:
                logger.info(f"DCA order {order_status}, will wait for resolution: {trade}")

        # Update fees if order is non-opened
        if order_status in constants.NON_OPEN_EXCHANGE_STATES:
            fully_canceled = self.update_trade_state(trade, order_id, order)
            if fully_canceled and mode != "replace":
                # Fully canceled orders, may happen with some time in force setups (IOC).
                # Should be handled immediately.
                self.handle_cancel_enter(
                    trade, order, order_obj, constants.CANCEL_REASON["TIMEOUT"]
                )

        return True

    def cancel_stoploss_on_exchange(self, trade: Trade, allow_nonblocking: bool = False) -> Trade:
        """
        Cancels on exchange stoploss orders for the given trade.
        :param trade: Trade for which to cancel stoploss order
        :param allow_nonblocking: If True, will skip cancelling stoploss on exchange
                                   if the exchange supports blocking stoploss orders.
        """
        if allow_nonblocking and not self.exchange.get_option("stoploss_blocks_assets", True):
            logger.info(f"Skipping cancelling stoploss on exchange for {trade}.")
            return trade
        # First cancelling stoploss on exchange ...
        for oslo in trade.open_sl_orders:
            try:
                logger.info(f"Cancelling stoploss on exchange for {trade} order: {oslo.order_id}")
                co = self.exchange.cancel_stoploss_order_with_result(
                    oslo.order_id, trade.pair, trade.amount
                )
                self.update_trade_state(trade, oslo.order_id, co, stoploss_order=True)
            except InvalidOrderException:
                # The exchange definitively no longer has this conditional
                # (already triggered/canceled, e.g. auto-removed when the
                # position went flat). Mark it canceled locally so the trade
                # state stays consistent and redelivered stream events stop
                # retrying the cancel forever.
                logger.warning(
                    f"Stoploss order {oslo.order_id} for pair {trade.pair} no longer "
                    "exists on the exchange; marking it canceled locally."
                )
                oslo.ft_is_open = False
                oslo.status = "canceled"
        return trade

    def get_valid_enter_price_and_stake(
        self,
        pair: str,
        price: float | None,
        stake_amount: float,
        trade_side: LongShort,
        entry_tag: str | None,
        trade: Trade | None,
        mode: EntryExecuteMode,
        leverage_: float | None,
    ) -> tuple[float, float, float]:
        """
        Validate and eventually adjust (within limits) limit, amount and leverage
        :return: Tuple with (price, amount, leverage)
        """

        if price:
            enter_limit_requested = price
        else:
            # Calculate price
            enter_limit_requested = self.exchange.get_rate(
                pair, side="entry", is_short=(trade_side == "short"), refresh=True
            )
        if mode != "replace":
            # Don't call custom_entry_price in order-adjust scenario
            custom_entry_price = strategy_safe_wrapper(
                self.strategy.custom_entry_price, default_retval=enter_limit_requested
            )(
                pair=pair,
                trade=trade,
                current_time=datetime.now(UTC),
                proposed_rate=enter_limit_requested,
                entry_tag=entry_tag,
                side=trade_side,
            )

            enter_limit_requested = self.get_valid_price(custom_entry_price, enter_limit_requested)

        if not enter_limit_requested:
            raise PricingError("Could not determine entry price.")

        if self.trading_mode != TradingMode.SPOT and trade is None:
            max_leverage = self.exchange.get_max_leverage(pair, stake_amount)
            if leverage_:
                leverage = leverage_
            else:
                leverage = strategy_safe_wrapper(self.strategy.leverage, default_retval=1.0)(
                    pair=pair,
                    current_time=datetime.now(UTC),
                    current_rate=enter_limit_requested,
                    proposed_leverage=1.0,
                    max_leverage=max_leverage,
                    side=trade_side,
                    entry_tag=entry_tag,
                )
            # Cap leverage between 1.0 and max_leverage.
            leverage = min(max(leverage, 1.0), max_leverage)
        else:
            # Changing leverage currently not possible
            leverage = trade.leverage if trade else 1.0

        # Min-stake-amount should actually include Leverage - this way our "minimal"
        # stake- amount might be higher than necessary.
        # We do however also need min-stake to determine leverage, therefore this is ignored as
        # edge-case for now.
        min_stake_amount = self.exchange.get_min_pair_stake_amount(
            pair,
            enter_limit_requested,
            self.strategy.stoploss if not mode == "pos_adjust" else 0.0,
            leverage,
        )
        max_stake_amount = self.exchange.get_max_pair_stake_amount(
            pair, enter_limit_requested, leverage
        )

        if trade is None:
            stake_available = self.wallets.get_available_stake_amount()
            stake_amount = strategy_safe_wrapper(
                self.strategy.custom_stake_amount, default_retval=stake_amount
            )(
                pair=pair,
                current_time=datetime.now(UTC),
                current_rate=enter_limit_requested,
                proposed_stake=stake_amount,
                min_stake=min_stake_amount,
                max_stake=min(max_stake_amount, stake_available),
                leverage=leverage,
                entry_tag=entry_tag,
                side=trade_side,
            )

        stake_amount = self.wallets.validate_stake_amount(
            pair=pair,
            stake_amount=stake_amount,
            min_stake_amount=min_stake_amount,
            max_stake_amount=max_stake_amount,
            trade_amount=trade.stake_amount if trade else None,
        )

        return enter_limit_requested, stake_amount, leverage

    def _notify_enter(
        self,
        trade: Trade,
        order: Order,
        order_type: str | None,
        fill: bool = False,
        sub_trade: bool = False,
    ) -> None:
        """
        Sends rpc notification when a entry order occurred.
        """
        open_rate = order.safe_price

        if open_rate is None:
            open_rate = trade.open_rate

        current_rate = self.exchange.get_rate(
            trade.pair, side="entry", is_short=trade.is_short, refresh=False
        )
        stake_amount = trade.stake_amount
        if not fill and trade.nr_of_successful_entries > 0:
            # If we have open orders, we need to add the stake amount of the open orders
            # as it's not yet included in the trade.stake_amount
            stake_amount += sum(
                o.stake_amount for o in trade.open_orders if o.ft_order_side == trade.entry_side
            )

        msg: RPCEntryMsg = {
            "trade_id": trade.id,
            "type": RPCMessageType.ENTRY_FILL if fill else RPCMessageType.ENTRY,
            "buy_tag": trade.enter_tag,
            "enter_tag": trade.enter_tag,
            "exchange": trade.exchange.capitalize(),
            "pair": trade.pair,
            "leverage": trade.leverage if trade.leverage else None,
            "direction": "Short" if trade.is_short else "Long",
            "limit": open_rate,  # Deprecated (?)
            "order_rate": open_rate,
            "open_rate": open_rate,
            "order_type": order_type or "unknown",
            "stake_amount": stake_amount,
            "stake_currency": self.config["stake_currency"],
            "base_currency": self.exchange.get_pair_base_currency(trade.pair),
            "quote_currency": self.exchange.get_pair_quote_currency(trade.pair),
            "fiat_currency": self.config.get("fiat_display_currency", None),
            "amount": order.safe_amount_after_fee if fill else (order.safe_amount or trade.amount),
            "open_date": trade.open_date_utc or datetime.now(UTC),
            "current_rate": current_rate,
            "sub_trade": sub_trade,
        }

        # Send the message
        self.rpc.send_msg(msg)

    def _notify_enter_cancel(
        self, trade: Trade, order_type: str, reason: str, sub_trade: bool = False
    ) -> None:
        """
        Sends rpc notification when a entry order cancel occurred.
        """
        current_rate = self.exchange.get_rate(
            trade.pair, side="entry", is_short=trade.is_short, refresh=False
        )

        msg: RPCCancelMsg = {
            "trade_id": trade.id,
            "type": RPCMessageType.ENTRY_CANCEL,
            "buy_tag": trade.enter_tag,
            "enter_tag": trade.enter_tag,
            "exchange": trade.exchange.capitalize(),
            "pair": trade.pair,
            "leverage": trade.leverage,
            "direction": "Short" if trade.is_short else "Long",
            "limit": trade.open_rate,
            "order_rate": trade.open_rate,
            "order_type": order_type,
            "stake_amount": trade.stake_amount,
            "open_rate": trade.open_rate,
            "stake_currency": self.config["stake_currency"],
            "base_currency": self.exchange.get_pair_base_currency(trade.pair),
            "quote_currency": self.exchange.get_pair_quote_currency(trade.pair),
            "fiat_currency": self.config.get("fiat_display_currency", None),
            "amount": trade.amount,
            "open_date": trade.open_date,
            "current_rate": current_rate,
            "reason": reason,
            "sub_trade": sub_trade,
        }

        # Send the message
        self.rpc.send_msg(msg)

    #
    # SELL / exit positions / close trades logic and methods
    #

    def exit_positions(self, trades: list[Trade]) -> int:
        """
        Tries to execute exit orders for open trades (positions)
        """
        trades_closed = 0
        for trade in trades:
            if (
                not trade.has_open_orders
                and not trade.has_open_sl_orders
                and trade.fee_open_currency is not None
                and not self.wallets.check_exit_amount(trade)
            ):
                logger.warning(
                    f"Not enough {trade.safe_base_currency} in wallet to exit {trade}. "
                    "Trying to recover."
                )
                if self.handle_onexchange_order(trade):
                    # Trade was deleted. Don't continue.
                    continue

            try:
                # PM unified data-failure policy for OPEN positions: when the
                # market-data stream for this pair is unhealthy and
                # data_failure_exit is enabled, force-close instead of holding
                # blind (default: hold - the policy is opt-in).
                if (
                    self._pm_ledger_enabled()
                    and self.config.get("exchange", {})
                    .get("portfolio_margin_risk", {})
                    .get("data_failure_exit", False)
                    and trade.is_open
                    and not trade.has_open_orders
                    and not trade.has_open_sl_orders
                ):
                    _snap, _reason = self._pm_ledger_snapshot(trade.pair)
                    if _reason:
                        logger.warning(
                            f"PM data-failure policy: force-exiting {trade.pair} "
                            f"(data unhealthy: {_reason})."
                        )
                        self._pm_record_signal_decision(
                            trade.pair,
                            decision="exit_data_failure",
                            reason=_reason,
                            decision_scope="exit",
                        )
                        exit_rate = self.exchange.get_rate(
                            trade.pair, side="exit", is_short=trade.is_short, refresh=True
                        )
                        if self.execute_trade_exit(
                            trade,
                            exit_rate,
                            ExitCheckTuple(
                                exit_type=ExitType.EXIT_SIGNAL, exit_reason="data_failure"
                            ),
                            exit_tag="data_failure",
                        ):
                            trades_closed += 1
                            Trade.commit()
                            continue
                try:
                    if self.strategy.order_types.get(
                        "stoploss_on_exchange"
                    ) and self.handle_stoploss_on_exchange(trade):
                        trades_closed += 1
                        Trade.commit()
                        continue

                except InvalidOrderException as exception:
                    logger.warning(
                        f"Unable to handle stoploss on exchange for {trade.pair}: {exception}"
                    )
                # Check if we can exit our current position for this trade
                if trade.has_open_position and trade.is_open and self.handle_trade(trade):
                    trades_closed += 1

            except DependencyException as exception:
                logger.warning(f"Unable to exit trade {trade.pair}: {exception}")

        # Updating wallets if any trade occurred
        if trades_closed:
            self.wallets.update()

        return trades_closed

    def handle_trade(self, trade: Trade) -> bool:
        """
        Exits the current pair if the threshold is reached and updates the trade record.
        :return: True if trade has been sold/exited_short, False otherwise
        """
        if not trade.is_open:
            raise DependencyException(f"Attempt to handle closed trade: {trade}")

        logger.debug("Handling %s ...", trade)

        (enter, exit_) = (False, False)
        exit_tag = None
        exit_signal_type = "exit_short" if trade.is_short else "exit_long"

        if self.config.get("use_exit_signal", True) or self.config.get(
            "ignore_roi_if_entry_signal", False
        ):
            analyzed_df, _ = self.dataprovider.get_analyzed_dataframe(
                trade.pair, self.strategy.timeframe
            )

            (enter, exit_, exit_tag) = self.strategy.get_exit_signal(
                trade.pair, self.strategy.timeframe, analyzed_df, is_short=trade.is_short
            )

        logger.debug("checking exit")
        exit_rate = self.exchange.get_rate(
            trade.pair, side="exit", is_short=trade.is_short, refresh=True
        )
        if self._check_and_execute_exit(trade, exit_rate, enter, exit_, exit_tag):
            return True

        logger.debug(f"Found no {exit_signal_type} signal for %s.", trade)
        return False

    def _check_and_execute_exit(
        self, trade: Trade, exit_rate: float, enter: bool, exit_: bool, exit_tag: str | None
    ) -> bool:
        """
        Check and execute trade exit
        """
        exits: list[ExitCheckTuple] = self.strategy.should_exit(
            trade,
            exit_rate,
            datetime.now(UTC),
            enter=enter,
            exit_=exit_,
            force_stoploss=0,
        )
        for should_exit in exits:
            if should_exit.exit_flag:
                exit_tag1 = exit_tag if should_exit.exit_type == ExitType.EXIT_SIGNAL else None
                if trade.has_open_orders:
                    if prev_eval := self._exit_reason_cache.get(
                        f"{trade.pair}_{trade.id}_{exit_tag1 or should_exit.exit_reason}", None
                    ):
                        logger.debug(
                            f"Exit reason already seen this candle, first seen at {prev_eval}"
                        )
                        continue

                logger.info(
                    f"Exit for {trade.pair} detected. Reason: {should_exit.exit_type}"
                    f"{f' Tag: {exit_tag1}' if exit_tag1 is not None else ''}"
                )
                exited = self.execute_trade_exit(trade, exit_rate, should_exit, exit_tag=exit_tag1)
                if exited:
                    return True
        return False

    def create_stoploss_order(self, trade: Trade, stop_price: float) -> bool:
        """
        Abstracts creating stoploss orders from the logic.
        Handles errors and updates the trade database object.
        Force-sells the pair (using EmergencySell reason) in case of Problems creating the order.
        :return: True if the order succeeded, and False in case of problems.
        """
        try:
            stoploss_order = self.exchange.create_stoploss(
                pair=trade.pair,
                amount=trade.amount,
                stop_price=stop_price,
                order_types=self.strategy.order_types,
                side=trade.exit_side,
                leverage=trade.leverage,
            )

            order_obj = Order.parse_from_ccxt_object(
                stoploss_order, trade.pair, "stoploss", trade.amount, stop_price
            )
            trade.orders.append(order_obj)
            # PM order-consistency: LINK the stoploss intent in the current
            # (uncommitted) transaction - the loop's Trade.commit() persists it
            # atomically with the local stoploss order row.
            self._pm_link_intent_for_order(stoploss_order, trade, str(order_obj.order_id))
            return True
        except InsufficientFundsError as e:
            logger.warning(f"Unable to place stoploss order {e}.")
            # Try to figure out what went wrong
            self.handle_insufficient_funds(trade)

        except InvalidOrderException as e:
            logger.error(f"Unable to place a stoploss order on exchange. {e}")
            logger.warning("Exiting the trade forcefully")
            self.emergency_exit(trade, stop_price)

        except ExchangeError:
            logger.exception("Unable to place a stoploss order on exchange.")
        return False

    def handle_stoploss_on_exchange(self, trade: Trade) -> bool:
        """
        Check if trade is fulfilled in which case the stoploss
        on exchange should be added immediately if stoploss on exchange
        is enabled.
        # TODO: liquidation price always on exchange, even without stoploss_on_exchange
        # Therefore fetching account liquidations for open pairs may make sense.
        """

        logger.debug("Handling stoploss on exchange %s ...", trade)

        stoploss_orders = []
        for slo in trade.open_sl_orders:
            stoploss_order = None
            try:
                # First we check if there is already a stoploss on exchange
                stoploss_order = (
                    self.exchange.fetch_stoploss_order(slo.order_id, trade.pair)
                    if slo.order_id
                    else None
                )
            except InvalidOrderException as exception:
                logger.warning("Unable to fetch stoploss order: %s", exception)

            if stoploss_order:
                stoploss_orders.append(stoploss_order)
                self.update_trade_state(trade, slo.order_id, stoploss_order, stoploss_order=True)

            # We check if stoploss order is fulfilled
            if stoploss_order and stoploss_order["status"] in ("closed", "triggered"):
                trade.exit_reason = ExitType.STOPLOSS_ON_EXCHANGE.value
                self._notify_exit(trade, "stoploss", True)
                self.handle_protections(trade.pair, trade.trade_direction)
                return True

        if (
            not trade.has_open_position
            or not trade.is_open
            or (trade.has_open_orders and self.exchange.get_option("stoploss_blocks_assets", True))
        ):
            # The trade can be closed already (sell-order fill confirmation came in this iteration)
            return False

        # If enter order is fulfilled but there is no stoploss, we add a stoploss on exchange
        if len(stoploss_orders) == 0:
            stop_price = trade.stoploss_or_liquidation

            if self.create_stoploss_order(trade=trade, stop_price=stop_price):
                # The above will return False if the placement failed and the trade was force-sold.
                # in which case the trade will be closed - which we must check below.
                return False

        self.manage_trade_stoploss_orders(trade, stoploss_orders)

        return False

    def manage_trade_stoploss_orders(self, trade: Trade, stoploss_orders: list[CcxtOrder]):
        """
        Perform required actions according to existing stoploss orders of trade
        :param trade: Corresponding Trade
        :param stoploss_orders: Current on exchange stoploss orders
        :return: None
        """
        # If all stoploss ordered are canceled for some reason we add it again
        canceled_sl_orders = [
            o for o in stoploss_orders if o["status"] in ("canceled", "cancelled")
        ]
        if (
            trade.is_open
            and len(stoploss_orders) > 0
            and len(stoploss_orders) == len(canceled_sl_orders)
        ):
            if self.create_stoploss_order(trade=trade, stop_price=trade.stoploss_or_liquidation):
                return False
            else:
                logger.warning("All Stoploss orders are cancelled, but unable to recreate one.")

        active_sl_orders = [o for o in stoploss_orders if o not in canceled_sl_orders]
        if len(active_sl_orders) > 0:
            last_active_sl_order = active_sl_orders[-1]
            # Finally we check if stoploss on exchange should be moved up because of trailing.
            # Triggered Orders are now real orders - so don't replace stoploss anymore
            if (
                trade.is_open
                and last_active_sl_order.get("status_stop") != "triggered"
                and (
                    self.config.get("trailing_stop", False)
                    or self.config.get("use_custom_stoploss", False)
                )
            ):
                # if trailing stoploss is enabled we check if stoploss value has changed
                # in which case we cancel stoploss order and put another one with new
                # value immediately
                self.handle_trailing_stoploss_on_exchange(trade, last_active_sl_order)

        return

    def handle_trailing_stoploss_on_exchange(self, trade: Trade, order: CcxtOrder) -> None:
        """
        Check to see if stoploss on exchange should be updated
        in case of trailing stoploss on exchange
        :param trade: Corresponding Trade
        :param order: Current on exchange stoploss order
        :return: None
        """
        stoploss_norm = self.exchange.price_to_precision(
            trade.pair,
            trade.stoploss_or_liquidation,
            rounding_mode=ROUND_DOWN if trade.is_short else ROUND_UP,
        )

        if self.exchange.stoploss_adjust(stoploss_norm, order, side=trade.exit_side):
            # we check if the update is necessary
            update_beat = self.strategy.order_types.get("stoploss_on_exchange_interval", 60)
            upd_req = datetime.now(UTC) - timedelta(seconds=update_beat)
            if trade.stoploss_last_update_utc and upd_req >= trade.stoploss_last_update_utc:
                # cancelling the current stoploss on exchange first
                logger.info(
                    f"Cancelling current stoploss on exchange for pair {trade.pair} "
                    f"(orderid:{order['id']}) in order to add another one ..."
                )

                self.cancel_stoploss_on_exchange(trade)
                if not trade.is_open:
                    logger.warning(
                        f"Trade {trade} is closed, not creating trailing stoploss order."
                    )
                    return

                # Create new stoploss order
                if not self.create_stoploss_order(trade=trade, stop_price=stoploss_norm):
                    logger.warning(
                        f"Could not create trailing stoploss order for pair {trade.pair}."
                    )

    def manage_open_orders(self) -> None:
        """
        Management of open orders on exchange. Unfilled orders might be cancelled if timeout
        was met or replaced if there's a new candle and user has requested it.
        Timeout setting takes priority over limit order adjustment request.
        :return: None
        """
        for trade in Trade.get_open_trades():
            open_order: Order
            for open_order in trade.open_orders:
                try:
                    order = self.exchange.fetch_order(open_order.order_id, trade.pair)

                except ExchangeError:
                    logger.info(
                        "Cannot query order for %s due to %s", trade, traceback.format_exc()
                    )
                    continue

                fully_cancelled = self.update_trade_state(trade, open_order.order_id, order)
                not_closed = order["status"] == "open" or fully_cancelled

                if not_closed:
                    if fully_cancelled or (
                        open_order
                        and self.strategy.ft_check_timed_out(trade, open_order, datetime.now(UTC))
                    ):
                        self.handle_cancel_order(
                            order, open_order, trade, constants.CANCEL_REASON["TIMEOUT"]
                        )
                    else:
                        self.replace_order(order, open_order, trade)

    def handle_cancel_order(
        self, order: CcxtOrder, order_obj: Order, trade: Trade, reason: str, replacing: bool = False
    ) -> bool:
        """
        Check if current analyzed order timed out and cancel if necessary.
        :param order: Order dict grabbed with exchange.fetch_order()
        :param order_obj: Order object from the database.
        :param trade: Trade object.
        :return: True if the order was canceled, False otherwise.
        """
        if order["side"] == trade.entry_side:
            return self.handle_cancel_enter(trade, order, order_obj, reason, replacing)
        else:
            canceled = self.handle_cancel_exit(trade, order, order_obj, reason)
            if not replacing:
                canceled_count = trade.get_canceled_exit_order_count()
                max_timeouts = self.config.get("unfilledtimeout", {}).get("exit_timeout_count", 0)
                if canceled and max_timeouts > 0 and canceled_count >= max_timeouts:
                    logger.warning(
                        f"Emergency exiting trade {trade}, as the exit order "
                        f"timed out {max_timeouts} times. force selling {order['amount']}."
                    )
                    # Trade.session.refresh(order_obj)

                    self.emergency_exit(trade, order["price"], order_obj.safe_remaining)
            return canceled

    def emergency_exit(
        self, trade: Trade, price: float, sub_trade_amt: float | None = None
    ) -> None:
        try:
            self.execute_trade_exit(
                trade,
                price,
                exit_check=ExitCheckTuple(exit_type=ExitType.EMERGENCY_EXIT),
                sub_trade_amt=sub_trade_amt,
            )
        except DependencyException as exception:
            logger.warning(f"Unable to emergency exit trade {trade.pair}: {exception}")

    def replace_order_failed(self, trade: Trade, msg: str) -> None:
        """
        Order replacement fail handling.
        Deletes the trade if necessary.
        :param trade: Trade object.
        :param msg: Error message.
        """
        logger.warning(msg)
        if trade.nr_of_successful_entries == 0:
            # this is the first entry and we didn't get filled yet, delete trade
            logger.warning(f"Removing {trade} from database.")
            self._notify_enter_cancel(
                trade,
                order_type=self.strategy.order_types["entry"],
                reason=constants.CANCEL_REASON["REPLACE_FAILED"],
            )
            trade.delete()

    def replace_order(self, order: CcxtOrder, order_obj: Order | None, trade: Trade) -> None:
        """
        Check if current analyzed entry order should be replaced or simply cancelled.
        To simply cancel the existing order(no replacement) adjust_order_price() should return None
        To maintain existing order adjust_order_price() should return order_obj.price
        To replace existing order adjust_order_price() should return desired price for limit order
        :param order: Order dict grabbed with exchange.fetch_order()
        :param order_obj: Order object.
        :param trade: Trade object.
        :return: None
        """
        analyzed_df, _ = self.dataprovider.get_analyzed_dataframe(
            trade.pair, self.strategy.timeframe
        )
        latest_candle_open_date = analyzed_df.iloc[-1]["date"] if len(analyzed_df) > 0 else None
        latest_candle_close_date = timeframe_to_next_date(
            self.strategy.timeframe, latest_candle_open_date
        )
        # Check if new candle
        if order_obj and latest_candle_close_date > order_obj.order_date_utc:
            is_entry = order_obj.side == trade.entry_side
            # New candle
            proposed_rate = self.exchange.get_rate(
                trade.pair,
                side="entry" if is_entry else "exit",
                is_short=trade.is_short,
                refresh=True,
            )
            adjusted_price = strategy_safe_wrapper(
                self.strategy.adjust_order_price, default_retval=order_obj.safe_placement_price
            )(
                trade=trade,
                order=order_obj,
                pair=trade.pair,
                current_time=datetime.now(UTC),
                proposed_rate=proposed_rate,
                current_order_rate=order_obj.safe_placement_price,
                entry_tag=trade.enter_tag,
                side=trade.trade_direction,
                is_entry=is_entry,
            )

            replacing = True
            cancel_reason = constants.CANCEL_REASON["REPLACE"]
            if not adjusted_price:
                replacing = False
                cancel_reason = constants.CANCEL_REASON["USER_CANCEL"]

            if order_obj.safe_placement_price != adjusted_price:
                self.handle_replace_order(
                    order,
                    order_obj,
                    trade,
                    adjusted_price,
                    is_entry,
                    cancel_reason,
                    replacing=replacing,
                )

    def handle_replace_order(
        self,
        order: CcxtOrder | None,
        order_obj: Order,
        trade: Trade,
        new_order_price: float | None,
        is_entry: bool,
        cancel_reason: str,
        replacing: bool = False,
    ) -> None:
        """
        Cancel existing order if new price is supplied, and if the cancel is successful,
        places a new order with the remaining capital.
        """
        if not order:
            order = self.exchange.fetch_order(order_obj.order_id, trade.pair)
        res = self.handle_cancel_order(order, order_obj, trade, cancel_reason, replacing=replacing)
        if not res:
            self.replace_order_failed(
                trade, f"Could not fully cancel order for {trade}, therefore not replacing."
            )
            return
        if new_order_price:
            # place new order only if new price is supplied
            try:
                if is_entry:
                    succeeded = self.execute_entry(
                        pair=trade.pair,
                        stake_amount=(
                            order_obj.safe_remaining * order_obj.safe_price / trade.leverage
                        ),
                        price=new_order_price,
                        trade=trade,
                        is_short=trade.is_short,
                        mode="replace",
                    )
                else:
                    succeeded = self.execute_trade_exit(
                        trade,
                        new_order_price,
                        exit_check=ExitCheckTuple(
                            exit_type=ExitType.CUSTOM_EXIT,
                            exit_reason=order_obj.ft_order_tag or "order_replaced",
                        ),
                        ordertype="limit",
                        sub_trade_amt=order_obj.safe_remaining,
                    )
                if not succeeded:
                    self.replace_order_failed(trade, f"Could not replace order for {trade}.")
            except DependencyException as exception:
                logger.warning(f"Unable to replace order for {trade.pair}: {exception}")
                self.replace_order_failed(trade, f"Could not replace order for {trade}.")

    def cancel_open_orders_of_trade(
        self, trade: Trade, sides: list[str], reason: str, replacing: bool = False
    ) -> None:
        """
        Cancel trade orders of specified sides that are currently open
        :param trade: Trade object of the trade we're analyzing
        :param reason: The reason for that cancellation
        :param sides: The sides where cancellation should take place
        :return: None
        """

        for open_order in trade.open_orders:
            try:
                order = self.exchange.fetch_order(open_order.order_id, trade.pair)
            except ExchangeError:
                logger.info("Can't query order for %s due to %s", trade, traceback.format_exc())
                continue

            if order["side"] in sides:
                if order["side"] == trade.entry_side:
                    self.handle_cancel_enter(trade, order, open_order, reason, replacing)

                elif order["side"] == trade.exit_side:
                    self.handle_cancel_exit(trade, order, open_order, reason)

    def cancel_all_open_orders(self) -> None:
        """
        Cancel all orders that are currently open
        :return: None
        """

        for trade in Trade.get_open_trades():
            self.cancel_open_orders_of_trade(
                trade, [trade.entry_side, trade.exit_side], constants.CANCEL_REASON["ALL_CANCELLED"]
            )

        Trade.commit()

    def handle_similar_open_order(
        self, trade: Trade, price: float, amount: float, side: str
    ) -> bool:
        """
        Keep existing open order if same amount and side otherwise cancel
        :param trade: Trade object of the trade we're analyzing
        :param price: Limit price of the potential new order
        :param amount: Quantity of assets of the potential new order
        :param side: Side of the potential new order
        :return: True if an existing similar order was found
        """
        if trade.has_open_orders:
            oo = trade.select_order(side, True)
            if oo is not None:
                if price == oo.price and side == oo.side and amount == oo.amount:
                    logger.info(
                        f"A similar open order was found for {trade.pair}. "
                        f"Keeping existing {trade.exit_side} order. {price=},  {amount=}"
                    )
                    return True
            # cancel open orders of this trade if order is different
            self.cancel_open_orders_of_trade(
                trade,
                [trade.entry_side, trade.exit_side],
                constants.CANCEL_REASON["REPLACE"],
                True,
            )
            Trade.commit()
            return False

        return False

    def handle_cancel_enter(
        self,
        trade: Trade,
        order: CcxtOrder,
        order_obj: Order,
        reason: str,
        replacing: bool | None = False,
    ) -> bool:
        """
        entry cancel - cancel order
        :param order_obj: Order object from the database.
        :param replacing: Replacing order - prevent trade deletion.
        :return: True if trade was fully cancelled
        """
        was_trade_fully_canceled = False
        order_id = order_obj.order_id
        side = trade.entry_side.capitalize()

        if order["status"] not in constants.NON_OPEN_EXCHANGE_STATES:
            filled_val: float = order.get("filled", 0.0) or 0.0
            filled_stake = filled_val * trade.open_rate
            minstake = self.exchange.get_min_pair_stake_amount(
                trade.pair, trade.open_rate, self.strategy.stoploss
            )

            if filled_val > 0 and minstake and filled_stake < minstake:
                logger.warning(
                    f"Order {order_id} for {trade.pair} not cancelled, "
                    f"as the filled amount of {filled_val} would result in an unexitable trade."
                )
                return False
            corder = self.exchange.cancel_order_with_result(order_id, trade.pair, trade.amount)
            order_obj.ft_cancel_reason = reason
            # if replacing, retry fetching the order 3 times if the status is not what we need
            if replacing:
                retry_count = 0
                while (
                    corder.get("status") not in constants.NON_OPEN_EXCHANGE_STATES
                    and retry_count < 3
                ):
                    sleep(0.5)
                    corder = self.exchange.fetch_order(order_id, trade.pair)
                    retry_count += 1

            # Avoid race condition where the order could not be cancelled coz its already filled.
            # Simply bailing here is the only safe way - as this order will then be
            # handled in the next iteration.
            if corder.get("status") not in constants.NON_OPEN_EXCHANGE_STATES:
                logger.warning(f"Order {order_id} for {trade.pair} not cancelled.")
                return False
        else:
            # Order was cancelled already, so we can reuse the existing dict
            corder = order
            if order_obj.ft_cancel_reason is None:
                order_obj.ft_cancel_reason = constants.CANCEL_REASON["CANCELLED_ON_EXCHANGE"]

        logger.info(f"{side} order {order_obj.ft_cancel_reason} for {trade}.")

        # Using filled to determine the filled amount
        filled_amount = safe_value_fallback2(corder, order, "filled", "filled")
        if isclose(filled_amount, 0.0, abs_tol=constants.MATH_CLOSE_PREC):
            was_trade_fully_canceled = True
            # if trade is not partially completed and it's the only order, just delete the trade
            open_order_count = len(
                [order for order in trade.orders if order.ft_is_open and order.order_id != order_id]
            )
            if open_order_count < 1 and trade.nr_of_successful_entries == 0 and not replacing:
                logger.info(f"{side} order fully cancelled. Removing {trade} from database.")
                trade.delete()
                order_obj.ft_cancel_reason += f", {constants.CANCEL_REASON['FULLY_CANCELLED']}"
            else:
                self.update_trade_state(trade, order_id, corder)
                logger.info(f"{side} Order timeout for {trade}.")
        else:
            # update_trade_state (and subsequently recalc_trade_from_orders) will handle updates
            # to the trade object
            self.update_trade_state(trade, order_id, corder)

            logger.info(
                f"Partial {trade.entry_side} order timeout for {trade}. Filled: {filled_amount}, "
                f"total: {order_obj.ft_amount}"
            )
            order_obj.ft_cancel_reason += f", {constants.CANCEL_REASON['PARTIALLY_FILLED']}"

        self.wallets.update()
        self._notify_enter_cancel(
            trade, order_type=self.strategy.order_types["entry"], reason=order_obj.ft_cancel_reason
        )
        return was_trade_fully_canceled

    def handle_cancel_exit(
        self, trade: Trade, order: CcxtOrder, order_obj: Order, reason: str
    ) -> bool:
        """
        exit order cancel - cancel order and update trade
        :return: True if exit order was cancelled, false otherwise
        """
        order_id = order_obj.order_id
        cancelled = False
        # Cancelled orders may have the status of 'canceled' or 'closed'
        if order["status"] not in constants.NON_OPEN_EXCHANGE_STATES:
            filled_amt: float = order.get("filled", 0.0) or 0.0
            # Filled val is in quote currency (after leverage)
            filled_rem_stake = trade.stake_amount - (filled_amt * trade.open_rate / trade.leverage)
            minstake = self.exchange.get_min_pair_stake_amount(
                trade.pair, trade.open_rate, self.strategy.stoploss
            )
            # Double-check remaining amount
            if filled_amt > 0:
                reason = constants.CANCEL_REASON["PARTIALLY_FILLED"]
                if minstake and filled_rem_stake < minstake:
                    logger.warning(
                        f"Order {order_id} for {trade.pair} not cancelled, as "
                        f"the filled amount of {filled_amt} would result in an unexitable trade."
                    )
                    reason = constants.CANCEL_REASON["PARTIALLY_FILLED_KEEP_OPEN"]

                    self._notify_exit_cancel(
                        trade,
                        order_type=self.strategy.order_types["exit"],
                        reason=reason,
                        order_id=order["id"],
                        sub_trade=trade.amount != order["amount"],
                    )
                    return False
            order_obj.ft_cancel_reason = reason
            try:
                order = self.exchange.cancel_order_with_result(
                    order["id"], trade.pair, trade.amount
                )
            except InvalidOrderException:
                logger.exception(f"Could not cancel {trade.exit_side} order {order_id}")
                return False

            # Set exit_reason for fill message
            exit_reason_prev = trade.exit_reason
            trade.exit_reason = trade.exit_reason + f", {reason}" if trade.exit_reason else reason
            # Order might be filled above in odd timing issues.
            if order.get("status") in ("canceled", "cancelled"):
                trade.exit_reason = None
            else:
                trade.exit_reason = exit_reason_prev
            cancelled = True
        else:
            if order_obj.ft_cancel_reason is None:
                order_obj.ft_cancel_reason = constants.CANCEL_REASON["CANCELLED_ON_EXCHANGE"]
            trade.exit_reason = None

        self.update_trade_state(trade, order["id"], order)

        logger.info(
            f"{trade.exit_side.capitalize()} order {order_obj.ft_cancel_reason} for {trade}."
        )
        trade.close_rate = None
        trade.close_rate_requested = None

        self._notify_exit_cancel(
            trade,
            order_type=self.strategy.order_types["exit"],
            reason=order_obj.ft_cancel_reason,
            order_id=order["id"],
            sub_trade=trade.amount != order["amount"],
        )
        return cancelled

    def _safe_exit_amount(self, trade: Trade, pair: str, amount: float) -> float:
        """
        Get exitable amount.
        Should be trade.amount - but will fall back to the available amount if necessary.
        This should cover cases where get_real_amount() was not able to update the amount
        for whatever reason.
        :param trade: Trade we're working with
        :param pair: Pair we're trying to exit
        :param amount: amount we expect to be available
        :return: amount to exit
        :raise: DependencyException: if available balance is not within 2% of the available amount.
        """
        # Update wallets to ensure amounts tied up in a stoploss is now free!
        self.wallets.update()
        if self.trading_mode == TradingMode.FUTURES:
            # A safe exit amount isn't needed for futures, you can just exit/close the position
            return amount

        trade_base_currency = self.exchange.get_pair_base_currency(pair)
        # Free + Used - open orders will eventually still be canceled.
        wallet_amount = self.wallets.get_free(trade_base_currency) + self.wallets.get_used(
            trade_base_currency
        )

        logger.debug(f"{pair} - Wallet: {wallet_amount} - Trade-amount: {amount}")
        if wallet_amount >= amount:
            return amount
        elif wallet_amount > amount * 0.98:
            logger.info(f"{pair} - Falling back to wallet-amount {wallet_amount} -> {amount}.")
            trade.amount = wallet_amount
            return wallet_amount
        else:
            raise DependencyException(
                f"Not enough amount to exit trade. Trade-amount: {amount}, Wallet: {wallet_amount}"
            )

    def execute_trade_exit(
        self,
        trade: Trade,
        limit: float,
        exit_check: ExitCheckTuple,
        *,
        exit_tag: str | None = None,
        ordertype: str | None = None,
        sub_trade_amt: float | None = None,
        skip_custom_exit_price: bool = False,
    ) -> bool:
        """
        Executes a trade exit for the given trade and limit
        :param trade: Trade instance
        :param limit: limit rate for the exit order
        :param exit_check: CheckTuple with signal and reason
        :return: True if it succeeds False
        """
        trade.set_funding_fees(
            self.exchange.get_funding_fees(
                pair=trade.pair,
                amount=trade.amount,
                is_short=trade.is_short,
                open_date=trade.date_last_filled_utc,
            )
        )

        exit_type = "exit"
        exit_reason = exit_tag or exit_check.exit_reason
        if exit_check.exit_type in (
            ExitType.STOP_LOSS,
            ExitType.TRAILING_STOP_LOSS,
            ExitType.LIQUIDATION,
        ):
            exit_type = "stoploss"

        order_type = (
            (ordertype or self.strategy.order_types[exit_type])
            if exit_check.exit_type != ExitType.EMERGENCY_EXIT
            else self.strategy.order_types.get("emergency_exit", "market")
        )

        # set custom_exit_price if available
        proposed_limit_rate = limit
        custom_exit_price = limit

        current_profit = trade.calc_profit_ratio(limit)
        if order_type == "limit" and not skip_custom_exit_price:
            custom_exit_price = strategy_safe_wrapper(
                self.strategy.custom_exit_price, default_retval=proposed_limit_rate
            )(
                pair=trade.pair,
                trade=trade,
                current_time=datetime.now(UTC),
                proposed_rate=proposed_limit_rate,
                current_profit=current_profit,
                exit_tag=exit_reason,
            )

        limit = self.get_valid_price(custom_exit_price, proposed_limit_rate)

        # First cancelling stoploss on exchange ...
        trade = self.cancel_stoploss_on_exchange(trade, allow_nonblocking=True)

        amount = self._safe_exit_amount(trade, trade.pair, sub_trade_amt or trade.amount)
        time_in_force = self.strategy.order_time_in_force["exit"]

        if (
            exit_check.exit_type != ExitType.LIQUIDATION
            and not sub_trade_amt
            and not strategy_safe_wrapper(self.strategy.confirm_trade_exit, default_retval=True)(
                pair=trade.pair,
                trade=trade,
                order_type=order_type,
                amount=amount,
                rate=limit,
                time_in_force=time_in_force,
                exit_reason=exit_reason,
                sell_reason=exit_reason,  # sellreason -> compatibility
                current_time=datetime.now(UTC),
            )
        ):
            logger.info(f"User denied exit for {trade.pair}.")
            return False

        if trade.has_open_orders:
            if self.handle_similar_open_order(trade, limit, amount, trade.exit_side):
                return False

        try:
            # Execute exit and update trade record
            order = self.exchange.create_order(
                pair=trade.pair,
                ordertype=order_type,
                side=trade.exit_side,
                amount=amount,
                rate=limit,
                leverage=trade.leverage,
                reduceOnly=self.trading_mode == TradingMode.FUTURES,
                time_in_force=time_in_force,
                initial_order=False,
            )
        except InsufficientFundsError as e:
            logger.warning(f"Unable to place order {e}.")
            # Try to figure out what went wrong
            self.handle_insufficient_funds(trade)
            return False

        self._exit_reason_cache[f"{trade.pair}_{trade.id}_{exit_reason}"] = dt_now()
        order_obj = Order.parse_from_ccxt_object(order, trade.pair, trade.exit_side, amount, limit)
        order_obj.ft_order_tag = exit_reason
        trade.orders.append(order_obj)

        trade.exit_order_status = ""
        trade.close_rate_requested = limit
        trade.exit_reason = exit_reason

        self._notify_exit(trade, order_type, sub_trade=bool(sub_trade_amt), order=order_obj)
        # In case of market exit orders the order can be closed immediately
        if order.get("status", "unknown") in ("closed", "expired"):
            self.update_trade_state(trade, order_obj.order_id, order)
        # PM order-consistency: LINK the exit intent in the same transaction as
        # the local Order commit (see execute_entry).
        self._pm_link_intent_for_order(order, trade, str(order_obj.order_id))
        Trade.commit()

        # PM signal decision ledger: record the exit decision for the current
        # closed candle under a SEPARATE scope, so it never overwrites the
        # entry audit row for the same candle.
        self._pm_record_signal_decision(
            trade.pair,
            decision="exit",
            reason=f"{exit_type}:{exit_reason}",
            decision_scope="exit",
        )

        return True

    def _notify_exit(
        self,
        trade: Trade,
        order_type: str | None,
        fill: bool = False,
        sub_trade: bool = False,
        order: Order | None = None,
    ) -> None:
        """
        Sends rpc notification when a sell occurred.
        """
        # Use cached rates here - it was updated seconds ago.
        current_rate = (
            self.exchange.get_rate(trade.pair, side="exit", is_short=trade.is_short, refresh=False)
            if not fill
            else None
        )

        # second condition is for mypy only; order will always be passed during sub trade
        if sub_trade and order is not None:
            amount = order.safe_filled if fill else order.safe_amount
            order_rate: float = order.safe_price

            profit = trade.calculate_profit(order_rate, amount, trade.open_rate)
        else:
            order_rate = trade.safe_close_rate
            profit = trade.calculate_profit(rate=order_rate)
            amount = trade.amount
        gain: ProfitLossStr = "profit" if profit.profit_ratio > 0 else "loss"

        msg: RPCExitMsg = {
            "type": (RPCMessageType.EXIT_FILL if fill else RPCMessageType.EXIT),
            "trade_id": trade.id,
            "exchange": trade.exchange.capitalize(),
            "pair": trade.pair,
            "leverage": trade.leverage,
            "direction": "Short" if trade.is_short else "Long",
            "gain": gain,
            "limit": order_rate,  # Deprecated
            "order_rate": order_rate,
            "order_type": order_type or "unknown",
            "amount": amount,
            "open_rate": trade.open_rate,
            "close_rate": order_rate,
            "current_rate": current_rate,
            "profit_amount": profit.profit_abs,
            "profit_ratio": profit.profit_ratio,
            "buy_tag": trade.enter_tag,
            "enter_tag": trade.enter_tag,
            "exit_reason": trade.exit_reason,
            "open_date": trade.open_date_utc,
            "close_date": trade.close_date_utc or datetime.now(UTC),
            "stake_amount": trade.stake_amount,
            "stake_currency": self.config["stake_currency"],
            "base_currency": self.exchange.get_pair_base_currency(trade.pair),
            "quote_currency": self.exchange.get_pair_quote_currency(trade.pair),
            "fiat_currency": self.config.get("fiat_display_currency"),
            "sub_trade": sub_trade,
            "cumulative_profit": trade.realized_profit,
            "final_profit_ratio": trade.close_profit if not trade.is_open else None,
            "is_final_exit": trade.is_open is False,
        }

        # Send the message
        self.rpc.send_msg(msg)

    def _notify_exit_cancel(
        self, trade: Trade, order_type: str, reason: str, order_id: str, sub_trade: bool = False
    ) -> None:
        """
        Sends rpc notification when a sell cancel occurred.
        """
        if trade.exit_order_status == reason:
            return
        else:
            trade.exit_order_status = reason

        order_or_none = trade.select_order_by_order_id(order_id)
        order = self.order_obj_or_raise(order_id, order_or_none)

        profit_rate: float = trade.safe_close_rate
        profit = trade.calculate_profit(rate=profit_rate)
        current_rate = self.exchange.get_rate(
            trade.pair, side="exit", is_short=trade.is_short, refresh=False
        )
        gain: ProfitLossStr = "profit" if profit.profit_ratio > 0 else "loss"

        msg: RPCExitCancelMsg = {
            "type": RPCMessageType.EXIT_CANCEL,
            "trade_id": trade.id,
            "exchange": trade.exchange.capitalize(),
            "pair": trade.pair,
            "leverage": trade.leverage,
            "direction": "Short" if trade.is_short else "Long",
            "gain": gain,
            "limit": profit_rate or 0,
            "order_rate": profit_rate or 0,
            "order_type": order_type,
            "amount": order.safe_amount_after_fee,
            "open_rate": trade.open_rate,
            "current_rate": current_rate,
            "profit_amount": profit.profit_abs,
            "profit_ratio": profit.profit_ratio,
            "buy_tag": trade.enter_tag,
            "enter_tag": trade.enter_tag,
            "exit_reason": trade.exit_reason,
            "open_date": trade.open_date,
            "close_date": trade.close_date or datetime.now(UTC),
            "stake_currency": self.config["stake_currency"],
            "base_currency": self.exchange.get_pair_base_currency(trade.pair),
            "quote_currency": self.exchange.get_pair_quote_currency(trade.pair),
            "fiat_currency": self.config.get("fiat_display_currency", None),
            "reason": reason,
            "sub_trade": sub_trade,
            "stake_amount": trade.stake_amount,
        }

        # Send the message
        self.rpc.send_msg(msg)

    def order_obj_or_raise(self, order_id: str, order_obj: Order | None) -> Order:
        if not order_obj:
            raise DependencyException(
                f"Order_obj not found for {order_id}. This should not have happened."
            )
        return order_obj

    #
    # Common update trade state methods
    #

    def update_trade_state(
        self,
        trade: Trade,
        order_id: str | None,
        action_order: CcxtOrder | None = None,
        *,
        stoploss_order: bool = False,
        send_msg: bool = True,
    ) -> bool:
        """
        Checks trades with open orders and updates the amount if necessary
        Handles closing both buy and sell orders.
        :param trade: Trade object of the trade we're analyzing
        :param order_id: Order-id of the order we're analyzing
        :param action_order: Already acquired order object
        :param send_msg: Send notification - should always be True except in "recovery" methods
        :return: True if order has been cancelled without being filled partially, False otherwise
        """
        if not order_id:
            logger.warning(f"Orderid for trade {trade} is empty.")
            return False

        # Update trade with order values
        if not stoploss_order:
            logger.info(f"Found open order for {trade}")
        try:
            order = action_order or self.exchange.fetch_order_or_stoploss_order(
                order_id, trade.pair, stoploss_order
            )
        except InvalidOrderException as exception:
            logger.warning("Unable to fetch order %s: %s", order_id, exception)
            return False

        trade.update_order(order)

        if self.exchange.check_order_canceled_empty(order):
            # Trade has been cancelled on exchange
            # Handling of this will happen in handle_cancel_order.
            return True

        order_obj_or_none = trade.select_order_by_order_id(order_id)
        order_obj = self.order_obj_or_raise(order_id, order_obj_or_none)

        self.handle_order_fee(trade, order_obj, order)

        trade.update_trade(order_obj, not send_msg)

        trade = self._update_trade_after_fill(trade, order_obj, send_msg)
        Trade.commit()

        self.order_close_notify(trade, order_obj, stoploss_order, send_msg)

        return False

    def _update_trade_after_fill(self, trade: Trade, order: Order, send_msg: bool) -> Trade:
        if order.status in constants.NON_OPEN_EXCHANGE_STATES:
            strategy_safe_wrapper(self.strategy.order_filled, supress_error=True)(
                pair=trade.pair, trade=trade, order=order, current_time=datetime.now(UTC)
            )
            # If a entry order was closed, force update on stoploss on exchange
            if order.ft_order_side == trade.entry_side:
                if send_msg:
                    if trade.nr_of_successful_entries > 1:
                        # Reset fee_open_currency so fee checking can work
                        # Only necessary for additional entries
                        trade.fee_open_currency = None
                    # Don't cancel stoploss in recovery modes immediately
                    trade = self.cancel_stoploss_on_exchange(trade)
                trade.adjust_stop_loss(trade.open_rate, self.strategy.stoploss, initial=True)
            if (
                order.ft_order_side == trade.entry_side
                or (trade.amount > 0 and trade.is_open)
                or self.margin_mode == MarginMode.CROSS
            ):
                # Must also run for partial exits
                # TODO: Margin will need to use interest_rate as well.
                # interest_rate = self.exchange.get_interest_rate()
                update_liquidation_prices(
                    trade,
                    exchange=self.exchange,
                    wallets=self.wallets,
                    stake_currency=self.config["stake_currency"],
                    dry_run=self.config["dry_run"],
                )
            if self.strategy.use_custom_stoploss and trade.is_open:
                current_rate = self.exchange.get_rate(
                    trade.pair, side="exit", is_short=trade.is_short, refresh=True
                )
                profit = trade.calc_profit_ratio(current_rate)
                self.strategy.ft_stoploss_adjust(
                    current_rate, trade, datetime.now(UTC), profit, 0, after_fill=True
                )
            if not trade.is_open:
                self.cancel_stoploss_on_exchange(trade)
            # Updating wallets when order is closed
            self.wallets.update()
        return trade

    def order_close_notify(self, trade: Trade, order: Order, stoploss_order: bool, send_msg: bool):
        """send "fill" notifications"""

        if order.ft_order_side == trade.exit_side:
            # Exit notification
            if send_msg and not stoploss_order and order.order_id not in trade.open_orders_ids:
                self._notify_exit(
                    trade, order.order_type, fill=True, sub_trade=trade.is_open, order=order
                )
            if not trade.is_open:
                self.handle_protections(trade.pair, trade.trade_direction)
        elif send_msg and order.order_id not in trade.open_orders_ids and not stoploss_order:
            sub_trade = not isclose(
                order.safe_amount_after_fee, trade.amount, abs_tol=constants.MATH_CLOSE_PREC
            )
            # Enter fill
            self._notify_enter(trade, order, order.order_type, fill=True, sub_trade=sub_trade)

    def handle_protections(self, pair: str, side: LongShort) -> None:
        # Lock pair for one candle to prevent immediate re-entries
        self.strategy.lock_pair(pair, datetime.now(UTC), reason="Auto lock", side=side)
        prot_trig = self.protections.stop_per_pair(pair, side=side)
        if prot_trig:
            msg: RPCProtectionMsg = {
                "type": RPCMessageType.PROTECTION_TRIGGER,
                "base_currency": self.exchange.get_pair_base_currency(prot_trig.pair),
                **prot_trig.to_json(),  # type: ignore
            }
            self.rpc.send_msg(msg)

        prot_trig_glb = self.protections.global_stop(side=side)
        if prot_trig_glb:
            msg = {
                "type": RPCMessageType.PROTECTION_TRIGGER_GLOBAL,
                "base_currency": self.exchange.get_pair_base_currency(prot_trig_glb.pair),
                **prot_trig_glb.to_json(),  # type: ignore
            }
            self.rpc.send_msg(msg)

    def apply_fee_conditional(
        self,
        trade: Trade,
        trade_base_currency: str,
        amount: float,
        fee_abs: float,
        order_obj: Order,
    ) -> float | None:
        """
        Applies the fee to amount (either from Order or from Trades).
        Can eat into dust if more than the required asset is available.
        In case of trade adjustment orders, trade.amount will not have been adjusted yet.
        Can't happen in Futures mode - where Fees are always in settlement currency,
        never in base currency.
        """
        self.wallets.update()
        amount_ = trade.amount
        if order_obj.ft_order_side == trade.exit_side or order_obj.ft_order_side == "stoploss":
            # check against remaining amount!
            amount_ = trade.amount - amount

        if trade.nr_of_successful_entries >= 1 and order_obj.ft_order_side == trade.entry_side:
            # In case of re-entry's, trade.amount doesn't contain the amount of the last entry.
            amount_ = trade.amount + amount

        if fee_abs != 0 and self.wallets.get_free(trade_base_currency) >= amount_:
            # Eat into dust if we own more than base currency
            logger.info(
                f"Fee amount for {trade} was in base currency - Eating Fee {fee_abs} into dust."
            )
        elif fee_abs != 0:
            logger.info(f"Applying fee on amount for {trade}, fee={fee_abs}.")
            return fee_abs
        return None

    def handle_order_fee(self, trade: Trade, order_obj: Order, order: CcxtOrder) -> None:
        # Try update amount (binance-fix - but also applies to different exchanges)
        try:
            if (fee_abs := self.get_real_amount(trade, order, order_obj)) is not None:
                order_obj.ft_fee_base = fee_abs
        except DependencyException as exception:
            logger.warning("Could not update trade amount: %s", exception)

    def get_real_amount(self, trade: Trade, order: CcxtOrder, order_obj: Order) -> float | None:
        """
        Detect and update trade fee.
        Calls trade.update_fee() upon correct detection.
        Returns modified amount if the fee was taken from the destination currency.
        Necessary for exchanges which charge fees in base currency (e.g. binance)
        :return: Absolute fee to apply for this order or None
        """
        # Init variables
        order_amount = safe_value_fallback(order, "filled", "amount")
        # Only run for closed orders
        if (
            trade.fee_updated(order.get("side", "")) or order["status"] == "open"
            # or order_obj.ft_fee_base
        ):
            return None

        trade_base_currency = self.exchange.get_pair_base_currency(trade.pair)
        # use fee from order-dict if possible
        if self.exchange.order_has_fee(order):
            fee_cost, fee_currency, fee_rate = self.exchange.extract_cost_curr_rate(
                order["fee"], order["symbol"], order["cost"], order_obj.safe_filled
            )
            logger.info(
                f"Fee for Trade {trade} [{order_obj.ft_order_side}]: "
                f"{fee_cost:.8g} {fee_currency} - rate: {fee_rate}"
            )
            if fee_rate is None or fee_rate < 0.02:
                # Reject all fees that report as > 2%.
                # These are most likely caused by a parsing bug in ccxt
                # due to multiple trades (https://github.com/ccxt/ccxt/issues/8025)
                trade.update_fee(fee_cost, fee_currency, fee_rate, order.get("side", ""))
                if trade_base_currency == fee_currency:
                    # Apply fee to amount
                    return self.apply_fee_conditional(
                        trade,
                        trade_base_currency,
                        amount=order_amount,
                        fee_abs=fee_cost,
                        order_obj=order_obj,
                    )
                return None
        return self.fee_detection_from_trades(
            trade, order, order_obj, order_amount, order.get("trades", [])
        )

    def _trades_valid_for_fee(self, trades: list[dict[str, Any]]) -> bool:
        """
        Check if trades are valid for fee detection.
        :return: True if trades are valid for fee detection, False otherwise
        """
        if not trades:
            return False
        # We expect amount and cost to be present in all trade objects.
        if any(trade.get("amount") is None or trade.get("cost") is None for trade in trades):
            return False
        return True

    def fee_detection_from_trades(
        self, trade: Trade, order: CcxtOrder, order_obj: Order, order_amount: float, trades: list
    ) -> float | None:
        """
        fee-detection fallback to Trades.
        Either uses provided trades list or the result of fetch_my_trades to get correct fee.
        """
        if not self._trades_valid_for_fee(trades):
            trades = self.exchange.get_trades_for_order(
                self.exchange.get_order_id_conditional(order), trade.pair, order_obj.order_date
            )

        if len(trades) == 0:
            logger.info("Applying fee on amount for %s failed: myTrade-dict empty found", trade)
            return None
        fee_currency = None
        amount = 0
        fee_abs = 0.0
        fee_cost = 0.0
        trade_base_currency = self.exchange.get_pair_base_currency(trade.pair)
        fee_rate_array: list[float] = []
        for exectrade in trades:
            amount += exectrade["amount"]
            if self.exchange.order_has_fee(exectrade):
                # Prefer singular fee
                fees = [exectrade["fee"]]
            else:
                fees = exectrade.get("fees", [])
            for fee in fees:
                fee_cost_, fee_currency, fee_rate_ = self.exchange.extract_cost_curr_rate(
                    fee, exectrade["symbol"], exectrade["cost"], exectrade["amount"]
                )
                fee_cost += fee_cost_
                if fee_rate_ is not None:
                    fee_rate_array.append(fee_rate_)
                # only applies if fee is in quote currency!
                if trade_base_currency == fee_currency:
                    fee_abs += fee_cost_
        # Ensure at least one trade was found:
        if fee_currency:
            # fee_rate should use mean
            fee_rate = sum(fee_rate_array) / float(len(fee_rate_array)) if fee_rate_array else None
            if fee_rate is not None and fee_rate < 0.02:
                # Only update if fee-rate is < 2%
                trade.update_fee(fee_cost, fee_currency, fee_rate, order.get("side", ""))
            else:
                logger.warning(
                    f"Not updating {order.get('side', '')}-fee - rate: {fee_rate}, {fee_currency}."
                )

        if not isclose(amount, order_amount, abs_tol=constants.MATH_CLOSE_PREC):
            # * Leverage could be a cause for this warning
            logger.warning(f"Amount {amount} does not match amount {trade.amount}")
            raise DependencyException("Half bought? Amounts don't match")

        if fee_abs != 0:
            return self.apply_fee_conditional(
                trade, trade_base_currency, amount=amount, fee_abs=fee_abs, order_obj=order_obj
            )
        return None

    def get_valid_price(self, custom_price: float, proposed_price: float) -> float:
        """
        Return the valid price.
        Check if the custom price is of the good type if not return proposed_price
        :return: valid price for the order
        """
        if custom_price:
            try:
                valid_custom_price = float(custom_price)
            except ValueError:
                valid_custom_price = proposed_price
        else:
            valid_custom_price = proposed_price

        cust_p_max_dist_r = self.config.get("custom_price_max_distance_ratio", 0.02)
        min_custom_price_allowed = proposed_price - (proposed_price * cust_p_max_dist_r)
        max_custom_price_allowed = proposed_price + (proposed_price * cust_p_max_dist_r)

        # Bracket between min_custom_price_allowed and max_custom_price_allowed
        final_price = max(
            min(valid_custom_price, max_custom_price_allowed), min_custom_price_allowed
        )

        # Log a warning if the custom price was adjusted by clamping.
        if final_price != valid_custom_price:
            logger.info(
                f"Custom price adjusted from {valid_custom_price} to {final_price} based on "
                "custom_price_max_distance_ratio of {cust_p_max_dist_r}."
            )

        return final_price
