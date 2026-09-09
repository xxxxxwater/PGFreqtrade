"""
Freqtrade is the main module of this bot. It contains the FreqtradeBot class.
"""

import json
import logging
import time as _time
import traceback
from hashlib import sha256
from copy import deepcopy
from datetime import UTC, datetime, time, timedelta
from math import isclose, isfinite
from threading import RLock, local
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
    StopWouldImmediatelyTrigger,
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
from freqtrade.persistence.pm_outbox import PMOutbox
from freqtrade.pm_order_ownership import PMOrderOwnershipMixin, PMOwnershipClass, pm_order_locked
from freqtrade.persistence.pm_stream_journal import PMStreamJournal
from freqtrade.plugins.pairlistmanager import PairListManager
from freqtrade.plugins.protectionmanager import ProtectionManager
from freqtrade.resolvers import ExchangeResolver, StrategyResolver
from freqtrade.state_persistence import invalidate_state_file, persist_state, read_persisted_state
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


class FreqtradeBot(PMOrderOwnershipMixin, LoggingMixin):
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
        # True when the last set_state() could not be persisted; the worker
        # retries persisting the current state until it succeeds.
        self._state_persist_failed = False

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
        if initial_state:
            self.state = State[initial_state.upper()]
        else:
            self.set_state(State.STOPPED)

        # Production fail-safe: restore the persisted state.  A valid record
        # of PAUSED/STOPPED survives restarts and reboots; a CORRUPT record
        # blocks auto-trading by starting PAUSED instead of silently
        # re-running; a persisted RUNNING never overrides an explicit
        # configuration.  The resolved state is persisted immediately, so a
        # crash before the first worker iteration loses nothing.
        persisted = read_persisted_state(self.config)
        if persisted.corrupt:
            self.set_state(State.PAUSED)
            logger.warning(
                "Persisted bot state is corrupt or unreadable; starting PAUSED. "
                "Entries are blocked until an operator reviews and sends /start."
            )
        elif persisted.state in (State.PAUSED, State.STOPPED):
            self.set_state(persisted.state)
            logger.warning(f"Restoring persisted bot state: {persisted.state.name}.")
        else:
            self.set_state(self.state)

        # Protect exit-logic from forcesell and vice versa
        self._exit_lock = RLock()
        timeframe_secs = timeframe_to_seconds(self.strategy.timeframe)
        self._exit_reason_cache = PeriodicCache(100, ttl=timeframe_secs)
        LoggingMixin.__init__(self, logger, timeframe_secs)

        self._schedule = Scheduler()
        # PM control-plane maintenance must continue even while the trading
        # state is STOPPED.  Keep it separate from the strategy/account-risk
        # scheduler so STOPPED can service listenKey/stream/recovery without
        # running trading or emergency account actions.
        self._pm_maintenance_schedule = Scheduler()

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
        self._pm_maintenance_schedule.every(keepalive_interval).minutes.do(
            self._pm_keepalive_listen_key
        )

        risk_interval = pm_risk_cfg.get("monitor_interval_minutes", 5)
        # Account-level risk actions (including emergency close-all) belong to
        # the active/paused trading control loop, never STOPPED maintenance.
        self._schedule.every(risk_interval).minutes.do(self._pm_risk_monitor)

        recovery_interval = pm_risk_cfg.get("order_recovery_interval_minutes", 5)
        self._pm_maintenance_schedule.every(recovery_interval).minutes.do(
            self._pm_order_recovery
        )

        health_interval = pm_risk_cfg.get("user_stream_health_interval_minutes", 1)
        self._pm_maintenance_schedule.every(health_interval).minutes.do(
            self._pm_user_stream_health_monitor
        )

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
            self._pm_unmatched_stream_order_ids: set[tuple[str, str]] = set()
        if not hasattr(self, "_pm_foreign_stream_order_events"):
            # Foreign exchange orders remain outside the strategy database.  Keep a
            # bounded id/status cache solely to avoid flooding logs when Binance
            # retransmits an external manual-order event.
            self._pm_foreign_stream_order_events: set[str] = set()
        if not hasattr(self, "_pm_actual_order_map"):
            # Real order id (after conditional trigger) -> local stoploss order id.
            # Populated whenever fetch_stoploss_order resolves an actual order.
            self._pm_actual_order_map: dict[tuple[str, str], str] = {}
        if not hasattr(self, "_pm_orders_blocked_reasons"):
            self._pm_orders_blocked_reasons: list[str] = []
        if not hasattr(self, "_pm_owned_resolve_cache"):
            # Per-batch cache of proven (pair, order_id, client_id) -> (trade, order).
            # Reset at the start of every user-stream batch; only successful
            # ownership resolutions are cached (misses keep retrying).
            self._pm_owned_resolve_cache: dict[tuple[str, str, str], tuple[Any, Any]] = {}
        if not hasattr(self, "_pm_clean_terminal_ids"):
            # Bounded set of (pair, order_id) whose ownership was verified clean,
            # so a terminal-event redelivery storm skips journal lookups.
            self._pm_clean_terminal_ids: set[tuple[str, str]] = set()
        if not hasattr(self, "_pm_last_auto_recovery_at"):
            self._pm_last_auto_recovery_at: float | None = None
        if not hasattr(self, "_pm_unresolved_instrument_ids"):
            # raw symbol -> {"namespace": ..., "event": {...}} for symbols that
            # failed canonicalization. Retried on recovery with FULL event
            # re-processing before the instrument_identity_unknown gate may
            # auto-release.
            self._pm_unresolved_instrument_ids: dict[str, dict[str, Any]] = {}
        if not hasattr(self, "_pm_position_mismatch_alerted"):
            self._pm_position_mismatch_alerted = False
        if not hasattr(self, "_pm_foreign_position_pairs"):
            self._pm_foreign_position_pairs: set[str] = set()
        if not hasattr(self, "_pm_foreign_order_pairs"):
            self._pm_foreign_order_pairs: set[str] = set()
        if not hasattr(self, "_pm_stop_protection_missing_alerted"):
            self._pm_stop_protection_missing_alerted = False
        if not hasattr(self, "_pm_last_journal_purge_at"):
            self._pm_last_journal_purge_at: float | None = None
        if not hasattr(self, "_pm_unresolved_alert_sent"):
            self._pm_unresolved_alert_sent = False
        if not hasattr(self, "_pm_last_reconciliation_warning_at"):
            self._pm_last_reconciliation_warning_at: datetime | None = None
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
                if PMStreamJournal.get_unresolved():
                    if "unmatched_stream_order" not in reasons:
                        reasons.append("unmatched_stream_order")
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

    def _pm_allow_foreign_positions(self) -> bool:
        """Whether operator-owned/manual PM exposure may coexist with BOT-owned exposure."""
        return bool(
            self.config.get("exchange", {})
            .get("portfolio_margin_risk", {})
            .get("allow_foreign_positions", False)
        )

    def _pm_close_foreign_positions_on_emergency(self) -> bool:
        """Whether account emergency-close is authorized to liquidate FOREIGN positions."""
        return bool(
            self.config.get("exchange", {})
            .get("portfolio_margin_risk", {})
            .get("emergency_close_foreign_positions", True)
        )

    def _pm_active_bot_owned_pairs(self) -> set[str]:
        """Pairs with current durable BOT ownership evidence.

        Open local Trades are authoritative. Active intent rows cover the crash
        window before/after exchange POST, including an initial entry whose local
        Trade has not committed yet. Store failure propagates and therefore fails
        closed in the caller; it must never turn uncertain BOT exposure into FOREIGN.
        """
        pairs = {
            self._pm_canonical_pair(trade.pair)
            for trade in Trade.get_open_trades()
            if trade.is_open
        }
        if self._pm_db_gate_active():
            active_states = ("PENDING", "PREPARED", "ACKED", "LINKED", "UNKNOWN")
            rows = PMOrderIntent.session.query(PMOrderIntent).filter(
                PMOrderIntent.state.in_(active_states)
            ).all()
            for row in rows:
                pairs.add(self._pm_canonical_pair(row.pair))

            # The outbox is a second durable source.  If intent state is ever
            # damaged/missing while delivery evidence remains active, ownership
            # must stay BOT-owned rather than being downgraded to FOREIGN.
            outbox_rows = PMOutbox.session.query(PMOutbox).filter(
                PMOutbox.state.in_(("PENDING", "ACKED", "LINKED"))
            ).all()
            for row in outbox_rows:
                payload = json.loads(row.payload or "{}")
                if not isinstance(payload, dict):
                    raise OperationalException("PM outbox payload is not an object")
                raw_pair = payload.get("pair") or payload.get("symbol")
                if not raw_pair:
                    raise OperationalException(
                        f"Active PM outbox {row.client_id} has no instrument identity"
                    )
                pairs.add(self._pm_canonical_pair(str(raw_pair)))
        return pairs

    def _pm_exchange_order_is_bot_owned(self, order: dict[str, Any]) -> bool:
        """Return True only when an unmatched exchange order has BOT identity evidence."""
        info = order.get("info") if isinstance(order, dict) else {}
        info = info if isinstance(info, dict) else {}
        client_id = str(
            order.get("clientOrderId")
            or order.get("clientAlgoId")
            or info.get("clientOrderId")
            or info.get("clientAlgoId")
            or info.get("clientStrategyId")
            or ""
        )
        if client_id.startswith(("ft", "st")):
            return True
        if self._pm_db_gate_active() and client_id:
            evidence = PMOutbox.get_by_client_id(client_id) or PMOrderIntent.get_by_client_id(
                client_id
            )
            return evidence is not None
        return False

    def _pm_refresh_foreign_order_pairs(self, pair: str | None = None) -> set[str]:
        """Refresh FOREIGN normal/conditional order ownership from the exchange.

        Unknown exchange orders with BOT client-id/durable evidence are NOT
        classified foreign: they are an ownership failure and must fail closed.
        Pure external/manual orders are read-only and only create a same-pair
        exposure-increase conflict when coexistence mode is enabled.
        """
        self._pm_init_user_stream_state()
        if not self._pm_allow_foreign_positions() or not self._pm_db_gate_active():
            self._pm_foreign_order_pairs = set()
            return set()
        canonical_filter = self._pm_canonical_pair(pair) if pair else None
        open_trades = Trade.get_open_trades()
        known_normal = {
            (self._pm_canonical_pair(trade.pair), str(order.order_id))
            for trade in open_trades
            for order in trade.open_orders
        }
        known_conditional = {
            (self._pm_canonical_pair(trade.pair), str(order.order_id))
            for trade in open_trades
            for order in trade.open_sl_orders
        }
        normal = self.exchange.fetch_open_orders(pair=canonical_filter)
        conditional = self.exchange.fetch_open_conditional_orders(pair=canonical_filter)
        foreign: set[str] = set()
        for order in normal:
            raw = order.get("symbol")
            canonical = self._pm_canonical_pair(raw)
            oid = str(order.get("id") or "")
            if oid and (canonical, oid) in known_normal:
                continue
            if self._pm_exchange_order_is_bot_owned(order):
                raise OperationalException(
                    f"Untracked BOT-owned normal order {oid or '<missing>'} on {canonical}"
                )
            foreign.add(canonical)
        for order in conditional:
            raw = order.get("symbol")
            canonical = self._pm_canonical_pair(raw)
            oid = str(order.get("id") or order.get("clientAlgoId") or "")
            if oid and (canonical, oid) in known_conditional:
                continue
            if self._pm_exchange_order_is_bot_owned(order):
                raise OperationalException(
                    f"Untracked BOT-owned conditional order {oid or '<missing>'} on {canonical}"
                )
            foreign.add(canonical)
        if canonical_filter is None:
            self._pm_foreign_order_pairs = foreign
        else:
            self._pm_foreign_order_pairs.discard(canonical_filter)
            if canonical_filter in foreign:
                self._pm_foreign_order_pairs.add(canonical_filter)
        return foreign

    def _pm_pair_entry_block_reasons(self, pair: str) -> list[str]:
        """Fresh pair-local FOREIGN ownership gate immediately before risk increase."""
        if not self._pm_allow_foreign_positions() or not self._pm_db_gate_active():
            return []
        self._pm_init_user_stream_state()
        canonical = self._pm_canonical_pair(pair)
        reasons: list[str] = []
        try:
            mismatches = self._pm_account_position_reconcile()
        except Exception as exc:
            self.log_once(
                f"PM pre-entry position ownership check unavailable for {canonical}: {exc}",
                logger.warning,
            )
            return ["foreign_ownership_check_unavailable"]
        if mismatches:
            # These are BOT-owned quantity mismatches only in coexistence mode.
            self._pm_block_orders("position_quantity_mismatch")
            reasons.append("position_quantity_mismatch")
        else:
            self._pm_unblock_orders("position_quantity_mismatch")
        if canonical in self._pm_foreign_position_pairs:
            reasons.append("foreign_position_conflict")
        try:
            self._pm_refresh_foreign_order_pairs(canonical)
        except Exception as exc:
            # A proven BOT-owned but untracked exchange order is a global
            # ownership incident. A generic read failure refuses THIS entry and
            # leaves scheduled recovery/risk health to decide broader state.
            if "BOT-owned" in str(exc):
                self._pm_block_orders("reconciliation_incomplete")
                reasons.append("bot_order_ownership_conflict")
            else:
                reasons.append("foreign_ownership_check_unavailable")
            self.log_once(
                f"PM pre-entry order ownership check failed for {canonical}: {exc}",
                logger.warning,
            )
        if canonical in self._pm_foreign_order_pairs:
            reasons.append("foreign_order_conflict")
        return list(dict.fromkeys(reasons))

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

    def _pm_pending_reduce_only_exit_intent(self, trade: Trade) -> PMOrderIntent | None:
        """Return a durable unresolved normal reduce-only exit for this Trade.

        A timeout-uncertain PM exit must be resolved by its original client id,
        never followed by a fresh client id every strategy loop.  Conditional
        stop protection is intentionally excluded: it may coexist with a normal
        reduce-only exit until the trade lifecycle cancels it.

        If the intent store itself is unavailable, do not turn this de-dup helper
        into a new risk-reduction blocker.  The exchange PM pipeline will still
        fail closed before submitting because it cannot durably enqueue an intent.
        """
        if not self._pm_db_gate_active():
            return None
        try:
            for intent in PMOrderIntent.get_unresolved_for_pair(trade.pair):
                if (
                    intent.kind == "order"
                    and bool(intent.reduce_only)
                    and str(intent.side or "").lower() == str(trade.exit_side).lower()
                ):
                    return intent
        except Exception as exc:
            self.log_once(
                f"PM exit de-dup could not read pending intents for {trade.pair}: {exc}",
                logger.warning,
            )
        return None

    def _pm_pending_conditional_stop_intent(
        self, trade: Trade
    ) -> tuple[str, PMOrderIntent | None, PMOutbox | None, bool]:
        """Find an unresolved conditional that may be this Trade's protection.

        Exact ``origin_trade_id`` ownership wins. Legacy rows without ownership
        are returned only as an *unowned conflict* (bool=False): they still block
        a new stop client-id and, if protection cannot be verified, eventually
        force the BOT Trade to reduce risk without pretending the row is owned.
        """
        if not self._pm_db_gate_active():
            return "none", None, None, False
        try:
            rows = [
                row
                for row in PMOrderIntent.get_unresolved_for_pair(trade.pair)
                if row.kind == "conditional"
            ]
        except Exception as exc:
            self.log_once(
                f"PM conditional-stop intent store unreadable for {trade.pair}: {exc}",
                logger.warning,
            )
            self._pm_block_orders("intent_store_unavailable")
            return "unknown", None, None, False
        exact = [row for row in rows if row.origin_trade_id == trade.id]
        candidates = exact or [row for row in rows if row.origin_trade_id is None]
        if not candidates:
            return "none", None, None, False
        row = sorted(candidates, key=lambda item: item.created_at or datetime.min)[0]
        try:
            outbox = PMOutbox.get_by_client_id(row.client_id)
        except Exception:
            outbox = None
        return "found", row, outbox, bool(exact)

    @staticmethod
    def _pm_age_seconds(value: datetime | None) -> float | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return max(0.0, (datetime.now(UTC) - value).total_seconds())

    def _pm_handle_uncertain_stop_dispatch(self, trade: Trade) -> bool:
        """Keep a BOT position from waiting forever on an UNKNOWN stop POST.

        Returns True when a pending/uncertain conditional owns the protection
        decision for this iteration, so the caller must NOT generate a new stop
        client id. A may-have-been-sent stop is always lookup-only. If it cannot
        be verified within the configured grace, the remaining BOT Trade is
        reduced via the normal idempotent emergency-exit path.
        """
        pending_state, intent, outbox, ownership_exact = (
            self._pm_pending_conditional_stop_intent(trade)
        )
        if pending_state == "none":
            return False
        if pending_state == "unknown" or intent is None:
            self._pm_protection_hold(
                trade, "conditional-stop intent store is UNKNOWN/unreadable"
            )
            return True

        pristine = bool(
            outbox is not None
            and outbox.dispatch_started_at is None
            and int(outbox.dispatch_attempts or 0) == 0
            and intent.state in {"PENDING", "PREPARED"}
        )
        if pristine:
            # This is the only safe redispatch class: the durable send boundary
            # was never crossed. Drive recovery with the SAME client id.
            try:
                self._pm_recover_pending_intents()
            except Exception as exc:
                logger.warning(
                    "PM stop recovery for Trade #%s %s failed: %s",
                    trade.id, trade.pair, exc,
                )
            return True

        evidence_time = None
        if outbox is not None:
            evidence_time = outbox.dispatch_started_at or outbox.processed_at
        evidence_time = evidence_time or intent.acked_at or intent.created_at
        age = self._pm_age_seconds(evidence_time) or 0.0
        threshold = int(
            self.config.get("exchange", {})
            .get("portfolio_margin_risk", {})
            .get("uncertain_stop_emergency_exit_seconds", 30)
        )
        identity = "owned" if ownership_exact else "legacy-unowned"
        self._pm_protection_hold(
            trade,
            f"{identity} stop intent {intent.client_id} outcome is {intent.state}; "
            f"age={age:.1f}s",
        )
        if age < threshold:
            return True

        self.log_once(
            f"PM protection escalation: Trade #{trade.id} {trade.pair} has no "
            f"verified stop and conditional intent {intent.client_id} has remained "
            f"uncertain for {age:.1f}s (limit={threshold}s). Submitting an "
            "idempotent reduce-only emergency exit; the original stop stays "
            "lookup-only and is never re-POSTed.",
            logger.critical,
        )
        self.emergency_exit(trade, trade.stoploss_or_liquidation)
        return True

    def _pm_reconciliation_incident_signature(self, result: dict[str, Any]) -> tuple:
        """Durable event-level identity for reconciliation warning de-duplication."""
        try:
            intents = tuple(
                sorted(
                    (
                        str(row.client_id),
                        str(row.state),
                        str(row.exchange_order_id or ""),
                        str(row.origin_trade_id or ""),
                        str(row.last_error or ""),
                    )
                    for row in PMOrderIntent.get_unresolved()
                )
            )
        except Exception as exc:
            intents = (("intent-store-unavailable", exc.__class__.__name__),)
        try:
            incidents = tuple(
                sorted(
                    (
                        int(row.id or 0),
                        str(row.pair),
                        str(row.exchange_order_id),
                        str(row.client_id or ""),
                        str(row.reason or ""),
                        float(row.max_cumulative_filled or 0.0),
                        bool(row.saw_filled),
                    )
                    for row in PMStreamJournal.get_unresolved()
                )
            )
        except Exception as exc:
            incidents = ((0, "journal-unavailable", exc.__class__.__name__),)
        errors = tuple(sorted(str(error) for error in result.get("errors", [])))
        return errors, intents, incidents

    def _pm_reconciliation_warning_due(self, signature: tuple) -> tuple[bool, bool, int]:
        """Return (send, is_reminder, interval_minutes) for one incident."""
        now = datetime.now(UTC)
        risk_cfg = self.config.get("exchange", {}).get("portfolio_margin_risk", {})
        reminder_minutes = max(
            1, int(risk_cfg.get("reconciliation_warning_reminder_minutes", 30))
        )
        previous = getattr(self, "_pm_last_reconciliation_warning", None)
        last_at = getattr(self, "_pm_last_reconciliation_warning_at", None)
        changed = signature != previous
        reminder_due = bool(
            not changed
            and last_at is not None
            and now - last_at >= timedelta(minutes=reminder_minutes)
        )
        if changed or reminder_due or last_at is None:
            self._pm_last_reconciliation_warning = signature
            self._pm_last_reconciliation_warning_at = now
            return True, reminder_due and not changed, reminder_minutes
        return False, False, reminder_minutes

    def _pm_auto_recovery_due(self) -> bool:
        """Rate-limit auto-triggered FULL recovery sweeps.

        A burst of distinct unknown orders would otherwise multiply into N
        full account sweeps (REST weight amplification). Targeted incident
        recovery is never throttled; only the full sweep is.
        """
        self._pm_init_user_stream_state()
        cooldown = float(
            self.config.get("exchange", {})
            .get("portfolio_margin_risk", {})
            .get("user_stream_auto_recovery_cooldown_seconds", 60)
        )
        last = getattr(self, "_pm_last_auto_recovery_at", None)
        now = _time.monotonic()
        if last is not None and now - last < cooldown:
            return False
        self._pm_last_auto_recovery_at = now
        return True

    def _pm_retry_unresolved_instruments(self) -> int:
        """Retry unresolved-instrument events with FULL evidence re-processing.

        Canonicalizing a raw symbol proves nothing about the ORDER: the stored
        original event (namespace, orderId, clientOrderId, status, fill
        evidence) is re-dispatched through the authoritative matcher. A symbol
        only counts as resolved when the re-dispatched event is owned or
        explained (no new unresolved journal incident), and the gate is only
        released when every stored symbol resolved AND the account position
        quantity invariant is clean. Returns the number of still-unresolved
        symbols.
        """
        self._pm_init_user_stream_state()
        pending = dict(getattr(self, "_pm_unresolved_instrument_ids", {}))
        for raw in sorted(pending):
            meta = pending[raw] or {}
            try:
                pair = self._pm_canonical_pair(raw, namespace=str(meta.get("namespace") or "um"))
            except OperationalException:
                continue  # still unresolvable: gate stays
            # Re-run the ORIGINAL event through the authoritative ownership /
            # fill high-water / incident path. An unexplained order creates a
            # durable journal incident (which itself blocks new exposure).
            try:
                self._pm_handle_order_trade_update(dict(meta.get("event") or {}), {})
            except Exception:
                logger.exception("PM re-dispatch of unresolved instrument event failed")
                continue
            order_data = (meta.get("event") or {}).get("o") or {}
            event_key = str(order_data.get("i") or order_data.get("c") or "")
            unresolved_incident = False
            if self._pm_db_gate_active() and event_key:
                try:
                    incident = PMStreamJournal.get(pair, event_key)
                    unresolved_incident = incident is not None and incident.unresolved
                except Exception:
                    unresolved_incident = True
            if unresolved_incident:
                # Ownership / fill evidence still unexplained: keep the identity
                # gate (the unmatched_stream_order gate holds risk increase too).
                continue
            self._pm_unresolved_instrument_ids.pop(raw, None)
        still = dict(getattr(self, "_pm_unresolved_instrument_ids", {}))
        if not still:
            # Full pipeline before release: the account-level quantity
            # invariant must also be clean.
            try:
                mismatches = self._pm_account_position_reconcile()
            except Exception:
                mismatches = ["unavailable"]
            if not mismatches:
                self._pm_unblock_orders("instrument_identity_unknown")
                if pending:
                    logger.info(
                        "PM instrument identity gate auto-released after %d event(s) "
                        "were re-dispatched and reconciled.",
                        len(pending),
                    )
        return len(still)

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
        try:
            return self._pm_canonical_pair(symbol_id)
        except OperationalException:
            return None

    @pm_order_locked
    def _pm_handle_order_trade_update(
        self, event: dict[str, Any], order_index: dict
    ) -> bool:
        order_data = event.get("o", {})
        order_id = str(order_data.get("i") or order_data.get("orderId") or "")
        client_order_id = str(order_data.get("c") or "")
        event_order_id = order_id or client_order_id
        if not event_order_id:
            return False

        self._pm_init_user_stream_state()
        try:
            pair = self._pm_canonical_pair(
                order_data.get("s"), str(event.get("fs") or "UM").lower()
            )
        except OperationalException as exc:
            self._pm_init_user_stream_state()
            raw = str(order_data.get("s") or "")
            if raw:
                # Persist the complete event evidence (namespace, orderId,
                # clientOrderId, status, fill data): releasing the gate later
                # requires re-processing THIS event, not just parsing the symbol.
                self._pm_unresolved_instrument_ids.setdefault(
                    raw,
                    {
                        "namespace": str(event.get("fs") or "UM").lower(),
                        "event": dict(event),
                    },
                )
            self._pm_block_orders("instrument_identity_unknown")
            logger.warning("PM stream instrument unresolved: %s", exc)
            return False

        # A PM conditional order may announce its generated real ``orderId`` in
        # ``i`` while keeping our ``newClientStrategyId`` in ``c``.  Stoploss
        # orders are stored locally under the strategy id, so consult both ids
        # before treating the event as an unmatched order.
        try:
            entry = self._pm_owned_order(pair, order_id, client_order_id, order_index)
        except Exception as exc:
            self._pm_note_stream_incident(pair, event_order_id, order_data, str(exc))
            return False
        if entry is None:
            if (pair, event_order_id) in self._pm_unmatched_stream_order_ids:
                # Keep cumulative evidence, but do not repeat conditional REST
                # probes/full recovery for every message in an unknown-order burst.
                self._pm_note_stream_incident(
                    pair, event_order_id, order_data, "RECOVERABLE_UNKNOWN: awaiting recovery"
                )
                return False
            return self._pm_handle_unmatched_order_trade_update(event_order_id, order_data, pair)

        trade, order = entry
        # A REST-confirmed fill remains owned even after removal from open_orders.
        # Absorb late NEW/PARTIAL/FILLED replays without another fee/notify pass.
        if self._pm_event_is_replay(order, order_data):
            key = (pair, event_order_id)
            had_incident = key in self._pm_unmatched_stream_order_ids
            clean = getattr(self, "_pm_clean_terminal_ids", None)
            if (
                not had_incident
                and self._pm_db_gate_active()
                and (clean is None or key not in clean)
            ):
                had_incident = PMStreamJournal.get(pair, event_order_id) is not None
            self._pm_finish_stream_incident(pair, event_order_id, order)
            classification = (
                PMOwnershipClass.KNOWN_LATE
                if order_data.get("X") in {"NEW", "PARTIALLY_FILLED"}
                else PMOwnershipClass.KNOWN_DUPLICATE
            )
            logger.debug("PM stream %s: %s orderId=%s", classification, pair, event_order_id)
            if had_incident:
                # Closed loop: resolving this incident re-evaluates the remaining
                # durable incidents immediately instead of waiting for the next
                # scheduled recovery (generation-safe: never a blanket clear).
                try:
                    self._pm_recover_stream_incidents()
                except Exception:
                    logger.exception("PM post-replay incident re-evaluation failed")
            return False
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
                self._pm_validate_rest_ownership(trade, order, exchange_order)
                self._pm_record_actual_order(exchange_order, strategy_id, trade.pair)
                self.update_trade_state(
                    trade,
                    strategy_id,
                    exchange_order,
                    stoploss_order=True,
                )
        else:
            with self._exit_lock:
                exchange_order = self.exchange.fetch_order(order.order_id, trade.pair)
                self._pm_validate_rest_ownership(trade, order, exchange_order)
                self.update_trade_state(
                    trade,
                    order.order_id,
                    exchange_order,
                    stoploss_order=False,
                )
        self._pm_finish_stream_incident(pair, event_order_id, order)
        logger.info(
            f"PM user stream reconciled order {event_order_id} on {trade.pair}: "
            f"{exchange_order.get('status')}."
        )
        return True

    def _pm_record_actual_order(
        self, exchange_order: CcxtOrder | dict, strategy_id: str, pair: str | None = None
    ) -> None:
        """Remember the real order id behind a triggered conditional strategy."""
        self._pm_init_user_stream_state()
        actual_id = exchange_order.get("id_stop")
        if actual_id:
            pair = self._pm_canonical_pair(pair or exchange_order.get("symbol"))
            self._pm_actual_order_map[(pair, str(actual_id))] = strategy_id
            self._pm_remember_child_ownership(pair, str(actual_id), strategy_id)

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
        strategy_id = self._pm_actual_order_map.get((pair, order_id))
        if strategy_id is not None:
            # Real order event for a locally known conditional stoploss.
            for trade in Trade.get_open_trades():
                if self._pm_canonical_pair(trade.pair) != pair:
                    continue
                if any(sl.order_id == strategy_id for sl in trade.orders):
                    with self._exit_lock:
                        exchange_order = self.exchange.fetch_stoploss_order(
                            strategy_id, trade.pair
                        )
                        self._pm_record_actual_order(exchange_order, strategy_id, pair)
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
            if self._pm_canonical_pair(trade.pair) != pair:
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
                self._pm_record_actual_order(exchange_order, sl.order_id, pair)
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
            self._pm_block_orders("reconciliation_incomplete")
            self._pm_note_stream_incident(
                pair, order_id, order_data, "conditional-history lookup failed"
            )
            return False

        if client_order_id.startswith(("ft", "st")):
            # Ours, but not in the local index -> we lost track of it.
            self._pm_note_stream_incident(
                pair, order_id, order_data,
                f"{self._pm_unowned_classification(client_order_id)}: "
                "no proven local Order/Trade ownership"
            )
            return False

        event_state = str(order_data.get("X") or order_data.get("x") or "unknown")
        event_key = f"{pair}:{order_id}:{event_state}"
        if len(self._pm_foreign_stream_order_events) > 1000:
            self._pm_foreign_stream_order_events.clear()
        if event_key not in self._pm_foreign_stream_order_events:
            self._pm_foreign_stream_order_events.add(event_key)
            logger.info(
                f"PM user stream: observed foreign order event {order_id} on {pair} "
                f"classification={PMOwnershipClass.EXTERNAL_ORDER} "
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
                previous_dropped = self._pm_user_stream_last_events_dropped
                self._pm_user_stream_state = "DEGRADED"
                self._pm_block_orders("user_stream_events_dropped")
                self._pm_user_stream_last_events_dropped = dropped
                self._pm_order_recovery()
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM User Stream CRITICAL: dropped events increased from "
                            f"{previous_dropped} to {dropped}. "
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
                        self._pm_last_reconciliation_warning = None
                        self._pm_last_reconciliation_warning_at = None
                    else:
                        self._pm_block_orders("reconciliation_incomplete")
                        unresolved_count = self._pm_unresolved_intent_count()
                        signature = self._pm_reconciliation_incident_signature(result)
                        send_warning, is_reminder, reminder_minutes = (
                            self._pm_reconciliation_warning_due(signature)
                        )
                        if send_warning:
                            reminder_text = (
                                f" Incident is still unresolved after {reminder_minutes} minute(s)."
                                if is_reminder
                                else ""
                            )
                            self.rpc.send_msg(
                                {
                                    "type": RPCMessageType.WARNING,
                                    "status": (
                                        "PM FAIL-CLOSED: user stream is connected but "
                                        "reconciliation did not complete cleanly "
                                        f"({len(result['errors'])} error(s), unresolved "
                                        f"intents={unresolved_count}). New orders stay "
                                        "BLOCKED (reconciliation_incomplete). Event-level "
                                        "duplicates are suppressed; unchanged incidents are "
                                        f"reminded every {reminder_minutes} minute(s)."
                                        f"{reminder_text}"
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

    @pm_order_locked
    def _pm_consume_user_stream_events(self, *, allow_account_risk_actions: bool = True) -> None:
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

        order_index: dict[tuple[str, str], tuple[Any, Any]] = {}
        for trade in Trade.get_open_trades():
            for order in trade.orders:
                pair = self._pm_canonical_pair(trade.pair)
                order_index[(pair, str(order.order_id))] = (trade, order)
        # Ownership identity is stable within one batch; reuse proven
        # resolutions so a redelivery storm does not re-query the DB per event.
        self._pm_init_user_stream_state()
        self._pm_owned_resolve_cache.clear()

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
                Trade.session.rollback()
                logger.warning(f"Failed to process Binance PM user stream event {event_type}: {e}")
                if event_type == "ORDER_TRADE_UPDATE":
                    data = event.get("o", {})
                    pair = self._pm_pair_from_exchange_symbol(data.get("s"))
                    if pair:
                        self._pm_note_stream_incident(
                            pair, str(data.get("i") or data.get("c") or ""), data, str(e)
                        )

        if needs_wallet_update:
            self.wallets.update(require_update=True)
        if needs_risk_check and allow_account_risk_actions:
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
                self.set_state(State.STOPPED)
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
                    # Persist the stop BEFORE attempting the closes: a crash
                    # mid-close must never resurrect a RUNNING state on reboot.
                    self.set_state(State.STOPPED)
                    self._pm_emergency_close_all()

        except Exception as e:
            logger.warning(f"PM risk monitor check failed: {e}")
            # Fail-closed: block new orders on any risk API exception.
            self._pm_block_orders("risk_check_failed")
            action = self._pm_risk_failure_action()
            if action == "stop":
                self.set_state(State.STOPPED)
            elif action == "pause":
                self.set_state(State.PAUSED)
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
        """Emergency-close BOT-owned PM exposure and optionally FOREIGN exposure.

        Account risk metrics remain account-wide.  When
        ``emergency_close_foreign_positions=false``, manual/external positions
        are intentionally left untouched; BOT-owned positions and a provable
        BOT orphan (active durable intent on the pair) are still closed and
        verified flat.  Creating an exit order is never treated as proof.
        """
        risk_cfg = self.config.get("exchange", {}).get("portfolio_margin_risk", {})
        max_retries = int(risk_cfg.get("emergency_close_retries", 3))
        close_foreign = self._pm_close_foreign_positions_on_emergency()
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
                        f"enumerated ({e.__class__.__name__}). Manual intervention required."
                    ),
                }
            )
            return
        if not all_positions:
            logger.info("PM emergency close: account is already flat.")
            return

        bot_owned_pairs: set[str] = set()
        ownership_error: str | None = None
        if not close_foreign:
            try:
                bot_owned_pairs = self._pm_active_bot_owned_pairs()
            except Exception as e:
                ownership_error = f"{e.__class__.__name__}: {e}"
                logger.error(
                    "PM emergency close: ownership store unavailable; untracked positions "
                    "will be preserved rather than risk liquidating operator assets: %s", e
                )

        closed = 0
        failed = 0
        preserved = 0
        failed_details: list[str] = []
        preserved_details: list[str] = []
        with self._exit_lock:
            for position in all_positions:
                symbol = str(position.get("symbol") or "")
                side = str(position.get("side") or "")
                contracts = abs(float(position.get("contracts") or 0))
                trade = self._pm_open_trade_for_position(symbol, side)

                if trade is None and not close_foreign:
                    canonical = self._pm_canonical_pair(symbol)
                    if canonical not in bot_owned_pairs:
                        preserved += 1
                        preserved_details.append(f"{canonical} ({side}, {contracts})")
                        logger.warning(
                            "PM emergency close: preserving FOREIGN position %s (%s, %s contracts) "
                            "by policy; account risk metrics still include it.",
                            canonical, side, contracts,
                        )
                        continue

                success = False
                last_error = "unknown"
                for attempt in range(1, max_retries + 1):
                    try:
                        if trade is not None and trade.is_open and trade.has_open_position:
                            if not self._safe_force_exit(trade):
                                last_error = f"attempt {attempt}: exit order not confirmed"
                        else:
                            # No local Trade but durable ownership proves this pair
                            # belongs to an in-flight/orphaned BOT operation.
                            if not self._pm_force_close_foreign_position(symbol, side, contracts):
                                last_error = f"attempt {attempt}: untracked BOT close failed"
                    except Exception as e:
                        last_error = f"attempt {attempt}: {e}"
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
            "PM emergency close: %d closed, %d failed, %d FOREIGN preserved, out of %d total.",
            closed, failed, preserved, len(all_positions),
        )
        if failed:
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM EMERGENCY CLOSE PARTIAL FAILURE: {failed} BOT-owned position(s) "
                        f"could NOT be confirmed flat and remain OPEN: "
                        f"{', '.join(failed_details[:5])}. Manual intervention is required."
                    ),
                }
            )
        if preserved:
            suffix = f" Ownership-store error: {ownership_error}." if ownership_error else ""
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM emergency policy preserved {preserved} FOREIGN/manual position(s) "
                        f"without liquidation: {', '.join(preserved_details[:5])}." + suffix
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

    @pm_order_locked
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
            "transitions": [],
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
                            self._pm_record_actual_order(exchange_order, order.order_id, trade.pair)
                        else:
                            exchange_order = self.exchange.fetch_order(order.order_id, trade.pair)
                        if exchange_order.get("status") != order.status:
                            mismatch_detail = (
                                f"{order.order_id}({trade.pair}): DB={order.status} "
                                f"Exchange={exchange_order.get('status')}"
                            )
                            # REST winning a race with WS is ordinary lifecycle
                            # progress, not lost position ownership.
                            if order.status == "open" and exchange_order.get("status") in {
                                "closed", "canceled", "expired", "rejected"
                            }:
                                result["transitions"].append(mismatch_detail)
                                logger.info("PM order lifecycle synchronized: %s", mismatch_detail)
                            else:
                                result["mismatches"].append(mismatch_detail)
                                logger.warning("PM order state mismatch: %s", mismatch_detail)
                        self._pm_validate_rest_ownership(trade, order, exchange_order)
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
                        Trade.session.rollback()
                        result["errors"].append(f"{order.order_id}: {e}")
                        logger.debug(f"PM order recovery check failed for {order.order_id}: {e}")
            Trade.commit()
            # Reconcile LINKED intents: tombstone only the ones the exchange confirms.
            result["intents"] = self._pm_reconcile_linked_intents()
        except Exception as e:
            logger.warning(f"PM order recovery check failed: {e}")
            result["errors"].append(f"recovery: {e}")
        result["unresolved_intents"] = self._pm_unresolved_intent_count()

        # Account-level quantity invariant: settled local exposure must match the
        # exchange position per instrument. A mismatch is a dedicated SAFE_HOLD
        # reason (risk increase blocked, protection untouched) and auto-releases
        # when quantities reconcile again - never a silent "recovery passed".
        try:
            result["position_mismatches"] = self._pm_account_position_reconcile()
        except Exception as e:
            result["position_mismatches"] = [f"unavailable: {e}"]
        if result["position_mismatches"]:
            self._pm_block_orders("position_quantity_mismatch")
            if not self._pm_position_mismatch_alerted:
                self._pm_position_mismatch_alerted = True
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM SAFE_HOLD: account position quantity mismatch on "
                            f"{len(result['position_mismatches'])} instrument(s). New "
                            f"exposure BLOCKED until quantities reconcile. "
                            f"{'; '.join(result['position_mismatches'][:3])}"
                        ),
                    }
                )
        else:
            self._pm_position_mismatch_alerted = False
            self._pm_unblock_orders("position_quantity_mismatch")

        if self._pm_allow_foreign_positions():
            result["foreign_position_pairs"] = sorted(self._pm_foreign_position_pairs)
            try:
                self._pm_refresh_foreign_order_pairs()
                result["foreign_order_pairs"] = sorted(self._pm_foreign_order_pairs)
            except Exception as e:
                result["foreign_order_pairs"] = sorted(self._pm_foreign_order_pairs)
                result["errors"].append(f"foreign order ownership refresh: {e}")
        else:
            result["foreign_position_pairs"] = []
            result["foreign_order_pairs"] = []

        # Stop-protection invariant: a settled non-zero position must have
        # verifiable conditional protection on the exchange. Unverifiable
        # listing fails closed; missing protection blocks risk increase until
        # the regular loop recreates it (auto-heal).
        try:
            result["unprotected_positions"] = self._pm_verify_stop_protection()
            protection_verifiable = True
        except Exception as e:
            result["unprotected_positions"] = []
            protection_verifiable = False
            result["errors"].append(f"stop protection verification: {e}")
        if protection_verifiable and not result["unprotected_positions"]:
            self._pm_stop_protection_missing_alerted = False
            self._pm_unblock_orders("stop_protection_missing")
        else:
            self._pm_block_orders("stop_protection_missing")
            if result["unprotected_positions"] and not self._pm_stop_protection_missing_alerted:
                self._pm_stop_protection_missing_alerted = True
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM SAFE_HOLD: {len(result['unprotected_positions'])} open "
                            f"position(s) without verified stop protection "
                            f"({result['unprotected_positions'][0]['pair']}). New exposure "
                            "BLOCKED until protection is restored."
                        ),
                    }
                )

        if (
            not result["errors"]
            and not result["position_mismatches"]
            and not result["unprotected_positions"]
        ):
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

    @pm_order_locked
    def _pm_order_recovery(self, clear_unmatched_block: bool = False) -> None:
        result = self._pm_reconcile_open_orders()
        stream_report = self._pm_recover_stream_incidents()
        # A previously unresolvable instrument may now canonicalize (markets are
        # reloaded every loop) - retry so the identity gate can auto-release.
        self._pm_retry_unresolved_instruments()
        # Bounded retention of resolved stream-journal rows (once per hour).
        try:
            self._pm_purge_resolved_journal_if_due()
            Trade.commit()
        except Exception as exc:
            logger.warning("PM stream journal retention purge failed: %s", exc)
            Trade.session.rollback()
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
            self._pm_block_orders("reconciliation_incomplete")
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
        # The shared incident reconciler alone can clear ownership gates, after
        # checking the exact canonical instrument + order, even on scheduled
        # recovery. A command or an empty open-order scan is not authorization.
        if stream_report["errors"]:
            logger.warning("PM stream ownership reconciliation incomplete: %s", stream_report)

    def _pm_purge_resolved_journal_if_due(self) -> int:
        """Bounded cleanup of resolved stream-journal rows (retention window)."""
        self._pm_init_user_stream_state()
        now = _time.monotonic()
        last = getattr(self, "_pm_last_journal_purge_at", None)
        if last is not None and now - last < 3600:
            return 0
        from freqtrade.persistence.pm_stream_journal import PMStreamJournal

        retention_days = int(
            self.config.get("exchange", {})
            .get("portfolio_margin_risk", {})
            .get("user_stream_journal_retention_days", 30)
        )
        purged = PMStreamJournal.purge_resolved(datetime.now(UTC) - timedelta(days=retention_days))
        self._pm_last_journal_purge_at = now
        if purged:
            logger.info("PM stream journal retention purge removed %d resolved rows.", purged)
        return purged

    def _pm_has_open_trade_for(self, pair: str, side: str | None) -> bool:
        """Whether a local open trade exists for an exchange position (pair+side)."""
        for trade in Trade.get_open_trades():
            if (
                self._pm_canonical_pair(trade.pair) != self._pm_canonical_pair(pair)
                or not trade.is_open
            ):
                continue
            if side is None:
                return True
            if trade.is_short == (side == "short"):
                return True
        return False

    def _pm_account_position_reconcile(self) -> list[str]:
        """BOT-owned PM quantity invariant with explicit FOREIGN coexistence.

        With ``allow_foreign_positions=false`` (legacy/default), every non-zero
        exchange position participates in the full-account quantity invariant.

        With coexistence enabled, a position on an instrument with NO active BOT
        ownership evidence is classified FOREIGN/MANUAL: it remains visible to
        account equity/uniMMR, is never imported into Trade, and does not trigger
        the global quantity gate.  The pair is remembered so exposure-increasing
        BOT orders on that SAME instrument are refused by the pair-local gate.

        Instruments with local open Trades or active durable PM intents remain
        BOT-owned and keep the strict bounded in-flight invariant.  Therefore a
        lost/misaligned BOT position still blocks ALL new exposure fail-closed.
        """
        mismatches: list[str] = []
        if not (
            self.trading_mode == TradingMode.FUTURES
            and not self.config.get("dry_run", True)
            and getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
        ):
            return mismatches
        self._pm_init_user_stream_state()
        allow_foreign = self._pm_allow_foreign_positions()
        positions = self.exchange.fetch_positions()
        exchange_by_pair: dict[str, float] = {}
        for position in positions:
            contracts = float(position.get("contracts", 0) or 0)
            raw = position.get("symbol")
            if not raw or contracts == 0:
                continue
            pair = self._pm_canonical_pair(raw)
            signed = contracts if position.get("side") != "short" else -contracts
            exchange_by_pair[pair] = exchange_by_pair.get(pair, 0.0) + signed

        tolerance = float(
            self.config.get("exchange", {})
            .get("portfolio_margin_risk", {})
            .get("account_position_quantity_tolerance", 0.01)
        )
        inflight_max_s = float(
            self.config.get("exchange", {})
            .get("portfolio_margin_risk", {})
            .get("account_position_inflight_max_seconds", 3600)
        )
        now = datetime.now(UTC)
        local_by_pair: dict[str, float] = {}
        inflight: dict[str, dict[str, Any]] = {}
        for trade in Trade.get_open_trades():
            if not trade.is_open:
                continue
            pair = self._pm_canonical_pair(trade.pair)
            trade_sign = -1.0 if trade.is_short else 1.0
            signed = float(trade.amount) * trade_sign
            local_by_pair[pair] = local_by_pair.get(pair, 0.0) + signed
            entry = inflight.setdefault(pair, {"low": 0.0, "high": 0.0, "stale": False})
            for order in trade.open_orders:
                try:
                    filled = float(order.filled or 0)
                    total = float(order.amount or 0)
                except (TypeError, ValueError):
                    continue
                remaining = max(total - filled, 0.0)
                if remaining <= 0:
                    continue
                delta_sign = 1.0 if str(order.side) == trade.entry_side else -1.0
                delta = trade_sign * delta_sign * remaining
                entry["low"] = min(entry["low"], delta)
                entry["high"] = max(entry["high"], delta)
                order_date = order.order_date
                if order_date is not None:
                    if order_date.tzinfo is None:
                        order_date = order_date.replace(tzinfo=UTC)
                    if (now - order_date).total_seconds() > inflight_max_s:
                        entry["stale"] = True

        bot_owned_pairs = set(local_by_pair)
        if allow_foreign:
            # Active intent rows are the crash-window ownership proof for a BOT
            # order whose local Trade may not have committed yet. A store failure
            # propagates to the caller and therefore remains fail-closed.
            bot_owned_pairs |= self._pm_active_bot_owned_pairs()
            foreign_pairs = {
                pair for pair in exchange_by_pair if pair not in bot_owned_pairs
            }
            self._pm_foreign_position_pairs = foreign_pairs
        else:
            self._pm_foreign_position_pairs = set()
            bot_owned_pairs |= set(exchange_by_pair)

        for pair in sorted(set(exchange_by_pair) | set(local_by_pair)):
            if allow_foreign and pair in self._pm_foreign_position_pairs:
                continue
            exchange_amount = self.exchange._contracts_to_amount(
                pair, exchange_by_pair.get(pair, 0.0)
            )
            local_amount = local_by_pair.get(pair, 0.0)
            tol = max(abs(local_amount) * tolerance, 1e-9)
            entry = inflight.get(pair)
            if entry and not entry["stale"]:
                if (
                    local_amount + entry["low"] - tol
                    <= exchange_amount
                    <= local_amount + entry["high"] + tol
                ):
                    continue
            if abs(exchange_amount - local_amount) <= tol:
                continue
            mismatches.append(
                f"{pair}: exchange={exchange_amount!r} local={local_amount!r}"
                f" inflight={entry or {}}"
            )
        return mismatches


    def _pm_stop_identity_matches(self, trade: Trade, expected_id: str, check: Any) -> bool:
        """No lifecycle mutation without independent instrument, id and side evidence."""
        if not isinstance(check, dict) or not check.get("symbol"):
            return False
        try:
            return bool(
                str(check.get("id") or "") == str(expected_id)
                and self._pm_canonical_pair(check["symbol"]) == self._pm_canonical_pair(trade.pair)
                and str(check.get("side") or "").lower() == trade.exit_side
            )
        except OperationalException:
            return False

    def _pm_validate_protection_order(
        self,
        trade: Trade,
        expected_id: str,
        check: CcxtOrder | dict | None,
        *,
        require_reduce_only: bool = False,
    ) -> tuple[str, Any]:
        """Strictly validate one conditional protection order against the exchange.

        Returns (verdict, order) where verdict is one of:

        * ``active``: working trigger-pending conditional that really protects
          the position.
        * ``terminal``: the stop/triggered child is fully filled; the caller must
          process the fill before any other action.
        * ``triggered_pending``: the conditional fired but the actual child is
          absent/not-yet-visible or still OPEN/PARTIALLY_FILLED. This is an
          in-flight risk-reducing exit, not proof the Trade is closed.
        * ``triggered_failed``: the child is terminal without fully covering the
          remaining Trade (canceled/rejected/expired/partial terminal). Process
          any partial fill, then protect/exit the remaining position.
        * ``invalid``: wrong identity/semantics or a non-triggered terminal stop.
        """
        if not self._pm_stop_identity_matches(trade, expected_id, check):
            return "invalid", check
        pair = self._pm_canonical_pair(trade.pair)
        # 1. canonical instrument exact match (never by bare order id)
        try:
            check_pair = self._pm_canonical_pair(check.get("symbol") or trade.pair)
        except OperationalException:
            return "invalid", check
        if check_pair != pair:
            return "invalid", check
        # 2. expected exchange order id / child alias identity
        if str(check.get("id") or "") != str(expected_id):
            return "invalid", check
        status = str(check.get("status") or "").lower()
        triggered = str(check.get("status_stop") or "").lower() == "triggered"
        # 3. correct exit side
        if str(check.get("side") or "").lower() != trade.exit_side:
            return "invalid", check
        # 4. positionSide semantics (one-way "BOTH" or matching hedge side)
        info = check.get("info") if isinstance(check, dict) else {}
        info = info or {}
        position_side = str(info.get("positionSide") or "").upper()
        if position_side:
            expected_sides = {"BOTH", "LONG" if not trade.is_short else "SHORT"}
            if position_side not in expected_sides:
                return "invalid", check
        # 5. reduceOnly / closePosition semantics.
        reduce_only = info.get("reduceOnly")
        if reduce_only is None and isinstance(info.get("actual_order"), dict):
            reduce_only = (info.get("actual_order") or {}).get("reduceOnly")
            if reduce_only is None:
                reduce_only = ((info.get("actual_order") or {}).get("info") or {}).get("reduceOnly")
        if reduce_only is not None and str(reduce_only).lower() not in {"true", "1"}:
            return "invalid", check
        if require_reduce_only and reduce_only is None:
            return "invalid", check
        # 6. requested/child quantity must cover the remaining local exposure.
        quantity = check.get("amount")
        if quantity is None:
            return "invalid", check
        try:
            qty = float(quantity)
            trade_amount = float(trade.amount)
            tol = max(abs(trade_amount) * 1e-6, 1e-9)
            if not isfinite(qty) or not isfinite(trade_amount) or qty <= 0:
                return "invalid", check
            if qty + tol < trade_amount:
                return "invalid", check
        except (TypeError, ValueError):
            return "invalid", check

        if triggered:
            actual = info.get("actual_order") if isinstance(info, dict) else None
            # Trigger acknowledgement alone is NOT a fill. If the real child is
            # not visible yet, keep it as an in-flight exit and keep polling.
            if not isinstance(actual, dict):
                return "triggered_pending", check
            if status in {"open", "new"}:
                return "triggered_pending", check
            if status in {"canceled", "cancelled", "rejected", "expired"}:
                return "triggered_failed", check
            if status in {"closed", "filled"}:
                try:
                    filled = float(check.get("filled") or 0.0)
                except (TypeError, ValueError):
                    filled = 0.0
                return ("terminal" if filled + tol >= trade_amount else "triggered_failed"), check
            return "triggered_pending", check

        if status in {"closed", "filled"}:
            return "terminal", check
        if status not in {"open", "new"}:
            return "invalid", check
        return "active", check

    def _pm_protection_hold(self, trade: Trade, why: str) -> None:
        """Hold risk increase while protection is unverified; keep exits open."""
        self._pm_init_user_stream_state()
        self._pm_block_orders("stop_protection_missing")
        if not self._pm_stop_protection_missing_alerted:
            self._pm_stop_protection_missing_alerted = True
            try:
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            f"PM SAFE_HOLD: verified stop protection for Trade #{trade.id} "
                            f"{trade.pair} could not be established ({why}). New exposure "
                            "is BLOCKED; recovery and risk-reducing exits remain enabled."
                        ),
                        "incident_id": f"pm-stop-missing-trade-{trade.id}",
                        "trade_id": trade.id,
                        "pair": trade.pair,
                        "requires_open_trade": True,
                    }
                )
            except Exception:
                pass

    def _pm_stop_retire_hold(self, trade: Trade, why: str) -> None:
        """A verified replacement exists, but an OLD stop lifecycle is unresolved.

        This is deliberately distinct from ``stop_protection_missing``: the
        position still has verified protection, while the old conditional needs
        recovery so we do not risk a duplicate reduce-only child later.
        """
        self._pm_block_orders("stop_retire_unresolved")
        self.log_once(
            f"PM stop retirement pending for Trade #{trade.id} {trade.pair}: {why}. "
            "Verified replacement remains active; new exposure is blocked until "
            "the old lifecycle is reconciled.",
            logger.warning,
        )
        try:
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM RECOVERY: Trade #{trade.id} {trade.pair} has verified active "
                        f"replacement protection, but an old stop lifecycle is unresolved ({why}). "
                        "New exposure is blocked; protection is still present."
                    ),
                    "incident_id": f"pm-stop-retire-trade-{trade.id}",
                    "trade_id": trade.id,
                    "pair": trade.pair,
                    "requires_open_trade": True,
                }
            )
        except Exception:
            pass

    def _pm_triggered_exit_hold(self, trade: Trade, why: str) -> None:
        """Conditional fired and its actual reduce-only child is still in flight."""
        self._pm_block_orders("stop_triggered_exit_pending")
        self.log_once(
            f"PM triggered stop exit pending for Trade #{trade.id} {trade.pair}: {why}. "
            "The child order is being reconciled; new exposure is blocked.",
            logger.warning,
        )
        try:
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM EXIT PENDING: Trade #{trade.id} {trade.pair} stop has triggered, "
                        f"but the actual reduce-only child is not terminal ({why}). "
                        "Partial fills are being reconciled; no completion is claimed."
                    ),
                    "incident_id": f"pm-stop-child-trade-{trade.id}",
                    "trade_id": trade.id,
                    "pair": trade.pair,
                    "requires_open_trade": True,
                }
            )
        except Exception:
            pass

    def _pm_retire_stop_ids(self, trade: Trade, old_ids: list[str]) -> bool:
        """Retire only explicitly verified old stop ids.

        A DELETE returning "not found" is NOT a cancellation proof: the strategy
        may have triggered, become temporarily invisible, or already reached some
        other terminal state.  In that case query the full conditional lifecycle
        (including its triggered real child order) and only update the local Order
        from explicit exchange truth.  Ambiguous absence stays open locally and
        latches protection SAFE_HOLD for recovery.
        """
        all_resolved = True
        for old_id in sorted(set(old_ids)):
            try:
                co = self.exchange.cancel_stoploss_order_with_result(
                    old_id, trade.pair, trade.amount
                )
            except InvalidOrderException:
                try:
                    check = self.exchange.fetch_stoploss_order(old_id, trade.pair)
                except InvalidOrderException:
                    all_resolved = False
                    logger.warning(
                        "PM stop retire %s/%s: cancel returned not-found and a full "
                        "open/history/trigger-child lookup still could not establish a "
                        "terminal state. Keeping the local stop pending.",
                        trade.pair,
                        old_id,
                    )
                    self._pm_stop_retire_hold(
                        trade, f"old stop {old_id} terminal state is unconfirmed"
                    )
                    continue
                except Exception as e:
                    all_resolved = False
                    logger.warning(
                        "PM stop retire %s/%s: cancel result is ambiguous and lifecycle "
                        "lookup failed (%s). Keeping the local stop pending.",
                        trade.pair,
                        old_id,
                        e,
                    )
                    self._pm_stop_retire_hold(
                        trade, f"old stop {old_id} lifecycle lookup failed"
                    )
                    continue

                status = str((check or {}).get("status") or "").lower()
                status_stop = str((check or {}).get("status_stop") or "").lower()
                identity_matches = bool(
                    check
                    and str(check.get("id") or "") == str(old_id)
                    and check.get("symbol")
                    and self._pm_canonical_pair(check["symbol"])
                    == self._pm_canonical_pair(trade.pair)
                    and str(check.get("side") or "").lower() == trade.exit_side
                )
                if not identity_matches:
                    all_resolved = False
                    self._pm_stop_retire_hold(trade, f"old stop {old_id} identity unverified")
                    continue
                if status_stop == "triggered":
                    actual = (check.get("info") or {}).get("actual_order")
                    if not isinstance(actual, dict) or not actual.get("id"):
                        all_resolved = False
                        self._pm_triggered_exit_hold(trade, f"old stop {old_id} child unavailable")
                        continue
                    # Book observed fills, but never call an OPEN child resolved.
                    self.update_trade_state(trade, old_id, check, stoploss_order=True)
                    if status not in {
                        "closed", "filled", "canceled", "cancelled", "expired", "rejected"
                    }:
                        all_resolved = False
                        self._pm_triggered_exit_hold(trade, f"old stop {old_id} child still open")
                    continue
                if status in {"closed", "filled", "canceled", "cancelled", "expired", "rejected"}:
                    self.update_trade_state(trade, old_id, check, stoploss_order=True)
                    continue

                # Exchange still sees an active/unknown record after DELETE said
                # not-found.  Do not pretend retirement completed.
                all_resolved = False
                logger.warning(
                    "PM stop retire %s/%s: lifecycle lookup returned non-terminal "
                    "status=%s status_stop=%s; keeping recovery pending.",
                    trade.pair,
                    old_id,
                    status or "unknown",
                    status_stop or "none",
                )
                self._pm_stop_retire_hold(
                    trade, f"old stop {old_id} remains non-terminal after cancel ambiguity"
                )
                continue
            self.update_trade_state(trade, old_id, co, stoploss_order=True)
        return all_resolved

    def _pm_replace_stop_protection(
        self,
        trade: Trade,
        old_ids: list[str],
        *,
        new_stop_price: float | None = None,
    ) -> str:
        """Unified protection replacement primitive (all PM paths):

            CREATE NEW -> PERSIST CANDIDATE -> FETCH EXCHANGE TRUTH
            -> VALIDATE NEW -> only then RETIRE OLD

        Returns "active" (replacement verified, old retired), "kept_old"
        (replacement not created/verified - old protection stays on the
        exchange, risk increase held), "terminal" (replacement already fired -
        the fill was processed first; old conditionals were NOT canceled) or
        "not_required" (trade closed/flat).
        """
        old_ids = sorted({str(o) for o in old_ids})
        if not trade.is_open or not trade.has_open_position:
            return "not_required"
        stop_price = (
            new_stop_price if new_stop_price is not None else trade.stoploss_or_liquidation
        )
        old_ids_before = {str(sl.order_id) for sl in trade.open_sl_orders}
        if not self.create_stoploss_order(trade=trade, stop_price=stop_price):
            if trade.is_open and trade.has_open_position:
                self._pm_protection_hold(trade, "replacement creation failed")
            return "kept_old"
        if not trade.is_open:
            return "not_required"
        new_ids = {str(sl.order_id) for sl in trade.open_sl_orders} - old_ids_before
        if len(new_ids) != 1:
            self._pm_protection_hold(
                trade, f"expected one replacement conditional, found {len(new_ids)}"
            )
            return "kept_old"
        new_id = next(iter(new_ids))
        try:
            check = self.exchange.fetch_stoploss_order(new_id, trade.pair)
        except Exception as e:
            logger.warning(
                "PM protection replace: could not verify new conditional %s (%s); "
                "keeping the old protection.",
                new_id,
                e,
            )
            self._pm_protection_hold(trade, "replacement verification fetch failed")
            return "kept_old"
        verdict, _ = self._pm_validate_protection_order(trade, new_id, check)
        if verdict == "invalid":
            self._pm_protection_hold(trade, "replacement failed validation")
            return "kept_old"
        if verdict == "triggered_pending":
            # Triggering only created/started the real reduce-only child. Process
            # any partial fill now, but never call it a completed exit. The OLD
            # stop is retained and no replacement/duplicate child is submitted.
            self.update_trade_state(trade, new_id, check, stoploss_order=True)
            if trade.is_open and trade.has_open_position:
                self._pm_triggered_exit_hold(
                    trade, f"replacement {new_id} child is not terminal yet"
                )
            return "triggered_pending"
        if verdict == "triggered_failed":
            # A triggered child reached a terminal state without fully covering
            # the Trade. First book any partial fill, then reduce the remaining
            # BOT-owned exposure through the normal idempotent emergency-exit
            # path. Never fabricate a filled/canceled stop terminal.
            self.update_trade_state(trade, new_id, check, stoploss_order=True)
            if trade.is_open and trade.has_open_position:
                self._pm_protection_hold(
                    trade, f"triggered child for {new_id} ended before full fill"
                )
                self.emergency_exit(trade, trade.stoploss_or_liquidation)
            return "triggered_failed"
        if verdict == "terminal":
            logger.warning(
                "PM protection replace: replacement %s for %s is fully terminal; "
                "processing the fill before any further protection action.",
                new_id,
                trade.pair,
            )
            self.update_trade_state(trade, new_id, check, stoploss_order=True)
            if trade.is_open and trade.has_open_position:
                self._pm_protection_hold(
                    trade, f"terminal stop {new_id} left residual exposure"
                )
                self.emergency_exit(trade, trade.stoploss_or_liquidation)
            return "terminal"
        retired = self._pm_retire_stop_ids(trade, [o for o in old_ids if o != new_id])
        if not retired:
            # NEW protection is verified and stays active, but at least one OLD
            # stop has an unconfirmed terminal/cancel state.  Keep the gate closed
            # until recovery resolves that lifecycle instead of reporting success.
            return "kept_old"
        return "active"

    def _pm_switch_trailing_stoploss(
        self, trade: Trade, old_order: CcxtOrder, stoploss_norm: float
    ) -> None:
        """PM-safe trailing-stop replacement (unified primitive)."""
        verdict = self._pm_replace_stop_protection(
            trade, [str(old_order["id"])], new_stop_price=stoploss_norm
        )
        if verdict == "active":
            logger.info(
                "PM trailing stop switch: %s protection replaced %s.",
                trade.pair,
                old_order["id"],
            )

    def _pm_resize_stop_protection(self, trade: Trade) -> str:
        """DCA/entry-fill resize: the OLD conditional stays active until a
        verified replacement exists on the exchange (unified primitive)."""
        old_ids = [str(sl.order_id) for sl in trade.open_sl_orders]
        if not old_ids:
            if not self.strategy.order_types.get("stoploss_on_exchange"):
                return "not_required"
            # A partially-filled first entry can create real exposure before the
            # regular stoploss loop runs.  Create+verify the first protection
            # immediately through the same primitive instead of leaving a gap.
            return self._pm_replace_stop_protection(
                trade, [], new_stop_price=trade.stoploss_or_liquidation
            )
        verdict = self._pm_replace_stop_protection(
            trade, old_ids, new_stop_price=trade.stoploss_or_liquidation
        )
        if verdict == "kept_old":
            logger.warning(
                "PM stop protection resize kept the previous protection for %s.",
                trade.pair,
            )
        elif verdict == "active":
            logger.info("PM stop protection resized for %s.", trade.pair)
        return verdict

    def _pm_verify_stop_protection(self) -> list[dict[str, Any]]:
        """Return BOT Trades whose remaining exposure lacks verified protection.

        A verified replacement plus an unresolved OLD stop is tracked separately
        as ``stop_retire_unresolved``. A triggered child that is still working or
        partially filled is tracked as ``stop_triggered_exit_pending``. Neither is
        mislabeled as missing protection.
        """
        offenders: list[dict[str, Any]] = []
        retire_pending: list[dict[str, Any]] = []
        triggered_pending: list[dict[str, Any]] = []
        if not (
            self.trading_mode == TradingMode.FUTURES
            and not self.config.get("dry_run", True)
            and getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
        ):
            return offenders
        if not self.strategy.order_types.get("stoploss_on_exchange"):
            return offenders
        self._pm_init_user_stream_state()
        open_conditionals = self.exchange.fetch_open_conditional_orders()
        by_key: dict[tuple[str, str], Any] = {}
        for order in open_conditionals:
            raw_pair = order.get("symbol")
            try:
                pair = self._pm_canonical_pair(raw_pair) if raw_pair else ""
            except OperationalException:
                pair = str(raw_pair or "")
            by_key[(pair, str(order.get("id") or order.get("clientAlgoId") or ""))] = order

        for trade in Trade.get_open_trades():
            if not trade.is_open or not trade.has_open_position:
                continue
            pair = self._pm_canonical_pair(trade.pair)
            local_ids = {str(sl.order_id) for sl in trade.open_sl_orders}
            verified: set[str] = set()
            pending_exit_ids: set[str] = set()
            unresolved_old_ids: set[str] = set()
            problems: list[str] = []

            for sl in list(trade.open_sl_orders):
                sid = str(sl.order_id)
                order = by_key.get((pair, sid))
                if order is None:
                    try:
                        # Open-list absence does not establish a terminal state.
                        # Resolve open -> history -> actual child before deciding.
                        order = self.exchange.fetch_stoploss_order(sid, trade.pair)
                    except InvalidOrderException:
                        unresolved_old_ids.add(sid)
                        problems.append(f"{sid}:absent_open_and_history")
                        continue
                    except Exception as exc:
                        unresolved_old_ids.add(sid)
                        problems.append(f"{sid}:lookup_{exc.__class__.__name__}")
                        continue

                if not self._pm_stop_identity_matches(trade, sid, order):
                    unresolved_old_ids.add(sid)
                    problems.append(f"{sid}:identity_unverified")
                    continue
                verdict, checked = self._pm_validate_protection_order(
                    trade, sid, order, require_reduce_only=True
                )
                status = str((checked or {}).get("status") or "").lower()
                if verdict == "active":
                    verified.add(sid)
                    continue
                if verdict == "triggered_pending":
                    # Book any partial child fill but keep the Trade open until
                    # actual terminal exchange evidence arrives.
                    self.update_trade_state(
                        trade, sid, checked, stoploss_order=True, send_msg=False
                    )
                    if trade.is_open and trade.has_open_position:
                        pending_exit_ids.add(sid)
                        problems.append(f"{sid}:triggered_pending")
                    continue
                if verdict in {"terminal", "triggered_failed"}:
                    self.update_trade_state(
                        trade, sid, checked, stoploss_order=True, send_msg=False
                    )
                    if trade.is_open and trade.has_open_position:
                        problems.append(f"{sid}:{verdict}_residual")
                    continue
                if status in {"canceled", "cancelled", "expired", "rejected"}:
                    # Explicit terminal truth is safe to commit locally.
                    self.update_trade_state(
                        trade, sid, checked, stoploss_order=True, send_msg=False
                    )
                    continue
                unresolved_old_ids.add(sid)
                problems.append(f"{sid}:{verdict}")

            if not trade.is_open or not trade.has_open_position:
                continue
            if verified:
                unresolved = sorted((local_ids - verified) | unresolved_old_ids)
                if unresolved:
                    retire_pending.append(
                        {
                            "trade_id": trade.id,
                            "pair": trade.pair,
                            "old_ids": unresolved,
                            "verified_ids": sorted(verified),
                        }
                    )
                continue
            if pending_exit_ids:
                triggered_pending.append(
                    {
                        "trade_id": trade.id,
                        "pair": trade.pair,
                        "ids": sorted(pending_exit_ids),
                    }
                )
                continue
            offenders.append(
                {
                    "trade_id": trade.id,
                    "pair": trade.pair,
                    "local_ids": sorted(local_ids),
                    "problems": problems or ["no verified stop protection"],
                }
            )

        if retire_pending:
            self._pm_block_orders("stop_retire_unresolved")
        else:
            self._pm_unblock_orders("stop_retire_unresolved")
        if triggered_pending:
            self._pm_block_orders("stop_triggered_exit_pending")
        else:
            self._pm_unblock_orders("stop_triggered_exit_pending")
        return offenders

    def _pm_origin_trade_kwargs(self, trade: Trade | None) -> dict[str, int]:
        """Attach immutable pre-send Trade ownership only for live Binance PM."""
        if trade is None or trade.id is None or not self._pm_db_gate_active():
            return {}
        return {"origin_trade_id": int(trade.id)}

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

    def _pm_find_local_order(
        self, exchange_id: str, client_id: str, pair: str
    ) -> dict[str, Any] | None:
        """
        Find the local Order row for a resolved exchange order.

        Regular PM orders store the exchange orderId locally; conditional
        (stoploss) orders store the client algo id. Either identifier may match.
        """
        entry = self._pm_owned_order(self._pm_canonical_pair(pair), exchange_id, client_id, {})
        if entry:
            trade, order = entry
            return {"order_id": str(order.order_id), "trade_id": trade.id}
        return None

    def _pm_adopt_orphan_reduce_only_order(
        self, intent: dict[str, Any], exchange_order: CcxtOrder | dict[str, Any]
    ) -> tuple[Trade, Order] | None:
        """Adopt only a fully evidenced ACK->commit orphaned reduce-only exit.

        Binance identity fields are never filled from the intent.  The durable
        intent is a second source to compare against, not a substitute for missing
        exchange evidence.  Trade ownership comes only from ``origin_trade_id``
        persisted before POST.
        """
        client_id = str(intent.get("client_id") or "")
        if not client_id:
            return None
        persisted = PMOrderIntent.get_by_client_id(client_id)
        if persisted is None or persisted.state != "ACKED":
            return None
        if persisted.kind != "order" or not bool(persisted.reduce_only):
            return None
        if persisted.origin_trade_id is None or int(persisted.origin_trade_id) < 1:
            return None
        if not persisted.exchange_order_id:
            return None

        exchange_id = str(exchange_order.get("id") or "")
        exchange_symbol = str(exchange_order.get("symbol") or "")
        exchange_client_id = str(exchange_order.get("clientOrderId") or "")
        exchange_side = str(exchange_order.get("side") or "").lower()
        if not all((exchange_id, exchange_symbol, exchange_client_id, exchange_side)):
            return None
        if exchange_side not in {"buy", "sell"}:
            return None
        if exchange_client_id != client_id:
            return None
        if str(persisted.exchange_order_id) != exchange_id:
            return None

        info = exchange_order.get("info") if isinstance(exchange_order, dict) else None
        info = info if isinstance(info, dict) else {}
        missing = object()
        reduce_only_evidence = exchange_order.get("reduceOnly", missing)
        if reduce_only_evidence is missing:
            reduce_only_evidence = info.get("reduceOnly", missing)
        if reduce_only_evidence is missing:
            return None
        if str(reduce_only_evidence).lower() not in {"true", "1"}:
            return None

        try:
            exchange_amount = float(exchange_order.get("amount"))
        except (TypeError, ValueError):
            return None
        if exchange_amount <= 0:
            return None

        try:
            pair = self._pm_canonical_pair(str(persisted.pair or ""))
            order_pair = self._pm_canonical_pair(exchange_symbol)
        except OperationalException:
            return None
        if order_pair != pair:
            return None
        if str(persisted.side or "").lower() != exchange_side:
            return None
        if persisted.amount is None:
            return None
        try:
            expected = float(persisted.amount)
            if hasattr(self.exchange, "_contracts_to_amount"):
                expected = float(self.exchange._contracts_to_amount(pair, expected))
            tol = max(abs(exchange_amount) * 1e-6, 1e-9)
            if not isclose(expected, exchange_amount, rel_tol=1e-6, abs_tol=tol):
                return None
        except (TypeError, ValueError):
            return None

        trade = Trade.session.get(Trade, int(persisted.origin_trade_id))
        if trade is None or not trade.is_open:
            return None
        if self._pm_canonical_pair(trade.pair) != pair or exchange_side != trade.exit_side:
            return None
        tol = max(abs(float(trade.amount)) * 1e-6, 1e-9)
        if not isclose(float(trade.amount), exchange_amount, rel_tol=1e-6, abs_tol=tol):
            return None
        if any(
            o.ft_order_side == trade.exit_side and str(o.order_id) != exchange_id
            for o in trade.orders
            if o.ft_order_side != "stoploss"
        ):
            return None
        if Order.order_by_id(exchange_id) is not None:
            return None

        order_obj = Order.parse_from_ccxt_object(
            exchange_order, trade.pair, trade.exit_side, exchange_amount
        )
        order_obj.ft_order_tag = trade.exit_reason or "pm_recovered_exit"
        trade.exit_reason = trade.exit_reason or "pm_recovered_exit"
        trade.orders.append(order_obj)
        logger.warning(
            "PM recovery adopted fully-evidenced reduce-only orphan %s "
            "(clientId=%s, originTrade=%s) into Trade #%s %s; local lifecycle "
            "reconciliation will now finish it.",
            exchange_id, client_id, persisted.origin_trade_id, trade.id, trade.pair
        )
        return trade, order_obj

    def _pm_adopt_orphan_conditional_order(
        self, intent: dict[str, Any], exchange_order: CcxtOrder | dict[str, Any]
    ) -> tuple[Trade, Order] | None:
        """Adopt a BOT-owned stoploss that ACKed before its local Order commit.

        Ownership is never inferred from pair/size alone: the persisted
        ``origin_trade_id`` must name the exact Trade and the exchange response
        must independently prove clientAlgoId, pair, side, reduce-only semantics
        and protected quantity.
        """
        client_id = str(intent.get("client_id") or "")
        if not client_id:
            return None
        persisted = PMOrderIntent.get_by_client_id(client_id)
        if (
            persisted is None
            or persisted.state != "ACKED"
            or persisted.kind != "conditional"
            or not bool(persisted.reduce_only)
            or persisted.origin_trade_id is None
            or int(persisted.origin_trade_id) < 1
        ):
            return None
        exchange_id = str(exchange_order.get("id") or "")
        exchange_client_id = str(
            exchange_order.get("clientAlgoId")
            or (exchange_order.get("info") or {}).get("clientAlgoId")
            or ""
        )
        exchange_symbol = str(exchange_order.get("symbol") or "")
        exchange_side = str(exchange_order.get("side") or "").lower()
        if not all((exchange_id, exchange_client_id, exchange_symbol, exchange_side)):
            return None
        if exchange_id != client_id or exchange_client_id != client_id:
            return None
        if persisted.exchange_order_id and str(persisted.exchange_order_id) != exchange_id:
            return None
        try:
            pair = self._pm_canonical_pair(str(persisted.pair or ""))
            if self._pm_canonical_pair(exchange_symbol) != pair:
                return None
        except OperationalException:
            return None
        if str(persisted.side or "").lower() != exchange_side:
            return None
        info = exchange_order.get("info") if isinstance(exchange_order, dict) else {}
        info = info if isinstance(info, dict) else {}
        reduce_only = exchange_order.get("reduceOnly")
        if reduce_only is None:
            reduce_only = info.get("reduceOnly")
        if str(reduce_only).lower() not in {"true", "1"}:
            return None
        trade = Trade.session.get(Trade, int(persisted.origin_trade_id))
        if trade is None or not trade.is_open or not trade.has_open_position:
            return None
        if self._pm_canonical_pair(trade.pair) != pair or trade.exit_side != exchange_side:
            return None
        try:
            exchange_amount = float(exchange_order.get("amount"))
            expected_amount = float(persisted.amount)
            tol = max(abs(float(trade.amount)) * 1e-6, 1e-9)
        except (TypeError, ValueError):
            return None
        if not isclose(exchange_amount, expected_amount, rel_tol=1e-6, abs_tol=tol):
            return None
        if not isclose(exchange_amount, float(trade.amount), rel_tol=1e-6, abs_tol=tol):
            return None
        if persisted.stop_price is not None and exchange_order.get("stopPrice") is not None:
            try:
                if not isclose(
                    float(exchange_order.get("stopPrice")),
                    float(persisted.stop_price),
                    rel_tol=1e-8,
                    abs_tol=max(abs(float(persisted.stop_price)) * 1e-8, 1e-9),
                ):
                    return None
            except (TypeError, ValueError):
                return None
        if Order.order_by_id(client_id) is not None:
            return None
        order_obj = Order.parse_from_ccxt_object(
            exchange_order, trade.pair, "stoploss", trade.amount, persisted.stop_price
        )
        trade.orders.append(order_obj)
        logger.warning(
            "PM recovery adopted fully-evidenced conditional orphan %s "
            "(originTrade=%s) into Trade #%s %s.",
            client_id, persisted.origin_trade_id, trade.id, trade.pair,
        )
        return trade, order_obj

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
        # Optional live wall-clock bound for the strategy candle.  This catches
        # a refresh/analysis path that stopped advancing even if its last row is
        # internally well-formed.  The live config sets this to > 2x the 5m
        # candle spacing so a normal currently-forming candle is not rejected.
        max_age_s = float(
            self.config.get("exchange", {})
            .get("portfolio_margin_risk", {})
            .get("market_data_max_candle_age_seconds", 0)
            or 0
        )
        if max_age_s > 0 and snapshot.get("candle_open_time") is not None:
            candle_ts = pd.to_datetime(snapshot["candle_open_time"], utc=True).to_pydatetime()
            candle_age_s = max(0.0, (datetime.now(UTC) - candle_ts).total_seconds())
            if candle_age_s > max_age_s:
                return snapshot, f"market_data_stale: candle_age={candle_age_s:.1f}s"
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

    def _pm_exposure_data_block_reason(self, pair: str) -> str | None:
        """Unified data-safety gate for every exposure-increasing entry path."""
        cycle_reason = getattr(self, "_pm_cycle_exposure_block_reason", None)
        if cycle_reason:
            return str(cycle_reason)
        _snapshot, block_reason = self._pm_ledger_snapshot(pair)
        return block_reason

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
        Record the immutable factor snapshot plus an append-only decision event
        for the current closed candle of ``pair``.

        Factor computation may repeat after restart.  The snapshot row remains
        immutable while blocked/submitted/result transitions append under one
        stable decision id. Entry-scope snapshot creation additionally advances
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
            # The ledger/watermark is an audit/progress cursor, NOT an
            # exactly-once execution primitive (intent/outbox owns side-effect
            # idempotency). A ledger
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
        if decision_scope in {"entry", "dca", "replace"}:
            block_reason = getattr(self, "_pm_cycle_exposure_block_reason", None) or block_reason
        from freqtrade.persistence.pm_candle_watermark import PMCandleWatermark
        from freqtrade.persistence.pm_signal_decision_event import PMSignalDecisionEvent
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
        if decision == "no_signal" and reason in (None, "no_entry_signal"):
            reason = str(snapshot.get("no_signal_detail") or "no_entry_signal")
        existing_snapshot = PMSignalLedger.get_by_candle(
            pair, timeframe, candle_open, decision_scope
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
        # ``autoflush`` is disabled for the trading session. Flush the immutable
        # snapshot now so the append-only event has a real join id and the optional
        # client-id backfill can query the row in this same transaction.
        Trade.session.flush()
        if order_client_id:
            PMSignalLedger.set_order_client_id(
                pair, timeframe, candle_open, order_client_id, decision_scope
            )
        _event, event_created = PMSignalDecisionEvent.append_event(
            signal_ledger_id=row.id,
            pair=pair,
            timeframe=timeframe,
            candle_open_time=candle_open,
            decision_scope=decision_scope,
            strategy_version=row.strategy_version,
            factor_hash=row.factor_hash,
            decision=decision,
            decision_reason=reason,
            order_client_id=order_client_id,
        )
        if (
            decision_scope == "entry"
            and watermark is not None
            and not gap_latched_now
            and existing_snapshot is None
        ):
            watermark.mark_entry_decision(candle_open, recovered_gap=recovered_gap)
        Trade.session.commit()
        return event_created

    def _pm_record_entry_block_for_pairs(self, pairs: list[str], reason: str) -> None:
        if not self._pm_ledger_enabled():
            return
        open_pairs = {trade.pair for trade in Trade.get_open_trades()}
        for pair in dict.fromkeys(pairs):
            if pair not in open_pairs:
                self._pm_record_signal_decision(pair, "blocked", reason=reason)

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
                    self._pm_record_actual_order(exchange_order, sl.order_id, trade.pair)
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

    @pm_order_locked
    def _pm_recover_pending_intents(
        self, *, allow_exposure_increasing_relay: bool = True, preserve_operator_stop: bool = False
    ) -> dict[str, Any]:
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
                report["outbox"] = self.exchange.pm_drain_outbox(
                    allow_exposure_increasing=allow_exposure_increasing_relay
                )
            except Exception as e:
                logger.warning(f"PM outbox relay failed at startup: {e}")
                report["outbox"] = {"errors": [f"{e.__class__.__name__}: {e}"]}
        try:
            intents = self.exchange.list_pm_pending_intents()
        except Exception as e:
            # Fail-closed: an unreadable intent store must never be treated as empty.
            logger.warning(f"Could not read PM pending order intents: {e}")
            report["store_error"] = f"{e.__class__.__name__}: {e}"
            if not (preserve_operator_stop and self.state == State.STOPPED):
                self.set_state(State.PAUSED)
            self._pm_block_orders("intent_store_unavailable")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM FAIL-CLOSED: the order intent store could not be read at "
                        f"startup ({e}). Bot {self.state.name}; new orders BLOCKED."
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
            local = self._pm_find_local_order(exchange_id, client_id, intent["pair"])
            adopted: tuple[Trade, Order] | None = None
            adopted_kind: str | None = None
            if local is None and result.get("order") is not None:
                try:
                    if str(intent.get("kind") or "") == "conditional":
                        adopted = self._pm_adopt_orphan_conditional_order(intent, result["order"])
                        adopted_kind = "conditional" if adopted is not None else None
                    else:
                        adopted = self._pm_adopt_orphan_reduce_only_order(intent, result["order"])
                        adopted_kind = "order" if adopted is not None else None
                except Exception as e:
                    Trade.session.rollback()
                    logger.warning(
                        "PM orphan adoption check failed for %s: %s", client_id, e
                    )
                if adopted is not None:
                    trade, adopted_order = adopted
                    local = {"order_id": str(adopted_order.order_id), "trade_id": trade.id}
            if local is not None:
                try:
                    # For an adopted orphan this commit is atomic with the newly
                    # appended local Order row, restoring the missing ACK->commit
                    # transaction boundary rather than creating a second window.
                    self.exchange.pm_mark_intent_linked(
                        client_id, local["order_id"], local["trade_id"]
                    )
                    report["linked"] += 1
                    logger.info(
                        f"PM pending intent {client_id} LINKED to local order "
                        f"{local['order_id']} (trade {local['trade_id']})."
                    )
                    if adopted is not None:
                        trade, adopted_order = adopted
                        status = str((result.get("order") or {}).get("status") or "").lower()
                        status_stop = str(
                            (result.get("order") or {}).get("status_stop") or ""
                        ).lower()
                        if adopted_kind == "conditional":
                            # Even an OPEN triggered child may contain partial-fill
                            # evidence. Process it through the normal stop lifecycle
                            # without operator notifications during recovery.
                            self.update_trade_state(
                                trade,
                                adopted_order.order_id,
                                action_order=result["order"],
                                stoploss_order=True,
                                send_msg=False,
                            )
                            if status_stop == "triggered" and trade.is_open:
                                self._pm_block_orders("stop_triggered_exit_pending")
                        elif status in constants.NON_OPEN_EXCHANGE_STATES or status == "filled":
                            self.update_trade_state(
                                trade,
                                adopted_order.order_id,
                                action_order=result["order"],
                                send_msg=False,
                            )
                except Exception as e:
                    Trade.session.rollback()
                    logger.warning(f"Could not link/reconcile PM intent {client_id}: {e}")
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
            "foreign_positions": [],
            "foreign_orders": [],
            "foreign_conditional_orders": [],
            "local_open_trades_flat_on_exchange": [],
            "local_open_orders_missing_on_exchange": [],
            "recent_closed_order_mismatches": [],
        }
        try:
            allow_foreign = self._pm_allow_foreign_positions()
            bot_owned_pairs = self._pm_active_bot_owned_pairs() if allow_foreign else set()
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
                    record = {"pair": pair, "side": position.get("side"), "contracts": contracts}
                    canonical = self._pm_canonical_pair(pair)
                    if allow_foreign and canonical not in bot_owned_pairs:
                        result["foreign_positions"].append(record)
                    else:
                        result["unknown_positions"].append(record)

            open_trades = Trade.get_open_trades()
            known_order_ids = {
                (self._pm_canonical_pair(trade.pair), str(order.order_id))
                for trade in open_trades
                for order in trade.open_orders
            }
            known_strategy_ids = {
                (self._pm_canonical_pair(trade.pair), str(order.order_id))
                for trade in open_trades
                for order in trade.open_sl_orders
            }
            open_orders = self.exchange.fetch_open_orders()
            for order in open_orders:
                order_id = str(order.get("id") or "")
                symbol = self._pm_canonical_pair(order.get("symbol"))
                if not order_id:
                    continue
                result["exchange_open_orders"].append(
                    {"order_id": order_id, "symbol": symbol}
                )
                if (symbol, order_id) not in known_order_ids:
                    record = {"order_id": order_id, "symbol": symbol}
                    if allow_foreign and not self._pm_exchange_order_is_bot_owned(order):
                        result["foreign_orders"].append(record)
                    else:
                        result["unknown_orders"].append(record)

            # Conditional (stoploss) open orders live on a separate PAPI endpoint and
            # are identified by strategy ids - never mix them with normal order ids.
            conditional_orders = self.exchange.fetch_open_conditional_orders()
            for order in conditional_orders:
                strategy_id = str(order.get("id") or "")
                symbol = self._pm_canonical_pair(order.get("symbol"))
                if not strategy_id:
                    continue
                result["exchange_conditional_orders"].append(
                    {"order_id": strategy_id, "symbol": symbol}
                )
                if (symbol, strategy_id) not in known_strategy_ids:
                    record = {"order_id": strategy_id, "symbol": symbol}
                    if allow_foreign and not self._pm_exchange_order_is_bot_owned(order):
                        result["foreign_conditional_orders"].append(record)
                    else:
                        result["unknown_conditional_orders"].append(record)

            # --- Reverse direction: local records the exchange does NOT confirm ---
            # (one-way mode, so position matching is side-agnostic)
            exchange_position_pairs = {
                self._pm_canonical_pair(pos["pair"])
                for pos in result["exchange_positions"]
                if float(pos.get("contracts") or 0) != 0
            }
            exchange_order_ids = {
                (o["symbol"], o["order_id"]) for o in result["exchange_open_orders"]
            }
            exchange_conditional_ids = {
                (o["symbol"], o["order_id"]) for o in result["exchange_conditional_orders"]
            }
            for trade in open_trades:
                if not trade.is_open:
                    continue
                pair = self._pm_canonical_pair(trade.pair)
                local_open_ids = {(pair, str(o.order_id)) for o in trade.open_orders}
                local_sl_ids = {(pair, str(o.order_id)) for o in trade.open_sl_orders}
                if pair not in exchange_position_pairs:
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
                        {"trade_id": trade.id, "pair": pair,
                         "order_ids": [order_id for _, order_id in missing_ids]}
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
                pair = self._pm_canonical_pair(local_order.ft_pair)
                if (pair, order_id) in exchange_order_ids:
                    continue  # still open on the exchange - covered above
                try:
                    ex_order = (
                        self.exchange.fetch_stoploss_order(order_id, pair)
                        if local_order.ft_order_side == "stoploss"
                        else self.exchange.fetch_order(order_id, pair)
                    )
                except InvalidOrderException:
                    result["recent_closed_order_mismatches"].append(
                        {
                            "order_id": order_id,
                            "pair": pair,
                            "local_status": local_order.status,
                            "exchange_status": "absent",
                        }
                    )
                    continue
                except Exception as e:
                    raise OperationalException(
                        f"PM recent-order verification unavailable for {pair}/{order_id}: {e}"
                    ) from e
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
                            "pair": pair,
                            "local_status": local_order.status,
                            "exchange_status": ex_status,
                        }
                    )
        except Exception as e:
            logger.warning(f"PM startup consistency check failed: {e}")
            result["status"] = "error"
            result["error"] = f"{e.__class__.__name__}: {e}"
            return result

        if self._pm_allow_foreign_positions():
            self._pm_init_user_stream_state()
            self._pm_foreign_position_pairs = {
                self._pm_canonical_pair(item["pair"])
                for item in result["foreign_positions"]
            }
            self._pm_foreign_order_pairs = {
                self._pm_canonical_pair(item["symbol"])
                for item in (
                    result["foreign_orders"] + result["foreign_conditional_orders"]
                )
            }
            if (
                result["foreign_positions"]
                or result["foreign_orders"]
                or result["foreign_conditional_orders"]
            ):
                logger.info(
                    "PM ownership coexistence: observing %d FOREIGN position(s), %d normal "
                    "order(s), %d conditional order(s); read-only and pair-local only.",
                    len(result["foreign_positions"]),
                    len(result["foreign_orders"]),
                    len(result["foreign_conditional_orders"]),
                )

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

    def _pm_apply_startup_consistency(
        self,
        result: dict[str, Any],
        *,
        preserve_operator_stop: bool = False,
        allow_exchange_mutations: bool = True,
    ) -> None:
        """Apply startup consistency without violating an operator STOPPED boundary."""

        def fail_closed_pause() -> str:
            if preserve_operator_stop and self.state == State.STOPPED:
                return "STOPPED"
            self.set_state(State.PAUSED)
            return "PAUSED"
        if result["status"] == "error":
            # The consistency check itself failed - never start trading blind.
            fail_closed_state = fail_closed_pause()
            self._pm_block_orders("startup_consistency_error")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM FAIL-CLOSED: startup consistency check could not complete "
                        f"({result.get('error')}). Bot {fail_closed_state}; new orders BLOCKED."
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

        if mode == "cancel" and not allow_exchange_mutations:
            self._pm_block_orders("startup_consistency_mismatch")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM STOPPED RECOVERY: startup_consistency_mode=cancel is "
                        "suppressed while operator STOPPED. No unmatched exchange "
                        "orders are canceled; mismatch remains fail-closed."
                    ),
                }
            )
            mode = "pause"

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
                fail_closed_state = fail_closed_pause()
                self._pm_block_orders("startup_consistency_mismatch")
                self.rpc.send_msg(
                    {
                        "type": RPCMessageType.WARNING,
                        "status": (
                            "PM FAIL-CLOSED (startup cancel mode): "
                            f"{failed} cancel failure(s); second consistency check "
                            f"status={verify['status']}. Bot {fail_closed_state}; new orders BLOCKED "
                            "until unknown orders are gone on the exchange."
                        ),
                    }
                )

        if mode in {"pause", "cancel"} and unknown_positions:
            # Unmatched exchange positions cannot be auto-resolved safely - pause.
            fail_closed_state = fail_closed_pause()
            self._pm_block_orders("startup_consistency_mismatch")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM FAIL-CLOSED: exchange positions exist without local trade "
                        f"records. Bot {fail_closed_state}; new orders BLOCKED. Manual reconciliation "
                        "required before resuming."
                    ),
                }
            )
        elif mode == "pause" and (unknown_orders or unknown_conditional_orders):
            fail_closed_state = fail_closed_pause()
            self._pm_block_orders("startup_consistency_mismatch")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM FAIL-CLOSED: exchange open orders (normal or conditional) "
                        f"exist without local order records. Bot {fail_closed_state}; new orders "
                        "BLOCKED. Manual reconciliation required before resuming."
                    ),
                }
            )

        if mode == "pause" and (local_flat_trades or local_missing_orders):
            # Reverse direction: the local database claims state the exchange does
            # not confirm. Never auto-mutate - pause and require manual review.
            fail_closed_state = fail_closed_pause()
            self._pm_block_orders("startup_consistency_mismatch")
            self.rpc.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        "PM FAIL-CLOSED: local database records that the exchange does "
                        "NOT confirm (open trade flat on the exchange / open order "
                        f"missing on the exchange). Bot {fail_closed_state}; new orders BLOCKED. "
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

    def set_state(self, state: State) -> None:
        """
        Change the bot state and persist it SYNCHRONOUSLY before anything else
        (notifications, worker loops) can observe the transition.

        Persisting at the assignment site means a crash in the window between
        a pause/stop decision and the next worker iteration can never lose
        that decision.  When the write FAILS the state change still takes
        effect in memory, but the failure is logged CRITICAL and pushed to
        the operator: a restart could resurrect the previous (stale) state.
        """
        scope = getattr(self, "_pm_operator_state_scope", None)
        if (
            scope is not None
            and getattr(scope, "preserve_stopped", False)
            and getattr(self, "state", None) == State.STOPPED
            and state != State.STOPPED
        ):
            logger.warning(
                "PM STOPPED recovery suppressed internal state transition STOPPED -> %s; "
                "only an operator command may leave STOPPED during this scope.",
                state.name,
            )
            return
        self.state = state
        if not hasattr(self, "config"):
            return
        if persist_state(self.config, state) is False:
            self._state_persist_failed = True
            invalidated = invalidate_state_file(self.config)
            logger.critical(
                f"CRITICAL: could not persist bot state {state.name} to disk. "
                + (
                    "The previous state record was invalidated: a restart will "
                    "start PAUSED (no auto-trading)."
                    if invalidated
                    else "The previous state record could NOT be invalidated: "
                    "a restart may restore the previous state."
                )
            )
            rpc = getattr(self, "rpc", None)
            if rpc is not None:
                try:
                    rpc.send_msg(
                        {
                            "type": RPCMessageType.WARNING,
                            "status": (
                                f"CRITICAL: state {state.name} could not be persisted "
                                "to disk. The change is active in memory only; "
                                + (
                                    "the previous state record was invalidated, so a "
                                    "restart will start PAUSED."
                                    if invalidated
                                    else "the previous state record could not be "
                                    "invalidated - a restart may restore it."
                                )
                            ),
                        }
                    )
                except Exception:  # alerting must never break the state change
                    logger.exception("Could not send state-persistence failure alert")
        else:
            self._state_persist_failed = False

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

        self._pm_cold_start_reconcile()

        if (
            self.trading_mode == TradingMode.FUTURES
            and not self.config["dry_run"]
            and getattr(self, "_pm_listen_key", None) is not None
            and not self._pm_listen_key
        ):
            self._pm_create_listen_key()

    def _pm_cold_start_reconcile(self, *, stopped_mode: bool = False) -> None:
        """Run PM crash recovery independently of the trading RUNNING state.

        Persisted STOPPED is a legitimate operator state and must never prevent
        exchange/local bookkeeping from converging after a crash.  This routine
        performs only PM reconciliation/protection maintenance; it does not arm
        entries or transition the bot to RUNNING.
        """
        if not (
            self.trading_mode == TradingMode.FUTURES
            and not self.config["dry_run"]
            and getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
        ):
            return
        if not hasattr(self, "_pm_operator_state_scope"):
            self._pm_operator_state_scope = local()
        previous_guard = getattr(self._pm_operator_state_scope, "preserve_stopped", False)
        if stopped_mode and self.state == State.STOPPED:
            self._pm_operator_state_scope.preserve_stopped = True
        try:
            # 1) Resolve durable intents first (including the narrow, provable
            #    reduce-only ACK->commit orphan adoption path).
            self._pm_recover_pending_intents(
                allow_exposure_increasing_relay=not stopped_mode,
                preserve_operator_stop=stopped_mode,
            )
            # 2) Rebuild stoploss child-id ownership before any replayed stream event.
            self._pm_rebuild_actual_order_map()
            # 3) Verify full-account consistency before any future /start can expose
            #    capital.  Failures remain latched fail-closed.
            consistency = self._pm_startup_consistency_check()
            self._pm_apply_startup_consistency(
                consistency,
                preserve_operator_stop=stopped_mode,
                allow_exchange_mutations=not stopped_mode,
            )
            # 4) Full idempotent order/position/protection reconcile.
            self._pm_order_recovery()
        finally:
            self._pm_operator_state_scope.preserve_stopped = previous_guard

    def process(self) -> None:
        """
        Queries the persistence layer for open trades and handles them,
        otherwise a new trade is created.
        :return: True if one or more trades has been created or closed, False otherwise
        """

        # Protective/recovery control-plane work runs BEFORE potentially slow
        # market refresh and factor analysis.  This keeps a scheduled stop repair,
        # user-stream reconciliation or account-risk action from sitting behind a
        # slow pairlist/REST request.
        self._pm_cycle_exposure_block_reason = None
        self._pm_consume_user_stream_events()
        self._pm_maintenance_schedule.run_pending()
        self._schedule.run_pending()

        market_cycle_started = _time.monotonic()
        self.exchange.reload_markets()

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

        market_cycle_elapsed = _time.monotonic() - market_cycle_started
        market_budget_s = float(
            self.config.get("exchange", {})
            .get("portfolio_margin_risk", {})
            .get("market_analysis_budget_seconds", 0)
            or 0
        )
        if market_budget_s > 0 and market_cycle_elapsed > market_budget_s:
            self._pm_cycle_exposure_block_reason = (
                f"market_analysis_budget_exceeded: {market_cycle_elapsed:.2f}s>"
                f"{market_budget_s:.2f}s"
            )
            self.log_once(
                "PM exposure increase blocked for this cycle: "
                + self._pm_cycle_exposure_block_reason,
                logger.warning,
            )

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

        # Then looking for entry opportunities.  A full-slot condition is
        # itself an auditable per-pair decision for the current candle.
        if self.state == State.RUNNING:
            if self.get_free_open_trades():
                self.enter_positions()
            else:
                self._pm_record_entry_block_for_pairs(
                    self.active_pair_whitelist, "max_open_trades_reached"
                )
        Trade.commit()
        self.rpc.process_msg_queue(self.dataprovider._msg_queue)
        self.last_process = datetime.now(UTC)

    def process_stopped(self) -> None:
        """
        Keep STOPPED non-trading while still allowing one cold PM reconciliation.
        """
        if not getattr(self, "_pm_stopped_cold_reconcile_done", False):
            # Set before the call so an exchange outage cannot create a tight
            # retry/spam loop every worker throttle.  Scheduled/manual recovery
            # remains available afterwards, and a later /start runs startup()
            # with another full reconcile before entries can be considered.
            self._pm_stopped_cold_reconcile_done = True
            try:
                self._pm_cold_start_reconcile(stopped_mode=True)
            except Exception as e:
                self._pm_block_orders("reconciliation_incomplete")
                logger.exception(
                    "PM cold reconciliation while STOPPED failed; bot remains STOPPED "
                    "and new exposure stays blocked: %s",
                    e,
                )
        if (
            self.trading_mode == TradingMode.FUTURES
            and not self.config.get("dry_run", True)
            and getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
        ):
            # Drain/process stream ownership while STOPPED so the bounded queue
            # cannot fill with stale events.  Keep account-level risk actions
            # disabled here: STOPPED must not force-close manual/external assets.
            self._pm_consume_user_stream_events(allow_account_risk_actions=False)
            self._pm_maintenance_schedule.run_pending()
            Trade.commit()
            self.last_process = datetime.now(UTC)
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
                    if getattr(self.exchange, "_is_portfolio_margin", lambda: False)():
                        self._pm_resize_stop_protection(trade)
                    else:
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
            reason = "; ".join(self._pm_blocked_order_reasons())
            self.log_once(
                "Not creating new trades. PM orders blocked: " + reason,
                logger.info,
            )
            self._pm_record_entry_block_for_pairs(self.active_pair_whitelist, reason)
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
            self._pm_record_entry_block_for_pairs(whitelist, "global_pairlock")
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
            self._pm_record_signal_decision(
                pair, decision="blocked", reason="max_open_trades_reached"
            )
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
                    self._pm_record_signal_decision(
                        pair, decision="blocked", reason="depth_of_market_rejected"
                    )
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
        decision_scope = (
            "entry" if mode == "initial" else "dca" if mode == "pos_adjust" else "replace"
        )
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
            self._pm_record_signal_decision(
                pair,
                decision="blocked",
                reason="; ".join(self._pm_blocked_order_reasons()),
                decision_scope=decision_scope,
            )
            return False

        pair_block_reasons = self._pm_pair_entry_block_reasons(pair)
        if pair_block_reasons:
            self.log_once(
                f"Refusing exposure increase on {pair}. PM pair ownership blocked: "
                + ", ".join(pair_block_reasons),
                logger.warning,
            )
            self._pm_record_signal_decision(
                pair,
                decision="blocked",
                reason="; ".join(pair_block_reasons),
                decision_scope=decision_scope,
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
            self._pm_record_signal_decision(
                pair, decision="blocked", reason="stake_unavailable", decision_scope=decision_scope
            )
            return False

        data_block_reason = self._pm_exposure_data_block_reason(pair)
        if data_block_reason:
            self._pm_record_signal_decision(
                pair,
                decision="blocked_data",
                reason=data_block_reason,
                decision_scope=decision_scope,
            )
            logger.warning(
                f"Refusing PM exposure increase for {pair} ({mode}): data not usable "
                f"({data_block_reason}). Reduction/protection paths remain enabled."
            )
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
            self._pm_record_signal_decision(
                pair,
                decision="blocked",
                reason="confirm_trade_entry_denied",
                decision_scope=decision_scope,
            )
            return False

        if trade and self.handle_similar_open_order(trade, enter_limit_requested, amount, side):
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
            **self._pm_origin_trade_kwargs(trade),
        )
        order_obj = Order.parse_from_ccxt_object(order, pair, side, amount, enter_limit_requested)
        order_obj.ft_order_tag = enter_tag
        order_id = order["id"]
        order_status = order.get("status")
        logger.info(f"Order {order_id} was created for {pair} and status is {order_status}.")

        # Append the submitted lifecycle event under the stable decision id.
        self._pm_record_signal_decision(
            pair,
            decision="entry_submitted",
            reason=f"order_id={order_id}",
            order_client_id=str(order.get("clientOrderId") or ""),
            decision_scope=decision_scope,
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
                logger.info(
                    f"DCA order closed; full fill lifecycle will resize protection safely: {trade}"
                )
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
        # PM cancellation is evidence-driven.  A DELETE/not-found response is
        # never itself terminal proof; resolve the full conditional lifecycle
        # (including a triggered real child) before changing local state.
        if getattr(self.exchange, "_is_portfolio_margin", lambda: False)():
            self._pm_retire_stop_ids(
                trade, [str(oslo.order_id) for oslo in trade.open_sl_orders]
            )
            return trade

        # Non-PM legacy behavior.
        for oslo in trade.open_sl_orders:
            try:
                logger.info(f"Cancelling stoploss on exchange for {trade} order: {oslo.order_id}")
                co = self.exchange.cancel_stoploss_order_with_result(
                    oslo.order_id, trade.pair, trade.amount
                )
                self.update_trade_state(trade, oslo.order_id, co, stoploss_order=True)
            except InvalidOrderException:
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
                **self._pm_origin_trade_kwargs(trade),
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

        except StopWouldImmediatelyTrigger as e:
            # The exchange rejected the new conditional, not the protective exit.
            # Retain existing stops until the regular exit lifecycle confirms fills.
            # Do not retry with a looser trigger or claim a stop order has FILLED.
            logger.warning(
                "PM stop trigger crossed for Trade #%s %s; requesting an idempotent "
                "protective emergency exit, retaining old protection until confirmed "
                "fills. %s", trade.id, trade.pair, e,
            )
            self.emergency_exit(trade, stop_price)

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

            pm_stop_verdict: str | None = None
            if stoploss_order:
                if (
                    getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
                    and not self._pm_stop_identity_matches(trade, str(slo.order_id), stoploss_order)
                ):
                    self._pm_protection_hold(trade, f"stop {slo.order_id} identity unverified")
                    return False
                stoploss_orders.append(stoploss_order)
                if getattr(self.exchange, "_is_portfolio_margin", lambda: False)():
                    pm_stop_verdict, _ = self._pm_validate_protection_order(
                        trade, str(slo.order_id), stoploss_order
                    )
                self.update_trade_state(trade, slo.order_id, stoploss_order, stoploss_order=True)

            if stoploss_order and pm_stop_verdict == "triggered_pending":
                if trade.is_open and trade.has_open_position:
                    self._pm_triggered_exit_hold(
                        trade, f"stop {slo.order_id} actual child is still working/partial"
                    )
                continue
            if stoploss_order and pm_stop_verdict == "triggered_failed":
                if trade.is_open and trade.has_open_position:
                    self._pm_protection_hold(
                        trade, f"triggered child for {slo.order_id} ended with residual exposure"
                    )
                    self.emergency_exit(trade, trade.stoploss_or_liquidation)
                return False
            if stoploss_order and pm_stop_verdict == "terminal":
                if not trade.is_open or not trade.has_open_position:
                    trade.exit_reason = ExitType.STOPLOSS_ON_EXCHANGE.value
                    self._notify_exit(trade, "stoploss", True)
                    self.handle_protections(trade.pair, trade.trade_direction)
                    self._pm_unblock_orders("stop_triggered_exit_pending")
                    return True
                self._pm_protection_hold(
                    trade, f"terminal stop {slo.order_id} left residual exposure"
                )
                self.emergency_exit(trade, trade.stoploss_or_liquidation)
                return False

            if (
                stoploss_order
                and pm_stop_verdict is None
                and stoploss_order["status"] in ("closed", "triggered")
            ):
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

            if getattr(self.exchange, "_is_portfolio_margin", lambda: False)():
                # A persisted conditional crash-window incident owns this
                # protection decision. Never bypass it with a fresh clientAlgoId.
                if self._pm_handle_uncertain_stop_dispatch(trade):
                    return False
                # Unified safe creation: create -> persist candidate -> fetch
                # exchange truth -> validate (no old conditional to retire).
                self._pm_replace_stop_protection(trade, [], new_stop_price=stop_price)
                return False

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
            if getattr(self.exchange, "_is_portfolio_margin", lambda: False)():
                # Unified safe recreation (the canceled conditionals need no
                # retirement - they are already gone on the exchange).
                self._pm_replace_stop_protection(
                    trade, [], new_stop_price=trade.stoploss_or_liquidation
                )
                return False
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
                if getattr(self.exchange, "_is_portfolio_margin", lambda: False)():
                    # PM safety switch: create + verify the replacement BEFORE
                    # retiring the old conditional, so a crash between the two
                    # steps can never leave a non-zero position unprotected.
                    self._pm_switch_trailing_stoploss(trade, order, stoploss_norm)
                    return
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
        # PM exit idempotency across timeout/ACK->commit windows.  If a
        # reduce-only normal exit for this instrument is already unresolved,
        # resolve THAT durable client id first instead of creating another one.
        # This prevents the 15-second retry storm observed in production after
        # an exchange-filled exit was missing its local Order commit.
        pending_exit = self._pm_pending_reduce_only_exit_intent(trade)
        if pending_exit is not None:
            if self._pm_auto_recovery_due():
                try:
                    self._pm_recover_pending_intents()
                except Exception as exc:
                    logger.warning(
                        "PM targeted pending-exit recovery failed for Trade #%s %s: %s",
                        trade.id,
                        trade.pair,
                        exc,
                    )
                pending_exit = self._pm_pending_reduce_only_exit_intent(trade)
            if not trade.is_open:
                # Recovery may have adopted a terminal orphan and closed the Trade.
                return False
            if pending_exit is not None:
                self.log_once(
                    "Suppressing duplicate PM reduce-only exit for "
                    f"Trade #{trade.id} {trade.pair}: unresolved intent "
                    f"{pending_exit.client_id} state={pending_exit.state}; "
                    "waiting for same-client-id reconciliation.",
                    logger.warning,
                )
                return False

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

        # PM reduce-only exits may safely coexist with conditional protection.
        # Keep the stop until the exit is terminal; cancel-after-fill is handled
        # by the normal trade lifecycle.  Non-PM retains the legacy behavior.
        if not getattr(self.exchange, "_is_portfolio_margin", lambda: False)():
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
                **self._pm_origin_trade_kwargs(trade),
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

        if (
            getattr(self.exchange, "_is_portfolio_margin", lambda: False)()
            and order_obj.ft_order_side == trade.entry_side
            and order_obj.ft_is_open
            and order_obj.safe_filled > 0
        ):
            self._pm_apply_partial_entry_fill(trade, order_obj)

        trade = self._update_trade_after_fill(trade, order_obj, send_msg)
        Trade.commit()

        self.order_close_notify(trade, order_obj, stoploss_order, send_msg)

        return False

    def _pm_apply_partial_entry_fill(self, trade: Trade, order: Order) -> None:
        """Book an OPEN partial entry fill and ensure stop quantity covers it.

        Core Freqtrade intentionally excludes open orders from trade recalculation.
        For PM this would leave a partially-filled DCA as real exchange exposure
        that the local Trade (and therefore its stop quantity) does not yet see.
        Temporarily include this one order in recalculation while preserving its
        OPEN lifecycle, then replace protection only when local coverage is short.
        """
        if (
            not order.ft_is_open
            or order.ft_order_side != trade.entry_side
            or order.safe_filled <= 0
        ):
            return
        was_open = order.ft_is_open
        order.ft_is_open = False
        try:
            trade.recalc_trade_from_orders()
        finally:
            order.ft_is_open = was_open
        Trade.commit()

        tolerance = max(abs(float(trade.amount)) * 1e-6, 1e-9)
        locally_covered = any(
            float(stop.safe_amount) + tolerance >= float(trade.amount)
            for stop in trade.open_sl_orders
        )
        if not locally_covered:
            logger.warning(
                "PM partial entry fill changed exposure for %s to %.8f; "
                "resizing stop protection before waiting for order terminal state.",
                trade.pair,
                trade.amount,
            )
            self._pm_resize_stop_protection(trade)

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
                    if getattr(self.exchange, "_is_portfolio_margin", lambda: False)():
                        # PM safety: resize the protection instead of
                        # cancel-then-recreate. The OLD conditional stays
                        # active until a verified replacement exists.
                        self._pm_resize_stop_protection(trade)
                    else:
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
