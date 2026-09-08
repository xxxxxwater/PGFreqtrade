# pragma pylint: disable=unused-argument, unused-variable, protected-access, invalid-name

"""
This module manage Telegram communication
"""

import asyncio
import hashlib
import json
import logging
import re
import uuid
from collections import deque
from collections.abc import Callable, Coroutine
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import partial, wraps
from html import escape
from itertools import chain
from math import isnan
from threading import Thread
from typing import Any, Literal

from tabulate import tabulate
from telegram import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import MessageLimit, ParseMode
from telegram.error import BadRequest, NetworkError, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    TypeHandler,
)
from telegram.helpers import escape_markdown

from freqtrade.__init__ import __version__
from freqtrade.constants import DUST_PER_COIN, Config
from freqtrade.enums import MarketDirection, RPCMessageType, SignalDirection, TradingMode
from freqtrade.exceptions import OperationalException
from freqtrade.misc import chunks, plural
from freqtrade.persistence import PMNotificationOutbox, PMOrderIntent, PMStreamJournal, Trade
from freqtrade.rpc import RPC, RPCException, RPCHandler
from freqtrade.rpc.rpc import set_audit_op_id
from freqtrade.rpc.rpc_types import RPCEntryMsg, RPCExitMsg, RPCOrderMsg, RPCSendMsg
from freqtrade.util import (
    dt_from_ts,
    dt_humanize_delta,
    fmt_coin,
    fmt_coin2,
    format_date,
    format_pct,
    round_value,
)


MAX_MESSAGE_LENGTH = MessageLimit.MAX_TEXT_LENGTH


logger = logging.getLogger(__name__)

logger.debug("Included module rpc.telegram ...")


def safe_async_db(func: Callable[..., Any]):
    """
    Decorator to safely handle sessions when switching async context
    :param func: function to decorate
    :return: decorated function
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """Decorator logic"""
        try:
            return func(*args, **kwargs)
        finally:
            Trade.session.remove()

    return wrapper


@dataclass
class TimeunitMappings:
    header: str
    message: str
    message2: str
    callback: str
    default: int
    dateformat: str


def authorized_only(command_handler: Callable[..., Coroutine[Any, Any, None]]):
    """
    Decorator to check if the message comes from the correct chat_id
    can only be used with Telegram Class to decorate instance methods.
    :param command_handler: Telegram CommandHandler
    :return: decorated function
    """

    @wraps(command_handler)
    async def wrapper(self, *args, **kwargs) -> None:
        """Decorator logic"""
        update = kwargs.get("update") or args[0]

        # Reject unauthorized messages
        message: Message = (
            update.message if update.callback_query is None else update.callback_query.message
        )
        cchat_id: int = int(message.chat_id)
        ctopic_id: int | None = message.message_thread_id
        from_user_id: str = str(update.effective_user.id if update.effective_user else "")

        chat_id = int(self._config["telegram"]["chat_id"])
        if cchat_id != chat_id:
            logger.info(f"Rejected unauthorized message from: {cchat_id}")
            return None
        if (topic_id := self._config["telegram"].get("topic_id")) is not None:
            if str(ctopic_id) != topic_id:
                # This can be quite common in multi-topic environments.
                logger.debug(f"Rejected message from wrong channel: {cchat_id}, {ctopic_id}")
                return None

        authorized = self._config["telegram"].get("authorized_users", None)
        if authorized is not None and from_user_id not in authorized:
            logger.info(f"Unauthorized user tried to control the bot: {from_user_id}")
            return None
        # Rollback session to avoid getting data stored in a transaction.
        Trade.rollback()
        # Propagate the audit op id (stashed by the inbound audit logger) into
        # the rpc execution context, so the execution outcome is logged with
        # the SAME op id as the inbound command line.
        try:
            context = kwargs.get("context")
            if context is None and len(args) > 1:
                context = args[1]
            op_id = getattr(context, "user_data", {}).get("audit_op_id")
            set_audit_op_id(op_id)
        except Exception:
            set_audit_op_id(None)
        logger.debug("Executing handler: %s for chat_id: %s", command_handler.__name__, chat_id)
        try:
            return await command_handler(self, *args, **kwargs)
        except RPCException as e:
            await self._send_msg(str(e))
        except BaseException:
            logger.exception("Exception occurred within Telegram module")
        finally:
            Trade.session.remove()
            set_audit_op_id(None)

    return wrapper


class Telegram(RPCHandler):
    """This class handles all telegram communication"""

    def __init__(self, rpc: RPC, config: Config) -> None:
        """
        Init the Telegram call, and init the super class RPCHandler
        :param rpc: instance of RPC Helper class
        :param config: Configuration object
        :return: None
        """
        super().__init__(rpc, config)

        self._app: Application
        self._loop: asyncio.AbstractEventLoop
        # Health/status tracking exposed via RPCManager.health() and API endpoints.
        self._init_failed = False
        self._send_failures = 0
        self._last_send_error: str | None = None
        self._last_send_retry_after: float | None = None
        self._last_sent_at: str | None = None
        # Database-outage fallback is intentionally bounded and in-memory only.
        # Persistent incidents are reconstructed from business records on restart.
        self._critical_fallback_queue: deque[dict[str, Any]] = deque(maxlen=100)
        self._critical_wakeup: asyncio.Event | None = None
        self._init_keyboard()
        self._start_thread()

    def _start_thread(self):
        """
        Creates and starts the polling thread
        """
        self._thread = Thread(target=self._init, name="FTTelegram")
        self._thread.start()

    def _init_keyboard(self) -> None:
        """
        Validates the keyboard configuration from telegram config
        section.
        """
        self._keyboard: list[list[str | KeyboardButton]] = [
            ["/daily", "/profit", "/balance"],
            ["/status", "/status table", "/pm_status"],
            ["/count", "/start", "/stop", "/help"],
        ]
        # do not allow commands with mandatory arguments and critical cmds
        # TODO: DRY! - its not good to list all valid cmds here. But otherwise
        #       this needs refactoring of the whole telegram module (same
        #       problem in _help()).
        valid_keys: list[str] = [
            r"/start$",
            r"/pause$",
            r"/stop$",
            r"/status$",
            r"/status table$",
            r"/trades$",
            r"/performance$",
            r"/buys",
            r"/entries",
            r"/sells",
            r"/exits",
            r"/mix_tags",
            r"/daily$",
            r"/daily \d+$",
            r"/profit([_ ]long|[_ ]short)?$",
            r"/profit([_ ]long|[_ ]short)? \d+$",
            r"/stats$",
            r"/count$",
            r"/locks$",
            r"/balance$",
            r"/stopbuy$",
            r"/stopentry$",
            r"/reload_config$",
            r"/show_config$",
            r"/logs$",
            r"/whitelist$",
            r"/whitelist(\ssorted|\sbaseonly)+$",
            r"/blacklist$",
            r"/bl_delete$",
            r"/weekly$",
            r"/weekly \d+$",
            r"/monthly$",
            r"/monthly \d+$",
            r"/forcebuy$",
            r"/forcelong$",
            r"/forceshort$",
            r"/forcesell$",
            r"/forceexit$",
            r"/pm_close$",
            r"/pm_close (all|USDT|USDC|usdt|usdc)$",
            r"/pm_close (all|USDT|USDC|usdt|usdc)? ?CONFIRM$",
            r"/pm_status$",
            r"/pm_risk$",
            r"/pm_recover$",
            r"/health$",
            r"/help$",
            r"/version$",
            r"/marketdir (long|short|even|none)$",
            r"/marketdir$",
        ]
        # Create keys for generation
        valid_keys_print = [k.replace("$", "") for k in valid_keys]

        # custom keyboard specified in config.json
        cust_keyboard = self._config["telegram"].get("keyboard", [])
        if cust_keyboard:
            combined = "(" + ")|(".join(valid_keys) + ")"
            # check for valid shortcuts
            invalid_keys = [
                b for b in chain.from_iterable(cust_keyboard) if not re.match(combined, b)
            ]
            if len(invalid_keys):
                err_msg = (
                    "config.telegram.keyboard: Invalid commands for "
                    f"custom Telegram keyboard: {invalid_keys}"
                    f"\nvalid commands are: {valid_keys_print}"
                )
                raise OperationalException(err_msg)
            else:
                self._keyboard = cust_keyboard
                logger.info(f"using custom keyboard from config.json: {self._keyboard}")

    def _init_telegram_app(self):
        return Application.builder().token(self._config["telegram"]["token"]).build()

    def _init(self) -> None:
        """
        Initializes this module with the given config,
        registers all known command handlers
        and starts polling for message updates
        Runs in a separate thread.
        """
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        self._app = self._init_telegram_app()

        # Audit log EVERY inbound interaction (commands, plain messages and
        # inline-button callbacks) before any handler runs.  Production
        # requirement: trading-class commands (/fx, /pm_close, ...) must be
        # traceable to a user and timestamp in the bot log.  Runs in group -1,
        # i.e. BEFORE the real command handlers.
        self._app.add_handler(TypeHandler(Update, self._log_inbound_update), group=-1)

        # Register command handler and start telegram message polling
        handles = [
            CommandHandler("status", self._status),
            CommandHandler("profit", self._profit),
            CommandHandler("balance", self._balance),
            CommandHandler("start", self._start),
            CommandHandler("stop", self._stop),
            CommandHandler(["forcesell", "forceexit", "fx"], self._force_exit),
            CommandHandler("pm_close", self._pm_close),
            CommandHandler("pm_status", self._pm_status),
            CommandHandler("pm_risk", self._pm_risk),
            CommandHandler("pm_recover", self._pm_recover),
            CommandHandler(
                ["forcebuy", "forcelong"],
                partial(self._force_enter, order_side=SignalDirection.LONG),
            ),
            CommandHandler(
                "forceshort", partial(self._force_enter, order_side=SignalDirection.SHORT)
            ),
            CommandHandler("reload_trade", self._reload_trade_from_exchange),
            CommandHandler("trades", self._trades),
            CommandHandler("delete", self._delete_trade),
            CommandHandler(["coo", "cancel_open_order"], self._cancel_open_order),
            CommandHandler("performance", self._performance),
            CommandHandler(["buys", "entries"], self._enter_tag_performance),
            CommandHandler(["sells", "exits"], self._exit_reason_performance),
            CommandHandler("mix_tags", self._mix_tag_performance),
            CommandHandler("stats", self._stats),
            CommandHandler("daily", self._daily),
            CommandHandler("weekly", self._weekly),
            CommandHandler("monthly", self._monthly),
            CommandHandler("count", self._count),
            CommandHandler("locks", self._locks),
            CommandHandler(["unlock", "delete_locks"], self._delete_locks),
            CommandHandler(["reload_config", "reload_conf"], self._reload_config),
            CommandHandler(["show_config", "show_conf"], self._show_config),
            CommandHandler(["stopbuy", "stopentry", "pause"], self._pause),
            CommandHandler("whitelist", self._whitelist),
            CommandHandler("blacklist", self._blacklist),
            CommandHandler(["blacklist_delete", "bl_delete"], self._blacklist_delete),
            CommandHandler("logs", self._logs),
            CommandHandler("health", self._health),
            CommandHandler("help", self._help),
            CommandHandler("version", self._version),
            CommandHandler("marketdir", self._changemarketdir),
            CommandHandler("order", self._order),
            CommandHandler("list_custom_data", self._list_custom_data),
            CommandHandler("tg_info", self._tg_info),
            CommandHandler("profit_long", self._profit_long),
            CommandHandler("profit_short", self._profit_short),
        ]
        callbacks = [
            CallbackQueryHandler(self._status_table, pattern="update_status_table"),
            CallbackQueryHandler(self._daily, pattern="update_daily"),
            CallbackQueryHandler(self._weekly, pattern="update_weekly"),
            CallbackQueryHandler(self._monthly, pattern="update_monthly"),
            CallbackQueryHandler(self._profit_long, pattern="update_profit_long"),
            CallbackQueryHandler(self._profit_short, pattern="update_profit_short"),
            CallbackQueryHandler(self._profit, pattern=r"update_profit$"),
            CallbackQueryHandler(self._balance, pattern="update_balance"),
            CallbackQueryHandler(self._performance, pattern="update_performance"),
            CallbackQueryHandler(
                self._enter_tag_performance, pattern="update_enter_tag_performance"
            ),
            CallbackQueryHandler(
                self._exit_reason_performance, pattern="update_exit_reason_performance"
            ),
            CallbackQueryHandler(self._mix_tag_performance, pattern="update_mix_tag_performance"),
            CallbackQueryHandler(self._count, pattern="update_count"),
            CallbackQueryHandler(self._force_exit_inline, pattern=r"force_exit__\S+"),
            CallbackQueryHandler(self._force_enter_inline, pattern=r"force_enter__\S+"),
        ]
        for handle in handles:
            self._app.add_handler(handle)

        for callback in callbacks:
            self._app.add_handler(callback)

        logger.info(
            "rpc.telegram is listening for following commands: %s",
            [[x for x in sorted(h.commands)] for h in handles],
        )
        self._loop.run_until_complete(self._startup_telegram())

    async def _log_inbound_update(self, update: Update, context: CallbackContext) -> None:
        """
        Audit-log every inbound Telegram interaction before its handler runs.

        Covers text commands (e.g. /fx, /pm_close ...), plain messages and
        inline-button callbacks (e.g. the force_exit confirmation buttons).
        Every interaction gets a short operation id that is stashed on the
        shared context, so trading handlers can pass it to the rpc layer and
        the execution outcome is logged with the SAME op id.

        Message text is length-limited and single-line (sanitized) so a
        pasted blob can never flood the logfile.  Audit logging must never
        break telegram handling, so any failure degrades to a debug log.
        """
        try:
            op_id = uuid.uuid4().hex[:8]
            context.user_data["audit_op_id"] = op_id
            user = update.effective_user
            user_id = getattr(user, "id", None)
            user_name = getattr(user, "full_name", "") or ""
            user_handle = f"@{user.username}" if getattr(user, "username", None) else ""
            chat_id = update.effective_chat.id if update.effective_chat else None
            identity = f"chat_id={chat_id} user={user_handle or user_name}({user_id})"
            if update.message and update.message.text:
                text = update.message.text.replace("\n", " ")
                text = text[:200] + ("..." if len(update.message.text) > 200 else "")
                entities = getattr(update.message, "entities", None) or []
                kind = (
                    "command"
                    if any(getattr(entity, "type", "") == "bot_command" for entity in entities)
                    else "message"
                )
                logger.info(f"Telegram inbound {kind}: op={op_id} {identity} text={text!r}")
            elif update.callback_query:
                logger.info(
                    f"Telegram inbound callback: op={op_id} {identity} "
                    f"data={update.callback_query.data!r}"
                )
        except Exception as exception:
            logger.debug(f"Telegram inbound audit log failed: {exception}")

    async def _startup_telegram(self) -> None:
        retries = 3
        attempt = 0
        while attempt < retries:
            try:
                await self._app.initialize()
                await self._app.start()
                break
            except Exception as ex:
                logger.error(
                    "Error starting Telegram bot (attempt %d/%d): %s", attempt + 1, retries, ex
                )
                attempt += 1
                if attempt == retries:
                    self._init_failed = True
                    logger.warning("Telegram init failed.")
                    return
                await asyncio.sleep(2)
        # Compensate business-commit -> notification-enqueue crash windows before
        # the delivery worker begins. Failure here never prevents Telegram polling.
        try:
            self._rebuild_critical_notifications_from_business_state()
        except Exception as exc:
            logger.warning("Initial critical-notification rebuild failed: %s", exc)
        retry_task = asyncio.create_task(self._critical_notification_retry_loop())
        try:
            if self._app.updater:
                await self._app.updater.start_polling(
                    bootstrap_retries=10,
                    timeout=20,
                    drop_pending_updates=True,
                )
                while True:
                    await asyncio.sleep(10)
                    if not self._app.updater.running:
                        break
        finally:
            retry_task.cancel()
            await asyncio.gather(retry_task, return_exceptions=True)

    async def _cleanup_telegram(self) -> None:
        if self._app.updater:
            await self._app.updater.stop()
        await self._app.stop()
        await self._app.shutdown()

    def cleanup(self) -> None:
        """
        Stops all running telegram threads.
        :return: None
        """
        # This can take up to `timeout` from the call to `start_polling`.
        asyncio.run_coroutine_threadsafe(self._cleanup_telegram(), self._loop)
        self._thread.join()

    def health(self) -> dict[str, Any]:
        """
        Telegram health/status exposed through RPCManager.health() and the API.
        """
        return {
            "module": self.name,
            "enabled": True,
            "init_failed": self._init_failed,
            "send_failures": self._send_failures,
            "last_send_error": self._last_send_error,
            "last_sent_at": self._last_sent_at,
        }

    def _exchange_from_msg(self, msg: RPCOrderMsg) -> str:
        """
        Extracts the exchange name from the given message.
        :param msg: The message to extract the exchange name from.
        :return: The exchange name.
        """
        return f"{msg['exchange']}{' (dry)' if self._config['dry_run'] else ''}"

    def _add_analyzed_candle(self, pair: str) -> str:
        candle_val = (
            self._config["telegram"].get("notification_settings", {}).get("show_candle", "off")
        )
        if candle_val != "off":
            if candle_val == "ohlc":
                analyzed_df, _ = self._rpc._freqtrade.dataprovider.get_analyzed_dataframe(
                    pair, self._config["timeframe"]
                )
                candle = analyzed_df.iloc[-1].squeeze() if len(analyzed_df) > 0 else None
                if candle is not None:
                    return (
                        f"*Candle OHLC*: `{candle['open']}, {candle['high']}, "
                        f"{candle['low']}, {candle['close']}`\n"
                    )

        return ""

    def _format_entry_msg(self, msg: RPCEntryMsg) -> str:
        is_fill = msg["type"] in [RPCMessageType.ENTRY_FILL]
        emoji = "\N{CHECK MARK}" if is_fill else "\N{LARGE BLUE CIRCLE}"

        terminology = {
            "1_enter": "New Trade",
            "1_entered": "New Trade filled",
            "x_enter": "Increasing position",
            "x_entered": "Position increase filled",
        }

        key = f"{'x' if msg['sub_trade'] else '1'}_{'entered' if is_fill else 'enter'}"
        wording = terminology[key]

        message = (
            f"{emoji} *{self._exchange_from_msg(msg)}:*"
            f" {wording} (#{msg['trade_id']})\n"
            f"*Pair:* `{msg['pair']}`\n"
        )
        message += self._add_analyzed_candle(msg["pair"])
        message += f"*Enter Tag:* `{msg['enter_tag']}`\n" if msg.get("enter_tag") else ""
        message += f"*Amount:* `{round_value(msg['amount'], 8)}`\n"
        message += f"*Direction:* `{msg['direction']}"
        if msg.get("leverage") and msg.get("leverage", 1.0) != 1.0:
            message += f" ({msg['leverage']:.3g}x)"
        message += "`\n"
        message += f"*Open Rate:* `{fmt_coin2(msg['open_rate'], msg['quote_currency'])}`\n"
        if msg["type"] == RPCMessageType.ENTRY and msg["current_rate"]:
            message += (
                f"*Current Rate:* `{fmt_coin2(msg['current_rate'], msg['quote_currency'])}`\n"
            )

        profit_fiat_extra = self.__format_profit_fiat(msg, "stake_amount")  # type: ignore
        total = fmt_coin(msg["stake_amount"], msg["quote_currency"])

        message += f"*{'New ' if msg['sub_trade'] else ''}Total:* `{total}{profit_fiat_extra}`"

        return message

    def _format_exit_msg(self, msg: RPCExitMsg) -> str:
        duration = msg["close_date"].replace(microsecond=0) - msg["open_date"].replace(
            microsecond=0
        )
        duration_min = duration.total_seconds() / 60

        leverage_text = (
            f" ({msg['leverage']:.3g}x)"
            if msg.get("leverage") and msg.get("leverage", 1.0) != 1.0
            else ""
        )

        profit_fiat_extra = self.__format_profit_fiat(msg, "profit_amount")

        profit_extra = (
            f" ({msg['gain']}: {fmt_coin(msg['profit_amount'], msg['quote_currency'])}"
            f"{profit_fiat_extra})"
        )

        is_fill = msg["type"] == RPCMessageType.EXIT_FILL
        is_sub_trade = msg.get("sub_trade")
        is_sub_profit = msg["profit_amount"] != msg.get("cumulative_profit")
        is_final_exit = msg.get("is_final_exit", False) and is_sub_profit
        profit_prefix = "Sub " if is_sub_trade else ""
        cp_extra = ""
        exit_wording = "Exited" if is_fill else "Exiting"
        if is_sub_trade or is_final_exit:
            cp_fiat = self.__format_profit_fiat(msg, "cumulative_profit")

            if is_final_exit:
                profit_prefix = "Sub "
                cp_extra = (
                    f"*Final Profit:* `{format_pct(msg['final_profit_ratio'])} "
                    f"({msg['cumulative_profit']:.8f} {msg['quote_currency']}{cp_fiat})`\n"
                )
            else:
                exit_wording = f"Partially {exit_wording.lower()}"
                if msg["cumulative_profit"]:
                    cp_extra = (
                        f"*Cumulative Profit:* `"
                        f"{fmt_coin(msg['cumulative_profit'], msg['stake_currency'])}{cp_fiat}`\n"
                    )
        enter_tag = f"*Enter Tag:* `{msg['enter_tag']}`\n" if msg.get("enter_tag") else ""
        message = (
            f"{self._get_exit_emoji(msg)} *{self._exchange_from_msg(msg)}:* "
            f"{exit_wording} {msg['pair']} (#{msg['trade_id']})\n"
            f"{self._add_analyzed_candle(msg['pair'])}"
            f"*{f'{profit_prefix}Profit' if is_fill else f'Unrealized {profit_prefix}Profit'}:* "
            f"`{format_pct(msg['profit_ratio'])}{profit_extra}`\n"
            f"{cp_extra}"
            f"{enter_tag}"
            f"*Exit Reason:* `{msg['exit_reason']}`\n"
            f"*Direction:* `{msg['direction']}"
            f"{leverage_text}`\n"
            f"*Amount:* `{round_value(msg['amount'], 8)}`\n"
            f"*Open Rate:* `{fmt_coin2(msg['open_rate'], msg['quote_currency'])}`\n"
        )
        if msg["type"] == RPCMessageType.EXIT and msg["current_rate"]:
            message += (
                f"*Current Rate:* `{fmt_coin2(msg['current_rate'], msg['quote_currency'])}`\n"
            )
            if msg["order_rate"]:
                message += f"*Exit Rate:* `{fmt_coin2(msg['order_rate'], msg['quote_currency'])}`"
        elif msg["type"] == RPCMessageType.EXIT_FILL:
            message += f"*Exit Rate:* `{fmt_coin2(msg['close_rate'], msg['quote_currency'])}`"

        if is_sub_trade:
            stake_amount_fiat = self.__format_profit_fiat(msg, "stake_amount")

            rem = fmt_coin(msg["stake_amount"], msg["quote_currency"])
            message += f"\n*Remaining:* `{rem}{stake_amount_fiat}`"
        else:
            message += f"\n*Duration:* `{duration} ({duration_min:.1f} min)`"
        return message

    def __format_profit_fiat(
        self, msg: RPCExitMsg, key: Literal["stake_amount", "profit_amount", "cumulative_profit"]
    ) -> str:
        """
        Format Fiat currency to append to regular profit output
        """
        profit_fiat_extra = ""
        if self._rpc._fiat_converter and (fiat_currency := msg.get("fiat_currency")):
            profit_fiat = self._rpc._fiat_converter.convert_amount(
                msg[key], msg["stake_currency"], fiat_currency
            )
            profit_fiat_extra = f" / {profit_fiat:.3f} {fiat_currency}"
        return profit_fiat_extra

    def compose_message(self, msg: RPCSendMsg) -> str | None:
        if msg["type"] == RPCMessageType.ENTRY or msg["type"] == RPCMessageType.ENTRY_FILL:
            message = self._format_entry_msg(msg)

        elif msg["type"] == RPCMessageType.EXIT or msg["type"] == RPCMessageType.EXIT_FILL:
            message = self._format_exit_msg(msg)

        elif (
            msg["type"] == RPCMessageType.ENTRY_CANCEL or msg["type"] == RPCMessageType.EXIT_CANCEL
        ):
            message_side = "enter" if msg["type"] == RPCMessageType.ENTRY_CANCEL else "exit"
            message = (
                f"\N{WARNING SIGN} *{self._exchange_from_msg(msg)}:* "
                f"Cancelling {'partial ' if msg.get('sub_trade') else ''}"
                f"{message_side} Order for {msg['pair']} "
                f"(#{msg['trade_id']}). Reason: {msg['reason']}."
            )

        elif msg["type"] == RPCMessageType.PROTECTION_TRIGGER:
            message = (
                f"*Protection* triggered due to {msg['reason']}. "
                f"`{msg['pair']}` will be locked until `{msg['lock_end_time']}`."
            )

        elif msg["type"] == RPCMessageType.PROTECTION_TRIGGER_GLOBAL:
            message = (
                f"*Protection* triggered due to {msg['reason']}. "
                f"*All pairs* will be locked until `{msg['lock_end_time']}`."
            )

        elif msg["type"] == RPCMessageType.STATUS:
            message = f"*Status:* `{msg['status']}`"

        elif msg["type"] == RPCMessageType.WARNING:
            message = f"\N{WARNING SIGN} *Warning:* `{msg['status']}`"
        elif msg["type"] == RPCMessageType.EXCEPTION:
            # Errors will contain exceptions, which are wrapped in triple ticks.
            message = f"\N{WARNING SIGN} *ERROR:* \n {msg['status']}"

        elif msg["type"] == RPCMessageType.STARTUP:
            message = f"{msg['status']}"
        elif msg["type"] == RPCMessageType.STRATEGY_MSG:
            message = f"{msg['msg']}"
        else:
            logger.debug("Unknown message type: %s", msg["type"])
            return None
        return message

    def _message_loudness(self, msg: RPCSendMsg) -> str:
        """Determine the loudness of the message - on, off or silent"""
        default_noti = "on"

        msg_type = msg["type"]
        noti = ""
        if msg["type"] == RPCMessageType.EXIT or msg["type"] == RPCMessageType.EXIT_FILL:
            sell_noti = (
                self._config["telegram"].get("notification_settings", {}).get(str(msg_type), {})
            )

            # For backward compatibility sell still can be string
            if isinstance(sell_noti, str):
                noti = sell_noti
            else:
                default_noti = sell_noti.get("*", default_noti)
                noti = sell_noti.get(str(msg["exit_reason"]), default_noti)
        else:
            noti = (
                self._config["telegram"]
                .get("notification_settings", {})
                .get(str(msg_type), default_noti)
            )

        return noti

    @staticmethod
    def _is_critical_notification(msg: RPCSendMsg) -> bool:
        msg_type = msg.get("type")
        if msg_type in {
            RPCMessageType.WARNING,
            RPCMessageType.EXCEPTION,
            RPCMessageType.PROTECTION_TRIGGER,
            RPCMessageType.PROTECTION_TRIGGER_GLOBAL,
        }:
            return True
        return bool(msg_type == RPCMessageType.EXIT_FILL and msg.get("is_final_exit"))

    def _notification_business_fingerprint(self, msg: RPCSendMsg) -> str:
        """Stable PM incident identity from durable business evidence when available."""
        status = str(msg.get("status") or msg.get("reason") or "")
        if "PM" not in status and not msg.get("pair") and not msg.get("trade_id"):
            return ""
        parts: list[str] = []
        try:
            intents = (
                PMNotificationOutbox.session.query(PMOrderIntent)
                .filter(PMOrderIntent.state.in_(("PENDING", "PREPARED", "ACKED", "UNKNOWN")))
                .all()
            )
            parts.extend(
                f"i:{row.client_id}:{row.state}:{row.exchange_order_id or ''}:{row.origin_trade_id or ''}"
                for row in sorted(intents, key=lambda item: item.client_id)
            )
        except Exception:
            pass
        try:
            incidents = (
                PMNotificationOutbox.session.query(PMStreamJournal)
                .filter(PMStreamJournal.unresolved.is_(True))
                .all()
            )
            parts.extend(
                f"j:{row.id}:{row.pair}:{row.exchange_order_id}:{row.client_id or ''}:{row.reason}"
                for row in sorted(incidents, key=lambda item: int(item.id or 0))
            )
        except Exception:
            pass
        return "|".join(parts)

    def _notification_incident_id(self, msg: RPCSendMsg, message: str) -> tuple[str, str]:
        explicit = str(msg.get("incident_id") or "").strip()
        business = self._notification_business_fingerprint(msg)
        base = "|".join(
            [
                str(msg.get("type")),
                str(msg.get("pair") or ""),
                str(msg.get("trade_id") or ""),
                str(msg.get("status") or msg.get("reason") or message),
                business,
            ]
        )
        incident_id = explicit or ("tg-" + hashlib.sha256(base.encode("utf-8")).hexdigest()[:16])
        if msg.get("dedupe_once"):
            dedupe_basis = incident_id
        else:
            # Same incident is persisted at most once per 30-minute reminder window.
            bucket = int(datetime.now(UTC).timestamp() // 1800)
            dedupe_basis = f"{incident_id}:{bucket}"
        dedupe_key = hashlib.sha256(dedupe_basis.encode("utf-8")).hexdigest()
        return incident_id[:32], dedupe_key

    @staticmethod
    def _notification_priority(msg: RPCSendMsg) -> int:
        status = str(msg.get("status") or "").upper()
        msg_type = msg.get("type")
        if msg_type == RPCMessageType.EXCEPTION or any(
            token in status for token in ("FAIL-CLOSED", "SAFE_HOLD", "CRITICAL")
        ):
            return 100
        if msg_type in {RPCMessageType.PROTECTION_TRIGGER, RPCMessageType.PROTECTION_TRIGGER_GLOBAL}:
            return 90
        if msg_type == RPCMessageType.EXIT_FILL and msg.get("is_final_exit"):
            return 80
        return 50

    def _queue_critical_notification(
        self,
        msg: RPCSendMsg,
        message: str,
        disable_notification: bool,
    ) -> int | None:
        """Persist critical delivery without touching the trading transaction."""
        incident_id, dedupe_key = self._notification_incident_id(msg, message)
        body = message
        if "Incident:" not in body:
            body += f"\nIncident: `{incident_id}`"
        payload = {
            "text": body,
            "disable_notification": bool(disable_notification),
            "parse_mode": ParseMode.MARKDOWN,
            "trade_id": msg.get("trade_id"),
            "requires_open_trade": bool(msg.get("requires_open_trade", False)),
            "incident_id": incident_id,
            "event_type": str(msg.get("type")),
        }
        try:
            existing = PMNotificationOutbox.get_by_dedupe_key(dedupe_key)
            if existing is not None:
                return existing.id
            row = PMNotificationOutbox(
                incident_id=incident_id,
                dedupe_key=dedupe_key,
                channel="telegram",
                message=json.dumps(payload, default=str),
                state="PENDING",
                priority=self._notification_priority(msg),
            )
            PMNotificationOutbox.session.add(row)
            PMNotificationOutbox.session.commit()
            return row.id
        except Exception:
            try:
                PMNotificationOutbox.session.rollback()
            except Exception:
                pass
            logger.exception(
                "Could not persist critical Telegram notification; trading state is unchanged."
            )
            return None
        finally:
            try:
                PMNotificationOutbox.session.remove()
            except Exception:
                pass

    def _critical_payload_stale(self, payload: dict[str, Any]) -> bool:
        """True only when a queued action warning is provably no longer applicable."""
        if not payload.get("requires_open_trade"):
            return False
        trade_id = payload.get("trade_id")
        if not trade_id:
            # Missing validity identity is not proof the message is stale. Keep it
            # queued rather than sending an unvalidated action prompt.
            raise RuntimeError("critical notification requires open Trade but has no trade_id")
        trade = PMNotificationOutbox.session.get(Trade, int(trade_id))
        return trade is None or not trade.is_open or not trade.has_open_position

    def _rebuild_critical_notifications_from_business_state(self) -> int:
        """Compensate the crash window between business commit and notification enqueue.

        Durable PM intents/stream incidents and very recent closed Trades are the
        source of truth. Reconstructed notifications use stable incident ids, so
        replay can duplicate an operator message but can never replay a trade.
        """
        queued = 0
        try:
            intents = [
                {
                    "client_id": row.client_id,
                    "state": row.state,
                    "pair": row.pair,
                    "origin_trade_id": row.origin_trade_id,
                    "kind": row.kind,
                    "last_error": row.last_error,
                }
                for row in (
                    PMNotificationOutbox.session.query(PMOrderIntent)
                    .filter(PMOrderIntent.state.in_(("PENDING", "PREPARED", "ACKED", "UNKNOWN")))
                    .all()
                )
            ]
            journals = [
                {
                    "id": row.id,
                    "pair": row.pair,
                    "exchange_order_id": row.exchange_order_id,
                    "client_id": row.client_id,
                    "reason": row.reason,
                }
                for row in (
                    PMNotificationOutbox.session.query(PMStreamJournal)
                    .filter(PMStreamJournal.unresolved.is_(True))
                    .all()
                )
            ]
            cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=15)
            closed = [
                {
                    "id": trade.id,
                    "pair": trade.pair,
                    "close_date": trade.close_date,
                    "exit_reason": trade.exit_reason,
                }
                for trade in (
                    PMNotificationOutbox.session.query(Trade)
                    .filter(Trade.is_open.is_(False), Trade.close_date.isnot(None), Trade.close_date >= cutoff)
                    .limit(50)
                    .all()
                )
            ]
        except Exception as exc:
            try:
                PMNotificationOutbox.session.rollback()
            except Exception:
                pass
            logger.warning("Could not rebuild critical notifications from business state: %s", exc)
            return 0
        finally:
            try:
                PMNotificationOutbox.session.remove()
            except Exception:
                pass

        for item in intents:
            status = (
                f"PM RECOVERY: durable {item['kind']} intent {item['client_id']} "
                f"for {item['pair']} remains {item['state']}. "
                "The original client id is preserved and automatic duplicate POST is forbidden."
            )
            msg = {
                "type": RPCMessageType.WARNING,
                "status": status,
                "incident_id": f"pm-intent-{item['client_id']}",
                "pair": item["pair"],
                "trade_id": item["origin_trade_id"],
                "requires_open_trade": bool(
                    item["kind"] == "conditional" and item["origin_trade_id"]
                ),
            }
            rendered = self.compose_message(msg)
            if rendered and self._queue_critical_notification(msg, rendered, False) is not None:
                queued += 1

        for item in journals:
            msg = {
                "type": RPCMessageType.WARNING,
                "status": (
                    f"PM RECOVERY: unresolved user-stream ownership incident "
                    f"#{item['id']} {item['pair']} order={item['exchange_order_id']} "
                    f"client={item['client_id'] or 'unknown'} reason={item['reason']}."
                ),
                "incident_id": f"pm-stream-{item['id']}",
                "pair": item["pair"],
            }
            rendered = self.compose_message(msg)
            if rendered and self._queue_critical_notification(msg, rendered, False) is not None:
                queued += 1

        # A final fill may have committed immediately before the process died and
        # before Telegram enqueue. Reconstruct a one-time completion notice from
        # the durable Trade row. It is informational only and never calls trading.
        exit_fill_setting = (
            self._config.get("telegram", {})
            .get("notification_settings", {})
            .get(str(RPCMessageType.EXIT_FILL), "on")
        )
        if exit_fill_setting != "off":
            for item in closed:
                close_ts = item["close_date"].isoformat() if item["close_date"] else "unknown"
                msg = {
                    "type": RPCMessageType.WARNING,
                    "status": (
                        f"PM RECOVERY NOTICE: Trade #{item['id']} {item['pair']} is CLOSED "
                        f"in the durable database at {close_ts}; reason={item['exit_reason'] or 'unknown'}. "
                        "This completion notice was reconstructed after restart."
                    ),
                    "incident_id": f"pm-closed-{item['id']}-{close_ts}",
                    "trade_id": item["id"],
                    "pair": item["pair"],
                    "dedupe_once": True,
                }
                rendered = self.compose_message(msg)
                if rendered and self._queue_critical_notification(msg, rendered, False) is not None:
                    queued += 1
        return queued

    async def _deliver_critical_notification(self, row_id: int) -> None:
        try:
            row = PMNotificationOutbox.session.get(PMNotificationOutbox, row_id)
            if row is None or row.state != "PENDING":
                return
            payload = json.loads(row.message)
            if self._critical_payload_stale(payload):
                row.state = "STALE"
                row.last_error = "business state no longer satisfies delivery predicate"
                PMNotificationOutbox.session.commit()
                logger.info(
                    "Critical Telegram incident %s suppressed as stale before send.",
                    row.incident_id,
                )
                return

            ok = await self._send_msg(
                str(payload.get("text") or ""),
                parse_mode=str(payload.get("parse_mode") or ParseMode.MARKDOWN),
                disable_notification=bool(payload.get("disable_notification", False)),
            )
            now = datetime.now(UTC).replace(tzinfo=None)
            row.attempts += 1
            row.last_attempt_at = now
            if ok:
                previous_failures = max(0, row.attempts - 1)
                row.state = "SENT"
                row.sent_at = now
                row.next_attempt_at = None
                row.last_error = None
                # If this COMMIT fails after Telegram accepted the message the row
                # remains PENDING after rollback/restart. Re-delivery with the SAME
                # incident id is allowed; no trading business record is replayed.
                PMNotificationOutbox.session.commit()
                if previous_failures:
                    await self._send_msg(
                        f"Telegram delivery recovered for incident `{row.incident_id}` "
                        f"after {previous_failures} failed attempt(s).",
                        ParseMode.MARKDOWN,
                    )
                return
            delay = min(3600.0, 30.0 * (2 ** min(row.attempts - 1, 7)))
            if self._last_send_retry_after is not None:
                delay = max(delay, float(self._last_send_retry_after))
            row.next_attempt_at = now + timedelta(seconds=delay)
            row.last_error = self._last_send_error
            PMNotificationOutbox.session.commit()
        except Exception as e:
            try:
                PMNotificationOutbox.session.rollback()
            except Exception:
                pass
            logger.warning("Critical Telegram outbox delivery failed internally: %s", e)
        finally:
            try:
                PMNotificationOutbox.session.remove()
            except Exception:
                pass

    async def _drain_critical_notifications_once(self, limit: int = 20) -> int:
        """One bounded priority-ordered delivery pass; used by runtime and tests."""
        processed = 0
        try:
            now = datetime.now(UTC).replace(tzinfo=None)
            due = PMNotificationOutbox.due(now, limit=limit)
            ids = [row.id for row in due]
            PMNotificationOutbox.session.remove()
        except Exception as exc:
            try:
                PMNotificationOutbox.session.rollback()
                PMNotificationOutbox.session.remove()
            except Exception:
                pass
            ids = []
            logger.warning("Critical Telegram durable queue unavailable: %s", exc)

        for row_id in ids:
            await self._deliver_critical_notification(row_id)
            processed += 1

        # When the notification DB/pool was unavailable, a bounded memory queue
        # preserves best-effort alerts without creating one asyncio task per event.
        fallback_budget = max(0, min(5, limit - processed))
        for _ in range(fallback_budget):
            if not self._critical_fallback_queue:
                break
            item = self._critical_fallback_queue.popleft()
            try:
                if item.get("requires_open_trade"):
                    payload = {
                        "requires_open_trade": True,
                        "trade_id": item.get("trade_id"),
                    }
                    if self._critical_payload_stale(payload):
                        continue
                ok = await self._send_msg(
                    str(item.get("text") or ""),
                    parse_mode=str(item.get("parse_mode") or ParseMode.MARKDOWN),
                    disable_notification=bool(item.get("disable_notification", False)),
                )
            except Exception:
                ok = False
            finally:
                try:
                    PMNotificationOutbox.session.remove()
                except Exception:
                    pass
            if not ok:
                self._critical_fallback_queue.appendleft(item)
                break
            processed += 1
        return processed

    async def _critical_notification_retry_loop(self) -> None:
        self._critical_wakeup = asyncio.Event()
        rebuild_tick = 0
        while True:
            try:
                if rebuild_tick % 4 == 0:
                    self._rebuild_critical_notifications_from_business_state()
                rebuild_tick += 1
                await self._drain_critical_notifications_once(limit=20)
                self._critical_wakeup.clear()
                try:
                    await asyncio.wait_for(self._critical_wakeup.wait(), timeout=15.0)
                except TimeoutError:
                    pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Critical Telegram retry loop error: %s", e)
                await asyncio.sleep(1)

    def _wake_critical_notification_consumer(self) -> None:
        wakeup = self._critical_wakeup
        if wakeup is None or wakeup.is_set():
            return
        try:
            self._loop.call_soon_threadsafe(wakeup.set)
        except Exception:
            pass

    def send_msg(self, msg: RPCSendMsg) -> None:
        """Queue critical Telegram events; regular messages remain best-effort async."""
        noti = self._message_loudness(msg)
        if noti == "off":
            logger.info(f"Notification '{msg['type']}' not sent.")
            return
        message = self.compose_message(deepcopy(msg))
        if not message:
            return
        disable_notification = noti == "silent"
        if self._is_critical_notification(msg):
            row_id = self._queue_critical_notification(msg, message, disable_notification)
            if row_id is None:
                incident_id, _ = self._notification_incident_id(msg, message)
                body = message
                if "Incident:" not in body:
                    body += f"\nIncident: `{incident_id}`"
                if len(self._critical_fallback_queue) == self._critical_fallback_queue.maxlen:
                    logger.critical(
                        "Critical Telegram fallback queue full; oldest unsent fallback will be evicted."
                    )
                self._critical_fallback_queue.append(
                    {
                        "text": body,
                        "disable_notification": disable_notification,
                        "parse_mode": ParseMode.MARKDOWN,
                        "trade_id": msg.get("trade_id"),
                        "requires_open_trade": bool(msg.get("requires_open_trade", False)),
                    }
                )
            self._wake_critical_notification_consumer()
            return
        asyncio.run_coroutine_threadsafe(
            self._send_msg(message, disable_notification=disable_notification), self._loop
        )

    def _get_exit_emoji(self, msg):
        """
        Get emoji for exit-messages
        """

        if float(msg["profit_ratio"]) >= 0.05:
            return "\N{ROCKET}"
        elif float(msg["profit_ratio"]) >= 0.0:
            return "\N{EIGHT SPOKED ASTERISK}"
        elif msg["exit_reason"] == "stop_loss":
            return "\N{WARNING SIGN}"
        else:
            return "\N{CROSS MARK}"

    def _prepare_order_details(self, filled_orders: list, quote_currency: str, is_open: bool):
        """
        Prepare details of trade with entry adjustment enabled
        """
        lines_detail: list[str] = []
        if len(filled_orders) > 0:
            first_avg = filled_orders[0]["safe_price"]
        order_nr = 0
        for order in filled_orders:
            lines: list[str] = []
            if order["is_open"] is True:
                continue
            order_nr += 1
            wording = "Entry" if order["ft_is_entry"] else "Exit"

            cur_entry_amount = order["filled"] or order["amount"]
            cur_entry_average = order["safe_price"]
            lines.append("  ")
            lines.append(f"*{wording} #{order_nr}:*")
            if order_nr == 1:
                lines.append(
                    f"*Amount:* {round_value(cur_entry_amount, 8)} "
                    f"({fmt_coin(order['cost'], quote_currency)})"
                )
                lines.append(f"*Average Price:* {round_value(cur_entry_average, 8)}")
            else:
                # TODO: This calculation ignores fees.
                price_to_1st_entry = (cur_entry_average - first_avg) / first_avg
                if is_open:
                    lines.append(f"({dt_humanize_delta(order['order_filled_date'])})")
                lines.append(
                    f"*Amount:* {round_value(cur_entry_amount, 8)} "
                    f"({fmt_coin(order['cost'], quote_currency)})"
                )
                lines.append(
                    f"*Average {wording} Price:* {round_value(cur_entry_average, 8)} "
                    f"({format_pct(price_to_1st_entry)} from 1st entry rate)"
                )
                lines.append(f"*Order Filled:* {order['order_filled_date']}")

            lines_detail.append("\n".join(lines))

        return lines_detail

    @authorized_only
    async def _order(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /order.
        Returns the orders of the trade
        :param bot: telegram bot
        :param update: message update
        :return: None
        """

        trade_ids = []
        if context.args and len(context.args) > 0:
            trade_ids = [int(i) for i in context.args if i.isnumeric()]

        try:
            results = self._rpc._rpc_trade_status(trade_ids=trade_ids)
        except RPCException as exc:
            # Freqtrade trade status intentionally contains only positions owned by
            # this bot.  On a PM account, fall back to the explicit read-only
            # account view so manual/external positions are visible without ever
            # importing them into strategy state.
            if (
                str(exc) == "no active trade"
                and self._config.get("trading_mode") == "futures"
                and self._config.get("exchange", {}).get("portfolio_margin")
            ):
                await self._pm_status(update, context)
                return
            raise
        for r in results:
            lines = [f"*Order List for Trade #*`{r['trade_id']}`"]

            lines_detail = self._prepare_order_details(
                r["orders"], r["quote_currency"], r["is_open"]
            )
            lines.extend(lines_detail if lines_detail else "")
            await self.__send_order_msg(lines, r)

    async def __send_order_msg(self, lines: list[str], r: dict[str, Any]) -> None:
        """
        Send status message.
        """
        msg = ""

        for line in lines:
            if line:
                if (len(msg) + len(line) + 1) < MAX_MESSAGE_LENGTH:
                    msg += line + "\n"
                else:
                    await self._send_msg(msg)
                    msg = f"*Order List for Trade #*`{r['trade_id']}` - continued\n" + line + "\n"

        await self._send_msg(msg)

    @authorized_only
    async def _status(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /status.
        Returns the current TradeThread status
        :param bot: telegram bot
        :param update: message update
        :return: None
        """

        if context.args and "table" in context.args:
            await self._status_table(update, context)
            return
        else:
            await self._status_msg(update, context)

    async def _status_msg(self, update: Update, context: CallbackContext) -> None:
        """
        handler for `/status` and `/status <id>`.

        """
        # Check if there's at least one numerical ID provided.
        # If so, try to get only these trades.
        trade_ids = []
        if context.args and len(context.args) > 0:
            trade_ids = [int(i) for i in context.args if i.isnumeric()]

        try:
            results = self._rpc._rpc_trade_status(trade_ids=trade_ids)
        except RPCException as exc:
            if (
                str(exc) == "no active trade"
                and self._config.get("trading_mode") == "futures"
                and self._config.get("exchange", {}).get("portfolio_margin")
            ):
                # Do not import manual account positions into the strategy.  In the
                # no-local-trade case, expose the live PAPI account snapshot instead.
                await self._pm_status(update, context)
                return
            raise
        position_adjust = self._config.get("position_adjustment_enable", False)
        max_entries = self._config.get("max_entry_position_adjustment", -1)
        for r in results:
            r["open_date_hum"] = dt_humanize_delta(r["open_date"])

            r["stake_amount_r"] = fmt_coin(r["stake_amount"], r["quote_currency"])
            r["max_stake_amount_r"] = fmt_coin(
                r["max_stake_amount"] or r["stake_amount"], r["quote_currency"]
            )
            r["profit_abs_r"] = fmt_coin(r["profit_abs"], r["quote_currency"])
            r["realized_profit_r"] = fmt_coin(r["realized_profit"], r["quote_currency"])
            r["total_profit_abs_r"] = fmt_coin(r["total_profit_abs"], r["quote_currency"])
            lines = [
                f"*Trade ID:* `{r['trade_id']}`"
                + (f" `(since {r['open_date_hum']})`" if r["is_open"] else ""),
                f"*Current Pair:* {r['pair']}",
                (
                    f"*Direction:* {'`Short`' if r.get('is_short') else '`Long`'}"
                    + (f" ` ({r['leverage']}x)`" if r.get("leverage") else "")
                ),
                f"*Amount:* `{r['amount']} ({r['stake_amount_r']})`",
                f"*Total invested:* `{r['max_stake_amount_r']}`" if position_adjust else "",
                f"*Enter Tag:* `{r['enter_tag']}`" if r["enter_tag"] else "",
                f"*Exit Reason:* `{r['exit_reason']}`" if r.get("exit_reason") else "",
            ]

            if position_adjust:
                max_buy_str = f"/{max_entries + 1}" if (max_entries > 0) else ""
                lines.extend(
                    [
                        f"*Number of Entries:* `{r['nr_of_successful_entries']}{max_buy_str}`",
                        f"*Number of Exits:* `{r['nr_of_successful_exits']}`",
                    ]
                )

            lines.extend(
                [
                    f"*Open Rate:* `{round_value(r['open_rate'], 8)}`",
                    f"*Close Rate:* `{round_value(r['close_rate'], 8)}`" if r["close_rate"] else "",
                    f"*Open Date:* `{r['open_date']}`",
                    f"*Close Date:* `{r['close_date']}`" if r["close_date"] else "",
                    (
                        f" \n*Current Rate:* `{round_value(r['current_rate'], 8)}`"
                        if r["is_open"]
                        else ""
                    ),
                    ("*Unrealized Profit:* " if r["is_open"] else "*Close Profit: *")
                    + f"`{format_pct(r['profit_ratio'])}` `({r['profit_abs_r']})`",
                ]
            )

            if r["is_open"]:
                if (
                    r.get("realized_profit") is not None
                    and r.get("realized_profit_ratio") is not None
                ):
                    lines.append(
                        f"*Realized Profit:* `{format_pct(r['realized_profit_ratio'])} "
                        f"({r['realized_profit_r']})`"
                    )
                if r.get("total_profit_ratio") is not None:
                    lines.append(
                        f"*Total Profit:* `{format_pct(r['total_profit_ratio'])} "
                        f"({r['total_profit_abs_r']})`"
                    )

                # Append empty line to improve readability
                lines.append(" ")
                # Adding liquidation only if it is not None
                if liquidation := r.get("liquidation_price"):
                    lines.append(f"*Liquidation:* `{round_value(liquidation, 8)}`")

                if (
                    r["stop_loss_abs"] != r["initial_stop_loss_abs"]
                    and r["initial_stop_loss_ratio"] is not None
                ):
                    # Adding initial stoploss only if it is different from stoploss
                    lines.append(
                        f"*Initial Stoploss:* `{r['initial_stop_loss_abs']:.8f}` "
                        f"`({format_pct(r['initial_stop_loss_ratio'])})`"
                    )

                # Adding stoploss and stoploss percentage only if it is not None
                lines.append(
                    f"*Stoploss:* `{round_value(r['stop_loss_abs'], 8)}` "
                    + (f"`({format_pct(r['stop_loss_ratio'])})`" if r["stop_loss_ratio"] else "")
                )
                lines.append(
                    f"*Stoploss distance:* `{round_value(r['stoploss_current_dist'], 8)}` "
                    f"`({format_pct(r['stoploss_current_dist_ratio'])})`"
                )
                if open_orders := r.get("open_orders"):
                    lines.append(
                        f"*Open Order:* `{open_orders}`"
                        + (f"- `{r['exit_order_status']}`" if r["exit_order_status"] else "")
                    )

            await self.__send_status_msg(lines, r)

    async def __send_status_msg(self, lines: list[str], r: dict[str, Any]) -> None:
        """
        Send status message.
        """
        msg = ""

        for line in lines:
            if line:
                if (len(msg) + len(line) + 1) < MAX_MESSAGE_LENGTH:
                    msg += line + "\n"
                else:
                    await self._send_msg(msg)
                    msg = f"*Trade ID:* `{r['trade_id']}` - continued\n" + line + "\n"

        await self._send_msg(msg)

    @authorized_only
    async def _status_table(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /status table.
        Returns the current TradeThread status in table format
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        fiat_currency = self._config.get("fiat_display_currency", "")
        try:
            statlist, head, fiat_profit_sum, fiat_total_profit_sum = self._rpc._rpc_status_table(
                self._config["stake_currency"], fiat_currency
            )
        except RPCException as exc:
            if (
                str(exc) == "no active trade"
                and self._config.get("trading_mode") == "futures"
                and self._config.get("exchange", {}).get("portfolio_margin")
            ):
                await self._pm_status(update, context)
                return
            raise

        show_total = not isnan(fiat_profit_sum) and len(statlist) > 1
        show_total_realized = (
            not isnan(fiat_total_profit_sum) and len(statlist) > 1 and fiat_profit_sum
        ) != fiat_total_profit_sum
        max_trades_per_msg = 50
        """
        Calculate the number of messages of 50 trades per message
        0.99 is used to make sure that there are no extra (empty) messages
        As an example with 50 trades, there will be int(50/50 + 0.99) = 1 message
        """
        messages_count = max(int(len(statlist) / max_trades_per_msg + 0.99), 1)
        for i in range(0, messages_count):
            trades = statlist[i * max_trades_per_msg : (i + 1) * max_trades_per_msg]
            if show_total and i == messages_count - 1:
                # append total line
                trades.append(["Total", "", "", f"{fiat_profit_sum:.2f} {fiat_currency}"])
                if show_total_realized:
                    trades.append(
                        [
                            "Total",
                            "(incl. realized Profits)",
                            "",
                            f"{fiat_total_profit_sum:.2f} {fiat_currency}",
                        ]
                    )

            message = tabulate(trades, headers=head, tablefmt="simple")
            if show_total and i == messages_count - 1:
                # insert separators line between Total
                lines = message.split("\n")
                offset = 2 if show_total_realized else 1
                message = "\n".join(lines[:-offset] + [lines[1]] + lines[-offset:])
            await self._send_msg(
                f"<pre>{message}</pre>",
                parse_mode=ParseMode.HTML,
                reload_able=True,
                callback_path="update_status_table",
                query=update.callback_query,
            )

    async def _timeunit_stats(self, update: Update, context: CallbackContext, unit: str) -> None:
        """
        Handler for /daily <n>
        Returns a daily profit (in BTC) over the last n days.
        :param bot: telegram bot
        :param update: message update
        :return: None
        """

        vals = {
            "days": TimeunitMappings("Day", "Daily", "days", "update_daily", 7, "%Y-%m-%d"),
            "weeks": TimeunitMappings(
                "Monday", "Weekly", "weeks (starting from Monday)", "update_weekly", 8, "%Y-%m-%d"
            ),
            "months": TimeunitMappings("Month", "Monthly", "months", "update_monthly", 6, "%Y-%m"),
        }
        val = vals[unit]

        stake_cur = self._config["stake_currency"]
        fiat_disp_cur = self._config.get("fiat_display_currency", "")
        try:
            timescale = int(context.args[0]) if context.args else val.default
        except (TypeError, ValueError, IndexError):
            timescale = val.default
        stats = self._rpc._rpc_timeunit_profit(timescale, stake_cur, fiat_disp_cur, unit)
        stats_tab = tabulate(
            [
                [
                    f"{period['date']:{val.dateformat}} ({period['trade_count']})",
                    f"{fmt_coin(period['abs_profit'], stats['stake_currency'])}",
                    f"{period['fiat_value']:.2f} {stats['fiat_display_currency']}",
                    f"{format_pct(period['rel_profit'])}",
                ]
                for period in stats["data"]
            ],
            headers=[
                f"{val.header} (count)",
                f"{stake_cur}",
                f"{fiat_disp_cur}",
                "Profit %",
                "Trades",
            ],
            tablefmt="simple",
        )
        message = (
            f"<b>{val.message} Profit over the last {timescale} {val.message2}</b>:\n"
            f"<pre>{stats_tab}</pre>"
        )
        await self._send_msg(
            message,
            parse_mode=ParseMode.HTML,
            reload_able=True,
            callback_path=val.callback,
            query=update.callback_query,
        )

    @authorized_only
    async def _daily(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /daily <n>
        Returns a daily profit (in BTC) over the last n days.
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        await self._timeunit_stats(update, context, "days")

    @authorized_only
    async def _weekly(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /weekly <n>
        Returns a weekly profit (in BTC) over the last n weeks.
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        await self._timeunit_stats(update, context, "weeks")

    @authorized_only
    async def _monthly(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /monthly <n>
        Returns a monthly profit (in BTC) over the last n months.
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        await self._timeunit_stats(update, context, "months")

    def _format_profit_message(
        self,
        stats: dict,
        stake_cur: str,
        fiat_disp_cur: str,
        timescale: int | None = None,
        direction: str | None = None,
    ) -> str:
        """
        Format profit statistics message for telegram.

        :param stats: Trade statistics dictionary
        :param stake_cur: Stake currency
        :param fiat_disp_cur: Fiat display currency
        :param timescale: Optional timescale filter
        :param direction: Optional direction filter ('long', 'short', or None for all)
        :return: Formatted markdown message
        """
        # Extract common variables
        profit_closed_coin = stats["profit_closed_coin"]
        profit_closed_ratio_mean = stats["profit_closed_ratio_mean"]
        profit_closed_percent = stats["profit_closed_percent"]
        profit_closed_fiat = stats["profit_closed_fiat"]
        profit_all_coin = stats["profit_all_coin"]
        profit_all_ratio_mean = stats["profit_all_ratio_mean"]
        profit_all_percent = stats["profit_all_percent"]
        profit_all_fiat = stats["profit_all_fiat"]
        trade_count = stats["trade_count"]
        first_trade_date = f"{stats['first_trade_humanized']} ({stats['first_trade_date']})"
        latest_trade_date = f"{stats['latest_trade_humanized']} ({stats['latest_trade_date']})"
        avg_duration = stats["avg_duration"]
        best_pair = stats["best_pair"]
        best_pair_profit_ratio = stats["best_pair_profit_ratio"]
        best_pair_profit_abs = fmt_coin(stats["best_pair_profit_abs"], stake_cur)
        winrate = stats["winrate"]
        expectancy = stats["expectancy"]
        expectancy_ratio = stats["expectancy_ratio"]

        # Direction-specific labels
        direction_label = f" {direction}" if direction else ""
        no_trades_msg = (
            f"No{direction_label} trades yet.\n*Bot started:* `{stats['bot_start_date']}`"
        )
        no_closed_msg = f"`No closed{direction_label} trade` \n"
        closed_roi_label = f"*ROI:* Closed{direction_label} trades"
        all_roi_label = f"*ROI:* All{direction_label} trades"

        if stats["trade_count"] == 0:
            return no_trades_msg

        # Build message
        if stats["closed_trade_count"] > 0:
            fiat_closed_trades = (
                f"∙ `{fmt_coin(profit_closed_fiat, fiat_disp_cur)}`\n" if fiat_disp_cur else ""
            )
            markdown_msg = (
                f"{closed_roi_label}\n"
                f"∙ `{fmt_coin(profit_closed_coin, stake_cur)} "
                f"({format_pct(profit_closed_ratio_mean)}) "
                f"({profit_closed_percent} \N{GREEK CAPITAL LETTER SIGMA}%)`\n"
                f"{fiat_closed_trades}"
            )
        else:
            markdown_msg = no_closed_msg

        fiat_all_trades = (
            f"∙ `{fmt_coin(profit_all_fiat, fiat_disp_cur)}`\n" if fiat_disp_cur else ""
        )
        markdown_msg += (
            f"{all_roi_label}\n"
            f"∙ `{fmt_coin(profit_all_coin, stake_cur)} "
            f"({format_pct(profit_all_ratio_mean)}) "
            f"({profit_all_percent} \N{GREEK CAPITAL LETTER SIGMA}%)`\n"
            f"{fiat_all_trades}"
            f"*Total Trade Count:* `{trade_count}`\n"
            f"*Bot started:* `{stats['bot_start_date']}`\n"
            f"*{'First Trade opened' if not timescale else 'Showing Profit since'}:* "
            f"`{first_trade_date}`\n"
            f"*Latest Trade opened:* `{latest_trade_date}`\n"
            f"*Win / Loss:* `{stats['winning_trades']} / {stats['losing_trades']}`\n"
            f"*Winrate:* `{format_pct(winrate)}`\n"
            f"*Expectancy (Ratio):* `{expectancy:.2f} ({expectancy_ratio:.2f})`"
        )

        if stats["closed_trade_count"] > 0:
            markdown_msg += (
                f"\n*Avg. Duration:* `{avg_duration}`\n"
                f"*Best Performing:* `{best_pair}: {best_pair_profit_abs} "
                f"({format_pct(best_pair_profit_ratio)})`\n"
                f"*Trading volume:* `{fmt_coin(stats['trading_volume'], stake_cur)}`\n"
                f"*Profit factor:* `{stats['profit_factor']:.2f}`\n"
                f"*Max Drawdown:* `{format_pct(stats['max_drawdown'])} "
                f"({fmt_coin(stats['max_drawdown_abs'], stake_cur)})`\n"
                f"    from `{stats['max_drawdown_start']} "
                f"({fmt_coin(stats['drawdown_high'], stake_cur)})`\n"
                f"    to `{stats['max_drawdown_end']} "
                f"({fmt_coin(stats['drawdown_low'], stake_cur)})`\n"
                f"*Current Drawdown:* `{format_pct(stats['current_drawdown'])} "
                f"({fmt_coin(stats['current_drawdown_abs'], stake_cur)})`\n"
                f"    from `{stats['current_drawdown_start']} "
                f"({fmt_coin(stats['current_drawdown_high'], stake_cur)})`\n"
            )

        return markdown_msg

    async def _profit_handler(
        self,
        update: Update,
        context: CallbackContext,
        direction: str | None = None,
    ) -> None:
        """
        Common handler for profit commands.

        :param update: Telegram update
        :param context: Callback context
        :param direction: Trade direction filter ('long', 'short', or None)
        :param callback_path: Callback path for message updates
        """
        stake_cur = self._config["stake_currency"]
        fiat_disp_cur = self._config.get("fiat_display_currency", "")

        start_date = datetime.fromtimestamp(0)
        timescale = None
        try:
            if context.args:
                if not direction:
                    arg = context.args[0].lower()
                    if arg in ("short", "long"):
                        direction = arg
                        context.args.pop(0)  # Remove direction from args
                timescale = int(context.args[0]) - 1
                today_start = datetime.combine(date.today(), datetime.min.time())
                start_date = today_start - timedelta(days=timescale)
        except (TypeError, ValueError, IndexError):
            pass

        # Get stats with optional direction filter
        stats_kwargs = {
            "stake_currency": stake_cur,
            "fiat_display_currency": fiat_disp_cur,
            "start_date": start_date,
        }
        if direction:
            stats_kwargs["direction"] = direction

        stats = self._rpc._rpc_trade_statistics(**stats_kwargs)
        markdown_msg = self._format_profit_message(
            stats, stake_cur, fiat_disp_cur, timescale, direction
        )

        await self._send_msg(
            markdown_msg,
            reload_able=True,
            callback_path="update_profit" if not direction else f"update_profit_{direction}",
            query=update.callback_query,
        )

    @authorized_only
    async def _profit(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /profit.
        Returns a cumulative profit statistics.
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        await self._profit_handler(update, context)

    @authorized_only
    async def _profit_long(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /profit_long.
        Returns cumulative profit statistics for long trades.
        """
        await self._profit_handler(update, context, direction="long")

    @authorized_only
    async def _profit_short(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /profit_short.
        Returns cumulative profit statistics for short trades.
        """
        await self._profit_handler(update, context, direction="short")

    @authorized_only
    async def _stats(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /stats
        Show stats of recent trades
        """
        stats = self._rpc._rpc_stats()

        reason_map = {
            "roi": "ROI",
            "stop_loss": "Stoploss",
            "trailing_stop_loss": "Trail. Stop",
            "stoploss_on_exchange": "Stoploss",
            "exit_signal": "Exit Signal",
            "force_exit": "Force Exit",
            "emergency_exit": "Emergency Exit",
        }
        exit_reasons_tabulate = [
            [reason_map.get(reason, reason), sum(count.values()), count["wins"], count["losses"]]
            for reason, count in stats["exit_reasons"].items()
        ]
        exit_reasons_msg = "No trades yet."
        for reason in chunks(exit_reasons_tabulate, 25):
            exit_reasons_msg = tabulate(reason, headers=["Exit Reason", "Exits", "Wins", "Losses"])
            if len(exit_reasons_tabulate) > 25:
                await self._send_msg(f"```\n{exit_reasons_msg}```", ParseMode.MARKDOWN)
                exit_reasons_msg = ""

        durations = stats["durations"]
        duration_msg = tabulate(
            [
                [
                    "Wins",
                    (
                        str(timedelta(seconds=durations["wins"]))
                        if durations["wins"] is not None
                        else "N/A"
                    ),
                ],
                [
                    "Losses",
                    (
                        str(timedelta(seconds=durations["losses"]))
                        if durations["losses"] is not None
                        else "N/A"
                    ),
                ],
            ],
            headers=["", "Avg. Duration"],
        )
        msg = f"""```\n{exit_reasons_msg}```\n```\n{duration_msg}```"""

        await self._send_msg(msg, ParseMode.MARKDOWN)

    @authorized_only
    async def _balance(self, update: Update, context: CallbackContext) -> None:
        """Handler for /balance"""
        full_result = context.args and "full" in context.args
        result = self._rpc._rpc_balance(
            self._config["stake_currency"], self._config.get("fiat_display_currency", "")
        )

        balance_dust_level = self._config["telegram"].get("balance_dust_level", 0.0)
        if not balance_dust_level:
            balance_dust_level = DUST_PER_COIN.get(self._config["stake_currency"], 1.0)

        output = ""
        if self._config["dry_run"]:
            output += "*Warning:* Simulated balances in Dry Mode.\n"
        starting_cap = fmt_coin(result["starting_capital"], self._config["stake_currency"])
        output += f"Starting capital: `{starting_cap}`"
        starting_cap_fiat = (
            fmt_coin(result["starting_capital_fiat"], self._config["fiat_display_currency"])
            if result["starting_capital_fiat"] > 0
            else ""
        )
        output += (f" `, {starting_cap_fiat}`.\n") if result["starting_capital_fiat"] > 0 else ".\n"

        total_dust_balance = 0
        total_dust_currencies = 0
        for curr in result["currencies"]:
            curr_output = ""
            if (curr["is_position"] or curr["est_stake"] > balance_dust_level) and (
                full_result or curr["is_bot_managed"]
            ):
                if curr["is_position"]:
                    curr_output = (
                        f"*{curr['currency']}:*\n"
                        f"\t`{curr['side']}: {round_value(curr['position'], 8)}`\n"
                        f"\t`Est. {curr['stake']}: "
                        f"{fmt_coin(curr['est_stake'], curr['stake'], False)}`\n"
                    )
                else:
                    est_stake = fmt_coin(
                        curr["est_stake" if full_result else "est_stake_bot"], curr["stake"], False
                    )

                    curr_output = (
                        f"*{curr['currency']}:*\n"
                        f"\t`Available: {fmt_coin(curr['free'], curr['currency'], False)}`\n"
                        f"\t`Balance: {fmt_coin(curr['balance'], curr['currency'], False)}`\n"
                        f"\t`Pending: {fmt_coin(curr['used'], curr['currency'], False)}`\n"
                        f"\t`Bot Owned: {fmt_coin(curr['bot_owned'], curr['currency'], False)}`\n"
                        f"\t`Est. {curr['stake']}: {est_stake}`\n"
                    )

            elif curr["est_stake"] <= balance_dust_level:
                total_dust_balance += curr["est_stake"]
                total_dust_currencies += 1

            # Handle overflowing message length
            if len(output + curr_output) >= MAX_MESSAGE_LENGTH:
                await self._send_msg(output)
                output = curr_output
            else:
                output += curr_output

        if total_dust_balance > 0:
            output += (
                f"*{total_dust_currencies} Other "
                f"{plural(total_dust_currencies, 'Currency', 'Currencies')} "
                f"(< {balance_dust_level} {result['stake']}):*\n"
                f"\t`Est. {result['stake']}: "
                f"{fmt_coin(total_dust_balance, result['stake'], False)}`\n"
            )
        tc = result["trade_count"] > 0
        stake_improve = f" `({result['starting_capital_ratio']:.2%})`" if tc else ""
        fiat_val = f" `({result['starting_capital_fiat_ratio']:.2%})`" if tc else ""
        value = fmt_coin(result["value" if full_result else "value_bot"], result["symbol"], False)
        total_stake = fmt_coin(
            result["total" if full_result else "total_bot"], result["stake"], False
        )
        fiat_estimated_value = (
            f"\t`{result['symbol']}: {value}`{fiat_val}\n" if result["symbol"] else ""
        )
        output += (
            f"\n*Estimated Value{' (Bot managed assets only)' if not full_result else ''}*:\n"
            f"\t`{result['stake']}: {total_stake}`{stake_improve}\n"
            f"{fiat_estimated_value}"
        )
        await self._send_msg(
            output, reload_able=True, callback_path="update_balance", query=update.callback_query
        )

    @authorized_only
    async def _start(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /start.
        Starts TradeThread
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        msg = self._rpc._rpc_start()
        await self._send_msg(f"Status: `{msg['status']}`")

    @authorized_only
    async def _stop(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /stop.
        Stops TradeThread
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        msg = self._rpc._rpc_stop()
        await self._send_msg(f"Status: `{msg['status']}`")

    @authorized_only
    async def _reload_config(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /reload_config.
        Triggers a config file reload
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        msg = self._rpc._rpc_reload_config()
        await self._send_msg(f"Status: `{msg['status']}`")

    @authorized_only
    async def _pause(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /stop_buy /stop_entry and /pause.
        Sets bot state to paused
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        msg = self._rpc._rpc_pause()
        await self._send_msg(f"Status: `{msg['status']}`")

    @authorized_only
    async def _reload_trade_from_exchange(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /reload_trade <tradeid>.
        """
        if not context.args or len(context.args) == 0:
            raise RPCException("Trade-id not set.")
        trade_id = int(context.args[0])
        msg = self._rpc._rpc_reload_trade_from_exchange(trade_id)
        await self._send_msg(f"Status: `{msg['status']}`")

    @authorized_only
    async def _force_exit(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /forceexit <id>.
        Sells the given trade at current price
        :param bot: telegram bot
        :param update: message update
        :return: None
        """

        if context.args:
            trade_id = context.args[0]
            await self._force_exit_action(trade_id)
        else:
            fiat_currency = self._config.get("fiat_display_currency", "")
            try:
                statlist, _, _, _ = self._rpc._rpc_status_table(
                    self._config["stake_currency"], fiat_currency
                )
            except RPCException:
                await self._send_msg(msg="No open trade found.")
                return
            trades = []
            for trade in statlist:
                trades.append((trade[0], f"{trade[0]} {trade[1]} {trade[2]} {trade[3]}"))

            trade_buttons = [
                InlineKeyboardButton(text=trade[1], callback_data=f"force_exit__{trade[0]}")
                for trade in trades
            ]
            buttons_aligned = self._layout_inline_keyboard(trade_buttons, cols=1)

            buttons_aligned.append(
                [InlineKeyboardButton(text="Cancel", callback_data="force_exit__cancel")]
            )
            await self._send_msg(msg="Which trade?", keyboard=buttons_aligned)

    async def _force_exit_action(self, trade_id: str):
        if trade_id != "cancel":
            try:
                loop = asyncio.get_running_loop()
                # Workaround to avoid nested loops
                await loop.run_in_executor(None, safe_async_db(self._rpc._rpc_force_exit), trade_id)
            except RPCException as e:
                await self._send_msg(str(e))

    @authorized_only
    async def _pm_close(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /pm_close [all|USDT|USDC] CONFIRM.
        Creates market exit orders for all matching futures trades.

        Market-closing positions is destructive, so an explicit ``CONFIRM`` token is
        required to prevent accidental one-command liquidation of the whole book.
        """
        args = context.args or []
        if not args:
            await self._send_msg(
                "Usage: `/pm_close [all|USDT|USDC] CONFIRM`.\n"
                "This market-closes matching futures trades. Re-send with `CONFIRM` to execute."
            )
            return

        if args[-1] != "CONFIRM":
            target = args[0] if args else "all"
            await self._send_msg(
                f"Confirm closing `{target}` trades by sending `/pm_close {target} CONFIRM`."
            )
            return

        target_currency = args[0] if len(args) > 1 else "all"
        try:
            loop = asyncio.get_running_loop()
            msg = await loop.run_in_executor(
                None,
                safe_async_db(self._rpc._rpc_pm_close),
                target_currency,
                "market",
            )
            await self._send_msg(f"Status: `{msg['result']}`")
        except RPCException as e:
            await self._send_msg(str(e))

    @authorized_only
    async def _pm_status(self, update: Update, context: CallbackContext) -> None:
        """Handler for /pm_status."""
        try:
            loop = asyncio.get_running_loop()
            status = await loop.run_in_executor(None, safe_async_db(self._rpc._rpc_pm_status))
        except RPCException as e:
            await self._send_msg(str(e))
            return

        balances = status["balances"]
        positions = status["positions"]
        stream = status.get("user_stream") or {}

        balance_lines = []
        for currency, balance in sorted(balances.items()):
            balance_lines.append(
                f"{currency}: total={round_value(balance.get('total', 0), 8)}, "
                f"free={round_value(balance.get('free', 0), 8)}, "
                f"used={round_value(balance.get('used', 0), 8)}"
            )
        if not balance_lines:
            balance_lines.append("none")

        position_lines = []
        for position in positions[:20]:
            entry = round_value(position.get("entryPrice"), 8)
            mark = round_value(position.get("markPrice"), 8)
            pnl = round_value(position.get("unrealizedPnl"), 8)
            leverage = round_value(position.get("leverage"), 8)
            liquidation = round_value(position.get("liquidationPrice"), 8)
            position_lines.append(
                f"{position.get('symbol')}: {position.get('side')} "
                f"contracts={round_value(position.get('contracts', 0), 8)} "
                f"{leverage}x entry={entry} mark={mark} uPnL={pnl} "
                f"liq={liquidation} margin={round_value(position.get('initialMargin') or 0, 8)}"
            )
        if len(positions) > 20:
            position_lines.append(f"... {len(positions) - 20} more")
        if not position_lines:
            position_lines.append("none")

        message = (
            "*Binance PM Status (read-only account view)*\n"
            "External/manual positions are displayed only; this bot does not manage them.\n"
            f"Account: `{status['account_status']}`\n"
            f"uniMMR: `{status['uni_mmr']}`\n"
            f"Equity: `{status['account_equity']}`\n"
            f"Initial margin: `{status['initial_margin']}`\n"
            f"Maintenance margin: `{status['maintenance_margin']}`\n"
            f"User stream: `enabled={stream.get('enabled')}, "
            f"running={stream.get('running')}, connected={stream.get('connected')}, "
            f"queued={stream.get('queued_events')}, last={stream.get('last_event_type')}`\n"
            f"Stream health: `reconnects={stream.get('reconnects')}, "
            f"dropped={stream.get('events_dropped')}, parse_errors={stream.get('parse_errors')}, "
            f"last_error={stream.get('last_error')}`\n\n"
            "*Balances:*\n"
            f"`{chr(10).join(balance_lines)}`\n\n"
            "*Positions:*\n"
            f"`{chr(10).join(position_lines)}`"
        )
        await self._send_msg(message, ParseMode.MARKDOWN)

    @authorized_only
    async def _pm_risk(self, update: Update, context: CallbackContext) -> None:
        """Handler for /pm_risk."""
        try:
            loop = asyncio.get_running_loop()
            risk = await loop.run_in_executor(None, safe_async_db(self._rpc._rpc_pm_risk))
        except RPCException as e:
            await self._send_msg(str(e))
            return

        alerts_str = "\n".join(f"  - {a}" for a in risk["alerts"]) if risk["alerts"] else "none"
        orders_ok = "YES" if risk["new_orders_allowed"] else "NO (BLOCKED)"

        message = (
            "*Binance PM Risk*\n"
            f"Account: `{risk['account_status']}`\n"
            f"uniMMR: `{risk['uni_mmr']}`\n"
            f"Equity: `{risk['account_equity']}`\n"
            f"Initial margin: `{risk['initial_margin']}`\n"
            f"Maintenance margin: `{risk['maintenance_margin']}`\n"
            f"Collateral: `{risk['total_collateral_value']}`\n"
            f"min_uni_mmr: `{risk['min_uni_mmr']}`\n"
            f"warning_uni_mmr: `{risk['warning_uni_mmr']}`\n"
            f"New orders allowed: `{orders_ok}`\n\n"
            f"*Alerts:*\n`{alerts_str}`"
        )
        await self._send_msg(message, ParseMode.MARKDOWN)

    @authorized_only
    async def _pm_recover(self, update: Update, context: CallbackContext) -> None:
        """Handler for /pm_recover."""
        try:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, safe_async_db(self._rpc._rpc_pm_recover))
        except RPCException as e:
            await self._send_msg(str(e))
            return

        mismatches_str = (
            "\n".join(f"  - {m}" for m in result["mismatches"][:10])
            if result["mismatches"]
            else "none"
        )
        errors_str = (
            "\n".join(f"  - {e}" for e in result["errors"][:10]) if result["errors"] else "none"
        )
        message = (
            "*Binance PM Order Recovery*\n"
            f"Open trades: `{result['open_trades']}`\n"
            f"Orders reconciled: `{result['reconciled_count']}`\n\n"
            f"*Mismatches:*\n`{mismatches_str}`\n\n"
            f"*Errors:*\n`{errors_str}`"
        )
        await self._send_msg(message, ParseMode.MARKDOWN)

    @authorized_only
    async def _force_exit_inline(self, update: Update, _: CallbackContext) -> None:
        if update.callback_query:
            query = update.callback_query
            if query.data and "__" in query.data:
                # Input data is "force_exit__<tradid|cancel>"
                trade_id = query.data.split("__")[1].split(" ")[0]
                if trade_id == "cancel":
                    await query.answer()
                    await query.edit_message_text(text="Force exit canceled.")
                    return
                trade: Trade | None = (
                    Trade.get_trades(trade_filter=Trade.id == int(trade_id)).first()
                    if trade_id.isdigit()
                    else None
                )
                await query.answer()
                if trade:
                    await query.edit_message_text(
                        text=f"Manually exiting Trade #{trade_id}, {trade.pair}"
                    )
                    await self._force_exit_action(trade_id)
                else:
                    await query.edit_message_text(text=f"Trade {trade_id} not found.")

    async def _force_enter_action(self, pair, price: float | None, order_side: SignalDirection):
        if pair != "cancel":
            try:

                @safe_async_db
                def _force_enter():
                    self._rpc._rpc_force_entry(pair, price, order_side=order_side)

                loop = asyncio.get_running_loop()
                # Workaround to avoid nested loops
                await loop.run_in_executor(None, _force_enter)
            except RPCException as e:
                logger.exception("Forcebuy error!")
                await self._send_msg(str(e), ParseMode.HTML)

    @authorized_only
    async def _force_enter_inline(self, update: Update, _: CallbackContext) -> None:
        if update.callback_query:
            query = update.callback_query
            if query.data and "__" in query.data:
                # Input data is "force_enter__<pair|cancel>_<side>"
                payload = query.data.split("__")[1]
                if payload == "cancel":
                    await query.answer()
                    await query.edit_message_text(text="Force enter canceled.")
                    return
                if payload and "_||_" in payload:
                    pair, side = payload.split("_||_")
                    order_side = SignalDirection(side)
                    await query.answer()
                    await query.edit_message_text(text=f"Manually entering {order_side} for {pair}")
                    await self._force_enter_action(pair, None, order_side)

    @staticmethod
    def _layout_inline_keyboard(
        buttons: list[InlineKeyboardButton], cols=3
    ) -> list[list[InlineKeyboardButton]]:
        return [buttons[i : i + cols] for i in range(0, len(buttons), cols)]

    @authorized_only
    async def _force_enter(
        self, update: Update, context: CallbackContext, order_side: SignalDirection
    ) -> None:
        """
        Handler for /forcelong <asset> <price> and `/forceshort <asset> <price>
        Buys a pair trade at the given or current price
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        if context.args:
            pair = context.args[0]
            price = float(context.args[1]) if len(context.args) > 1 else None
            await self._force_enter_action(pair, price, order_side)
        else:
            whitelist = self._rpc._rpc_whitelist()["whitelist"]
            pair_buttons = [
                InlineKeyboardButton(
                    text=pair, callback_data=f"force_enter__{pair}_||_{order_side}"
                )
                for pair in sorted(whitelist)
            ]
            buttons_aligned = self._layout_inline_keyboard(pair_buttons)

            buttons_aligned.append(
                [InlineKeyboardButton(text="Cancel", callback_data="force_enter__cancel")]
            )
            await self._send_msg(
                msg="Which pair?", keyboard=buttons_aligned, query=update.callback_query
            )

    @authorized_only
    async def _trades(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /trades <n>
        Returns last n recent trades.
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        stake_cur = self._config["stake_currency"]
        try:
            nrecent = int(context.args[0]) if context.args else 10
        except (TypeError, ValueError, IndexError):
            nrecent = 10
        nonspot = self._config.get("trading_mode", TradingMode.SPOT) != TradingMode.SPOT
        trades = self._rpc._rpc_trade_history(nrecent)
        trades_tab = tabulate(
            [
                [
                    dt_humanize_delta(dt_from_ts(trade["close_timestamp"])),
                    f"{trade['pair']} (#{trade['trade_id']}"
                    f"{(' ' + ('S' if trade['is_short'] else 'L')) if nonspot else ''})",
                    f"{format_pct(trade['close_profit'])} ({trade['close_profit_abs']})",
                ]
                for trade in trades["trades"]
            ],
            headers=[
                "Close Date",
                "Pair (ID L/S)" if nonspot else "Pair (ID)",
                f"Profit ({stake_cur})",
            ],
            tablefmt="simple",
        )
        message = f"<b>{min(trades['trades_count'], nrecent)} recent trades</b>:\n" + (
            f"<pre>{trades_tab}</pre>" if trades["trades_count"] > 0 else ""
        )
        await self._send_msg(message, parse_mode=ParseMode.HTML)

    @authorized_only
    async def _delete_trade(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /delete <id>.
        Delete the given trade
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        if not context.args or len(context.args) == 0:
            raise RPCException("Trade-id not set.")
        trade_id = int(context.args[0])
        msg = self._rpc._rpc_delete(trade_id)
        await self._send_msg(
            f"{msg['result_msg']}\n"
            "Please make sure to take care of this asset on the exchange manually."
        )

    @authorized_only
    async def _cancel_open_order(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /cancel_open_order <id>.
        Cancel open order for tradeid
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        if not context.args or len(context.args) == 0:
            raise RPCException("Trade-id not set.")
        trade_id = int(context.args[0])
        self._rpc._rpc_cancel_open_order(trade_id)
        await self._send_msg("Open order canceled.")

    @authorized_only
    async def _performance(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /performance.
        Shows a performance statistic from finished trades
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        trades = self._rpc._rpc_performance()
        output = "<b>Performance:</b>\n"
        for i, trade in enumerate(trades):
            stat_line = (
                f"{i + 1}.\t <code>{trade['pair']}\t"
                f"{fmt_coin(trade['profit_abs'], self._config['stake_currency'])} "
                f"({format_pct(trade['profit_ratio'])}) "
                f"({trade['count']})</code>\n"
            )

            if len(output + stat_line) >= MAX_MESSAGE_LENGTH:
                await self._send_msg(output, parse_mode=ParseMode.HTML)
                output = stat_line
            else:
                output += stat_line

        await self._send_msg(
            output,
            parse_mode=ParseMode.HTML,
            reload_able=True,
            callback_path="update_performance",
            query=update.callback_query,
        )

    @authorized_only
    async def _enter_tag_performance(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /entries PAIR .
        Shows a performance statistic from finished trades
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        pair = None
        if context.args and isinstance(context.args[0], str):
            pair = context.args[0]

        trades = self._rpc._rpc_enter_tag_performance(pair)
        output = "*Entry Tag Performance:*\n"
        for i, trade in enumerate(trades):
            stat_line = (
                f"{i + 1}.\t `{trade['enter_tag']}\t"
                f"{fmt_coin(trade['profit_abs'], self._config['stake_currency'])} "
                f"({format_pct(trade['profit_ratio'])}) "
                f"({trade['count']})`\n"
            )

            if len(output + stat_line) >= MAX_MESSAGE_LENGTH:
                await self._send_msg(output, parse_mode=ParseMode.MARKDOWN)
                output = stat_line
            else:
                output += stat_line

        await self._send_msg(
            output,
            parse_mode=ParseMode.MARKDOWN,
            reload_able=True,
            callback_path="update_enter_tag_performance",
            query=update.callback_query,
        )

    @authorized_only
    async def _exit_reason_performance(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /exits.
        Shows a performance statistic from finished trades
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        pair = None
        if context.args and isinstance(context.args[0], str):
            pair = context.args[0]

        trades = self._rpc._rpc_exit_reason_performance(pair)
        output = "*Exit Reason Performance:*\n"
        for i, trade in enumerate(trades):
            stat_line = (
                f"{i + 1}.\t `{trade['exit_reason']}\t"
                f"{fmt_coin(trade['profit_abs'], self._config['stake_currency'])} "
                f"({format_pct(trade['profit_ratio'])}) "
                f"({trade['count']})`\n"
            )

            if len(output + stat_line) >= MAX_MESSAGE_LENGTH:
                await self._send_msg(output, parse_mode=ParseMode.MARKDOWN)
                output = stat_line
            else:
                output += stat_line

        await self._send_msg(
            output,
            parse_mode=ParseMode.MARKDOWN,
            reload_able=True,
            callback_path="update_exit_reason_performance",
            query=update.callback_query,
        )

    @authorized_only
    async def _mix_tag_performance(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /mix_tags.
        Shows a performance statistic from finished trades
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        pair = None
        if context.args and isinstance(context.args[0], str):
            pair = context.args[0]

        trades = self._rpc._rpc_mix_tag_performance(pair)
        output = "*Mix Tag Performance:*\n"
        for i, trade in enumerate(trades):
            stat_line = (
                f"{i + 1}.\t `{trade['mix_tag']}\t"
                f"{fmt_coin(trade['profit_abs'], self._config['stake_currency'])} "
                f"({format_pct(trade['profit_ratio'])}) "
                f"({trade['count']})`\n"
            )

            if len(output + stat_line) >= MAX_MESSAGE_LENGTH:
                await self._send_msg(output, parse_mode=ParseMode.MARKDOWN)
                output = stat_line
            else:
                output += stat_line

        await self._send_msg(
            output,
            parse_mode=ParseMode.MARKDOWN,
            reload_able=True,
            callback_path="update_mix_tag_performance",
            query=update.callback_query,
        )

    @authorized_only
    async def _count(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /count.
        Returns the number of trades running
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        counts = self._rpc._rpc_count()
        message = tabulate(
            {k: [v] for k, v in counts.items()},
            headers=["current", "max", "total stake"],
            tablefmt="simple",
        )
        message = f"<pre>{message}</pre>"
        logger.debug(message)
        await self._send_msg(
            message,
            parse_mode=ParseMode.HTML,
            reload_able=True,
            callback_path="update_count",
            query=update.callback_query,
        )

    @authorized_only
    async def _locks(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /locks.
        Returns the currently active locks
        """
        rpc_locks = self._rpc._rpc_locks()
        if not rpc_locks["locks"]:
            await self._send_msg("No active locks.", parse_mode=ParseMode.HTML)

        for locks in chunks(rpc_locks["locks"], 25):
            message = tabulate(
                [
                    [lock["id"], lock["pair"], lock["lock_end_time"], lock["reason"]]
                    for lock in locks
                ],
                headers=["ID", "Pair", "Until", "Reason"],
                tablefmt="simple",
            )
            message = f"<pre>{escape(message)}</pre>"
            logger.debug(message)
            await self._send_msg(message, parse_mode=ParseMode.HTML)

    @authorized_only
    async def _delete_locks(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /delete_locks.
        Returns the currently active locks
        """
        arg = context.args[0] if context.args and len(context.args) > 0 else None
        lockid = None
        pair = None
        if arg:
            try:
                lockid = int(arg)
            except ValueError:
                pair = arg

        self._rpc._rpc_delete_lock(lockid=lockid, pair=pair)
        await self._locks(update, context)

    @authorized_only
    async def _whitelist(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /whitelist
        Shows the currently active whitelist
        """
        whitelist = self._rpc._rpc_whitelist()

        if context.args:
            if "sorted" in context.args:
                whitelist["whitelist"] = sorted(whitelist["whitelist"])
            if "baseonly" in context.args:
                whitelist["whitelist"] = [pair.split("/")[0] for pair in whitelist["whitelist"]]

        message = f"Using whitelist `{whitelist['method']}` with {whitelist['length']} pairs\n"
        message += f"`{', '.join(whitelist['whitelist'])}`"

        logger.debug(message)
        await self._send_msg(message)

    @authorized_only
    async def _blacklist(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /blacklist
        Shows the currently active blacklist
        """
        await self.send_blacklist_msg(self._rpc._rpc_blacklist(context.args))

    async def send_blacklist_msg(self, blacklist: dict):
        errmsgs = []
        for _, error in blacklist["errors"].items():
            errmsgs.append(f"Error: {error['error_msg']}")
        if errmsgs:
            await self._send_msg("\n".join(errmsgs))

        message = f"Blacklist contains {blacklist['length']} pairs\n"
        message += f"`{', '.join(blacklist['blacklist'])}`"

        logger.debug(message)
        await self._send_msg(message)

    @authorized_only
    async def _blacklist_delete(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /bl_delete
        Deletes pair(s) from current blacklist
        """
        await self.send_blacklist_msg(self._rpc._rpc_blacklist_delete(context.args or []))

    @authorized_only
    async def _logs(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /logs
        Shows the latest logs
        """
        try:
            limit = int(context.args[0]) if context.args else 10
        except (TypeError, ValueError, IndexError):
            limit = 10
        logs = RPC._rpc_get_logs(limit)["logs"]
        msgs = ""
        msg_template = "*{}* {}: {} \\- `{}`"
        for logrec in logs:
            msg = msg_template.format(
                escape_markdown(logrec[0], version=2),
                escape_markdown(logrec[2], version=2),
                escape_markdown(logrec[3], version=2),
                escape_markdown(logrec[4], version=2),
            )
            if len(msgs + msg) + 10 >= MAX_MESSAGE_LENGTH:
                # Send message immediately if it would become too long
                await self._send_msg(msgs, parse_mode=ParseMode.MARKDOWN_V2)
                msgs = msg + "\n"
            else:
                # Append message to messages to send
                msgs += msg + "\n"

        if msgs:
            await self._send_msg(msgs, parse_mode=ParseMode.MARKDOWN_V2)

    @authorized_only
    async def _help(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /help.
        Show commands of the bot
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        force_enter_text = (
            "*/forcelong <pair> [<rate>]:* `Instantly buys the given pair. "
            "Optionally takes a rate at which to buy "
            "(only applies to limit orders).` \n"
        )
        if self._rpc._freqtrade.trading_mode != TradingMode.SPOT:
            force_enter_text += (
                "*/forceshort <pair> [<rate>]:* `Instantly shorts the given pair. "
                "Optionally takes a rate at which to sell "
                "(only applies to limit orders).` \n"
            )
        message = (
            "_Bot Control_\n"
            "------------\n"
            "*/start:* `Starts the trader`\n"
            "*/pause:* `Pause the new entries for trader, but handles open trades gracefully`\n"
            "*/stop:* `Stops the trader`\n"
            "*/stopentry:* `Stops entering, but handles open trades gracefully` \n"
            "*/forceexit <trade_id>|all:* `Instantly exits the given trade or all trades, "
            "regardless of profit`\n"
            "*/fx <trade_id>|all:* `Alias to /forceexit`\n"
            "*/pm_close [all|USDT|USDC] CONFIRM:* `Market-closes all futures trades matching "
            "the contract settlement currency. BTC/ETH in PM is collateral only.`\n"
            "*/pm_status:* `Shows Binance PM account status, uniMMR, balances and positions.`\n"
            "*/pm_risk:* `Quick Binance PM risk check with alerts and new-order block status.`\n"
            "*/pm_recover:* `Reconciles PM order states between DB and exchange.`\n"
            f"{force_enter_text if self._config.get('force_entry_enable', False) else ''}"
            "*/delete <trade_id>:* `Instantly delete the given trade in the database`\n"
            "*/reload_trade <trade_id>:* `Reload trade from exchange Orders`\n"
            "*/cancel_open_order <trade_id>:* `Cancels open orders for trade. "
            "Only valid when the trade has open orders.`\n"
            "*/coo <trade_id>|all:* `Alias to /cancel_open_order`\n"
            "*/whitelist [sorted] [baseonly]:* `Show current whitelist. Optionally in "
            "order and/or only displaying the base currency of each pairing.`\n"
            "*/blacklist [pair]:* `Show current blacklist, or adds one or more pairs "
            "to the blacklist.` \n"
            "*/blacklist_delete [pairs]| /bl_delete [pairs]:* "
            "`Delete pair / pattern from blacklist. Will reset on reload_conf.` \n"
            "*/reload_config:* `Reload configuration file` \n"
            "*/unlock <pair|id>:* `Unlock this Pair (or this lock id if it's numeric)`\n"
            "_Current state_\n"
            "------------\n"
            "*/show_config:* `Show running configuration` \n"
            "*/locks:* `Show currently locked pairs`\n"
            "*/balance:* `Show bot managed balance per currency`\n"
            "*/balance total:* `Show account balance per currency`\n"
            "*/logs [limit]:* `Show latest logs - defaults to 10` \n"
            "*/count:* `Show number of active trades compared to allowed number of trades`\n"
            "*/health* `Show latest process timestamp - defaults to 1970-01-01 00:00:00` \n"
            "*/marketdir [long | short | even | none]:* `Updates the user managed variable "
            "that represents the current market direction. If no direction is provided `"
            "`the currently set market direction will be output.` \n"
            "*/list_custom_data <trade_id> <key>:* `List custom_data for Trade ID & Key combo.`\n"
            "`If no Key is supplied it will list all key-value pairs found for that Trade ID.`\n"
            "_Statistics_\n"
            "------------\n"
            "*/status <trade_id>|[table]:* `Lists all open trades`\n"
            "         *<trade_id> :* `Lists one or more specific trades.`\n"
            "                        `Separate multiple <trade_id> with a blank space.`\n"
            "         *table :* `will display trades in a table`\n"
            "                `pending buy orders are marked with an asterisk (*)`\n"
            "                `pending sell orders are marked with a double asterisk (**)`\n"
            "*/entries <pair|none>:* `Shows the enter_tag performance`\n"
            "*/exits <pair|none>:* `Shows the exit reason performance`\n"
            "*/mix_tags <pair|none>:* `Shows combined entry tag + exit reason performance`\n"
            "*/trades [limit]:* `Lists last closed trades (limited to 10 by default)`\n"
            "*/profit [<n>]:* `Lists cumulative profit from all finished trades, "
            "over the last n days`\n"
            "*/profit_long [<n>]:* `Lists cumulative profit from all finished long trades, "
            "over the last n days`\n"
            "*/profit_short [<n>]:* `Lists cumulative profit from all finished short trades, "
            "over the last n days`\n"
            "*/performance:* `Show performance of each finished trade grouped by pair`\n"
            "*/daily <n>:* `Shows profit or loss per day, over the last n days`\n"
            "*/weekly <n>:* `Shows statistics per week, over the last n weeks`\n"
            "*/monthly <n>:* `Shows statistics per month, over the last n months`\n"
            "*/stats:* `Shows Wins / losses by Sell reason as well as "
            "Avg. holding durations for buys and sells.`\n"
            "*/help:* `This help message`\n"
            "*/version:* `Show version`\n"
        )

        await self._send_msg(message, parse_mode=ParseMode.MARKDOWN)

    @authorized_only
    async def _health(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /health
        Shows the last process timestamp
        """
        health = self._rpc.health()
        message = f"Last process: `{health['last_process_loc']}`\n"
        message += f"Initial bot start: `{health['bot_start_loc']}`\n"
        message += f"Last bot restart: `{health['bot_startup_loc']}`"
        await self._send_msg(message)

    @authorized_only
    async def _version(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /version.
        Show version information
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        strategy_version = self._rpc._freqtrade.strategy.version()
        version_string = f"*Version:* `{__version__}`"
        if strategy_version is not None:
            version_string += f"\n*Strategy version: * `{strategy_version}`"

        await self._send_msg(version_string)

    @authorized_only
    async def _show_config(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /show_config.
        Show config information information
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        val = RPC._rpc_show_config(self._config, self._rpc._freqtrade.state)

        if val["trailing_stop"]:
            sl_info = (
                f"*Initial Stoploss:* `{val['stoploss']}`\n"
                f"*Trailing stop positive:* `{val['trailing_stop_positive']}`\n"
                f"*Trailing stop offset:* `{val['trailing_stop_positive_offset']}`\n"
                f"*Only trail above offset:* `{val['trailing_only_offset_is_reached']}`\n"
            )

        else:
            sl_info = f"*Stoploss:* `{val['stoploss']}`\n"

        if val["position_adjustment_enable"]:
            pa_info = (
                f"*Position adjustment:* On\n"
                f"*Max enter position adjustment:* `{val['max_entry_position_adjustment']}`\n"
            )
        else:
            pa_info = "*Position adjustment:* Off\n"

        await self._send_msg(
            f"*Mode:* `{'Dry-run' if val['dry_run'] else 'Live'}`\n"
            f"*Exchange:* `{val['exchange']}`\n"
            f"*Market: * `{val['trading_mode']}`\n"
            f"*Stake per trade:* `{val['stake_amount']} {val['stake_currency']}`\n"
            f"*Max open Trades:* `{val['max_open_trades']}`\n"
            f"*Minimum ROI:* `{val['minimal_roi']}`\n"
            f"*Entry strategy:* ```\n{json.dumps(val['entry_pricing'])}```\n"
            f"*Exit strategy:* ```\n{json.dumps(val['exit_pricing'])}```\n"
            f"{sl_info}"
            f"{pa_info}"
            f"*Timeframe:* `{val['timeframe']}`\n"
            f"*Strategy:* `{val['strategy']}`\n"
            f"*Current state:* `{val['state']}`"
        )

    @authorized_only
    async def _list_custom_data(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /list_custom_data <id> <key>.
        List custom_data for specified trade (and key if supplied).
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        try:
            if not context.args or len(context.args) == 0:
                raise RPCException("Trade-id not set.")
            trade_id = int(context.args[0])
            key = None if len(context.args) < 2 else str(context.args[1])

            results = self._rpc._rpc_list_custom_data(trade_id, key)
            messages = []
            if len(results) > 0:
                trade_custom_data = results[0]["custom_data"]
                messages.append(
                    "Found custom-data entr" + ("ies: " if len(trade_custom_data) > 1 else "y: ")
                )
                for custom_data in trade_custom_data:
                    lines = [
                        f"*Key:* `{custom_data['key']}`",
                        f"*Type:* `{custom_data['type']}`",
                        f"*Value:* `{custom_data['value']}`",
                        f"*Create Date:* `{format_date(custom_data['created_at'])}`",
                        f"*Update Date:* `{format_date(custom_data['updated_at'])}`",
                    ]
                    # Filter empty lines using list-comprehension
                    messages.append("\n".join([line for line in lines if line]))
                for msg in messages:
                    if len(msg) > MAX_MESSAGE_LENGTH:
                        msg = "Message dropped because length exceeds "
                        msg += f"maximum allowed characters: {MAX_MESSAGE_LENGTH}"
                        logger.warning(msg)
                    await self._send_msg(msg)
            else:
                message = f"Didn't find any custom-data entries for Trade ID: `{trade_id}`"
                message += f" and Key: `{key}`." if key is not None else ""
                await self._send_msg(message)

        except RPCException as e:
            await self._send_msg(str(e))

    async def _update_msg(
        self,
        query: CallbackQuery,
        msg: str,
        callback_path: str = "",
        reload_able: bool = False,
        parse_mode: str = ParseMode.MARKDOWN,
    ) -> None:
        if reload_able:
            reply_markup = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Refresh", callback_data=callback_path)],
                ]
            )
        else:
            reply_markup = InlineKeyboardMarkup([[]])
        msg += f"\nUpdated: {datetime.now().ctime()}"
        if not query.message:
            return

        try:
            await query.edit_message_text(
                text=msg, parse_mode=parse_mode, reply_markup=reply_markup
            )
        except BadRequest as e:
            if "not modified" in e.message.lower():
                pass
            else:
                logger.warning("TelegramError: %s", e.message)
        except TelegramError as telegram_err:
            logger.warning("TelegramError: %s! Giving up on that message.", telegram_err.message)

    async def _send_msg(
        self,
        msg: str,
        parse_mode: str = ParseMode.MARKDOWN,
        disable_notification: bool = False,
        keyboard: list[list[InlineKeyboardButton]] | None = None,
        callback_path: str = "",
        reload_able: bool = False,
        query: CallbackQuery | None = None,
    ) -> bool:
        """
        Send given markdown message
        :param msg: message
        :param bot: alternative bot
        :param parse_mode: telegram parse mode
        :return: None
        """
        reply_markup: InlineKeyboardMarkup | ReplyKeyboardMarkup
        if query:
            await self._update_msg(
                query=query,
                msg=msg,
                parse_mode=parse_mode,
                callback_path=callback_path,
                reload_able=reload_able,
            )
            return True
        if reload_able and self._config["telegram"].get("reload", True):
            reply_markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Refresh", callback_data=callback_path)]]
            )
        else:
            if keyboard is not None:
                reply_markup = InlineKeyboardMarkup(keyboard)
            else:
                reply_markup = ReplyKeyboardMarkup(self._keyboard, resize_keyboard=True)
        try:
            try:
                await self._app.bot.send_message(
                    self._config["telegram"]["chat_id"],
                    text=msg,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                    disable_notification=disable_notification,
                    message_thread_id=self._config["telegram"].get("topic_id"),
                )
            except NetworkError as network_err:
                # Sometimes the telegram server resets the current connection,
                # if this is the case we send the message again.
                logger.warning(
                    "Telegram NetworkError: %s! Trying one more time.", network_err.message
                )
                await self._app.bot.send_message(
                    self._config["telegram"]["chat_id"],
                    text=msg,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                    disable_notification=disable_notification,
                    message_thread_id=self._config["telegram"].get("topic_id"),
                )
        except RetryAfter as retry_err:
            self._send_failures += 1
            retry_after = getattr(retry_err, "retry_after", 30)
            if hasattr(retry_after, "total_seconds"):
                retry_after = retry_after.total_seconds()
            try:
                self._last_send_retry_after = max(1.0, float(retry_after))
            except (TypeError, ValueError):
                self._last_send_retry_after = 30.0
            self._last_send_error = (
                f"{retry_err.__class__.__name__}: {retry_err.message}; "
                f"retry_after={self._last_send_retry_after}s"
            )
            logger.warning(
                "Telegram rate limited for %.1fs; durable critical messages remain queued.",
                self._last_send_retry_after,
            )
            return False
        except TelegramError as telegram_err:
            self._send_failures += 1
            self._last_send_retry_after = None
            self._last_send_error = f"{telegram_err.__class__.__name__}: {telegram_err.message}"
            logger.warning(
                "TelegramError: %s! Current attempt failed; durable critical messages remain queued.",
                telegram_err.message,
            )
            return False
        else:
            self._last_sent_at = datetime.now(UTC).isoformat()
            self._last_send_error = None
            self._last_send_retry_after = None
            return True

    @authorized_only
    async def _changemarketdir(self, update: Update, context: CallbackContext) -> None:
        """
        Handler for /marketdir.
        Updates the bot's market_direction
        :param bot: telegram bot
        :param update: message update
        :return: None
        """
        if context.args and len(context.args) == 1:
            new_market_dir_arg = context.args[0]
            old_market_dir = self._rpc._get_market_direction()
            new_market_dir = None
            if new_market_dir_arg == "long":
                new_market_dir = MarketDirection.LONG
            elif new_market_dir_arg == "short":
                new_market_dir = MarketDirection.SHORT
            elif new_market_dir_arg == "even":
                new_market_dir = MarketDirection.EVEN
            elif new_market_dir_arg == "none":
                new_market_dir = MarketDirection.NONE

            if new_market_dir is not None:
                self._rpc._update_market_direction(new_market_dir)
                await self._send_msg(
                    "Successfully updated market direction"
                    f" from *{old_market_dir}* to *{new_market_dir}*."
                )
            else:
                raise RPCException(
                    "Invalid market direction provided. \n"
                    "Valid market directions: *long, short, even, none*"
                )
        elif context.args is not None and len(context.args) == 0:
            old_market_dir = self._rpc._get_market_direction()
            await self._send_msg(f"Currently set market direction: *{old_market_dir}*")
        else:
            raise RPCException(
                "Invalid usage of command /marketdir. \n"
                "Usage: */marketdir [short |  long | even | none]*"
            )

    async def _tg_info(self, update: Update, context: CallbackContext) -> None:
        """
        Intentionally unauthenticated Handler for /tg_info.
        Returns information about the current telegram chat - even if chat_id does not
        correspond to this chat.

        :param update: message update
        :return: None
        """
        if not update.message:
            return
        chat_id = update.message.chat_id
        topic_id = update.message.message_thread_id
        user_id = (
            update.effective_user.id if topic_id is not None and update.effective_user else None
        )

        msg = f"""Freqtrade Bot Info:
        ```json
            {{
                "enabled": true,
                "token": "********",
                "chat_id": "{chat_id}",
                {f'"topic_id": "{topic_id}",' if topic_id else ""}
                {f'//"authorized_users": ["{user_id}"]' if topic_id and user_id else ""}
            }}
        ```
        """
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode=ParseMode.MARKDOWN_V2,
                message_thread_id=topic_id,
            )
        except TelegramError as telegram_err:
            logger.warning("TelegramError: %s! Giving up on that message.", telegram_err.message)
