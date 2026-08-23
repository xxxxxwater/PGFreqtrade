"""
This module manages webhook communication
"""

import logging
import queue
import threading
import time
from typing import Any

from requests import RequestException, post

from freqtrade.constants import Config
from freqtrade.enums import RPCMessageType
from freqtrade.rpc import RPC, RPCHandler
from freqtrade.rpc.rpc_types import RPCSendMsg


logger = logging.getLogger(__name__)

logger.debug("Included module rpc.webhook ...")


class Webhook(RPCHandler):
    """This class handles all webhook communication"""

    def __init__(self, rpc: RPC, config: Config) -> None:
        """
        Init the Webhook class, and init the super class RPCHandler
        :param rpc: instance of RPC Helper class
        :param config: Configuration object
        :return: None
        """
        super().__init__(rpc, config)

        self._url = self._config["webhook"]["url"]
        self._format = self._config["webhook"].get("format", "form")
        self._retries = self._config["webhook"].get("retries", 0)
        self._retry_delay = self._config["webhook"].get("retry_delay", 0.1)
        self._timeout = self._config["webhook"].get("timeout", 10)
        self._init_sender_queue(self._config["webhook"].get("queue_maxsize", 100))

    def _init_sender_queue(self, queue_maxsize: int = 100) -> None:
        """
        Start the background sender queue. Outbound HTTP is sent on a dedicated daemon
        thread so synchronous requests (with their timeout/retries) never block the
        trading main loop. Also used by the Discord subclass.
        """
        self._queue_maxsize = int(queue_maxsize)
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=self._queue_maxsize)
        self._stop_event = threading.Event()
        self._sender_thread = threading.Thread(
            name="webhook_sender", target=self._sender_loop, daemon=True
        )
        self._sender_thread.start()

        self._sent = 0
        self._failed = 0
        self._dropped = 0

    def cleanup(self) -> None:
        """
        Stop the background sender thread. Pending queued messages are flushed
        synchronously first, then the thread is stopped.
        """
        self._drain_queue()
        self._stop_event.set()
        if self._sender_thread and self._sender_thread.is_alive():
            self._sender_thread.join(timeout=self._timeout + 1)
        self._sender_thread = None  # type: ignore[assignment]

    def _drain_queue(self) -> None:
        """Synchronously send all currently queued messages (tests and shutdown)."""
        while True:
            try:
                payload = self._queue.get_nowait()
            except queue.Empty:
                return
            try:
                self._send_msg(payload)
                self._sent += 1
            except Exception:
                self._failed += 1
                logger.exception("Failed to send webhook message.")

    def _enqueue(self, payload: dict[str, Any]) -> None:
        """Queue a payload for the background sender; drop + count when full."""
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            self._dropped += 1
            logger.warning(
                "Webhook queue full (%d); dropping message. Total dropped: %d",
                self._queue_maxsize,
                self._dropped,
            )

    def _sender_loop(self) -> None:
        """Background worker: drain the queue and send each payload."""
        while not self._stop_event.is_set():
            try:
                payload = self._queue.get(timeout=1)
            except queue.Empty:
                continue
            try:
                self._send_msg(payload)
                self._sent += 1
            except Exception:
                self._failed += 1
                logger.exception("Failed to send webhook message.")

    def health(self) -> dict[str, Any]:
        """Health/status for the API and bot health endpoints."""
        return {
            "module": self.name,
            "queue_size": self._queue.qsize(),
            "queue_maxsize": self._queue_maxsize,
            "sent": self._sent,
            "failed": self._failed,
            "dropped": self._dropped,
        }

    def _get_value_dict(self, msg: RPCSendMsg) -> dict[str, Any] | None:
        whconfig = self._config["webhook"]
        if msg["type"].value in whconfig:
            # Explicit types should have priority
            valuedict = whconfig.get(msg["type"].value)
        # The below is deprecated 2022.10 - only keep generic method.
        elif msg["type"] in [RPCMessageType.ENTRY]:
            valuedict = whconfig.get("webhookentry")
        elif msg["type"] in [RPCMessageType.ENTRY_CANCEL]:
            valuedict = whconfig.get("webhookentrycancel")
        elif msg["type"] in [RPCMessageType.ENTRY_FILL]:
            valuedict = whconfig.get("webhookentryfill")
        elif msg["type"] == RPCMessageType.EXIT:
            valuedict = whconfig.get("webhookexit")
        elif msg["type"] == RPCMessageType.EXIT_FILL:
            valuedict = whconfig.get("webhookexitfill")
        elif msg["type"] == RPCMessageType.EXIT_CANCEL:
            valuedict = whconfig.get("webhookexitcancel")
        elif msg["type"] in (
            RPCMessageType.STATUS,
            RPCMessageType.STARTUP,
            RPCMessageType.EXCEPTION,
            RPCMessageType.WARNING,
        ):
            valuedict = whconfig.get("webhookstatus")
        elif msg["type"] in (
            RPCMessageType.PROTECTION_TRIGGER,
            RPCMessageType.PROTECTION_TRIGGER_GLOBAL,
            RPCMessageType.WHITELIST,
            RPCMessageType.ANALYZED_DF,
            RPCMessageType.NEW_CANDLE,
            RPCMessageType.STRATEGY_MSG,
        ):
            # Don't fail for non-implemented types
            return None
        return valuedict

    def recursive_format(self, obj: dict | list | str, msg: RPCSendMsg):
        """
        Format the given object using the provided message.
        """
        match obj:
            case dict():
                return {k: self.recursive_format(v, msg) for k, v in obj.items()}
            case list():
                return [self.recursive_format(item, msg) for item in obj]
            case str():
                return obj.format(**msg)
            case _:
                return obj

    def send_msg(self, msg: RPCSendMsg) -> None:
        """Queue a message for asynchronous delivery to the webhook."""
        try:
            valuedict = self._get_value_dict(msg)

            if not valuedict:
                logger.debug("Message type '%s' not configured for webhooks", msg["type"])
                return

            payload = self.recursive_format(valuedict, msg)
            self._enqueue(payload)
        except KeyError as exc:
            logger.error(
                "Problem calling Webhook. Please check your webhook configuration. Exception: %s",
                exc,
            )

    def _send_msg(self, payload: dict) -> None:
        """do the actual call to the webhook"""

        success = False
        attempts = 0
        while not success and attempts <= self._retries:
            if attempts:
                if self._retry_delay:
                    time.sleep(self._retry_delay)
                logger.info("Retrying webhook...")

            attempts += 1

            try:
                if self._format == "form":
                    response = post(self._url, data=payload, timeout=self._timeout)
                elif self._format == "json":
                    response = post(self._url, json=payload, timeout=self._timeout)
                elif self._format == "raw":
                    response = post(
                        self._url,
                        data=payload["data"],
                        headers={"Content-Type": "text/plain"},
                        timeout=self._timeout,
                    )
                else:
                    raise NotImplementedError(f"Unknown format: {self._format}")

                # Throw a RequestException if the post was not successful
                response.raise_for_status()
                success = True

            except RequestException as exc:
                logger.warning("Could not call webhook url. Exception: %s", exc)
