"""Binance Portfolio Margin user data stream consumer."""

import asyncio
import json
import logging
from collections import deque
from datetime import UTC, datetime
from threading import Event, RLock, Thread
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed


logger = logging.getLogger(__name__)


class BinancePMUserStream:
    """
    Lightweight Binance PM account stream.

    The stream thread only receives and queues events.  Database and trade-state changes are
    handled by the bot's main thread when it drains the queue.
    """

    _BASE_URL = "wss://fstream.binance.com/pm/ws"

    def __init__(
        self,
        listen_key: str,
        *,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
        receive_timeout: float = 20.0,
        ping_timeout: float = 10.0,
        max_queue_size: int = 2000,
        max_message_size: int = 2**20,
        base_url: str = _BASE_URL,
    ) -> None:
        self._listen_key = listen_key
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._next_reconnect_delay = reconnect_delay
        self._last_reconnect_delay: float | None = None
        self._receive_timeout = receive_timeout
        self._ping_timeout = ping_timeout
        self._max_message_size = max_message_size
        self._base_url = base_url.rstrip("/")

        self._events: deque[dict[str, Any]] = deque(maxlen=max_queue_size)
        self._state_lock = RLock()
        self._stop_event = Event()
        self._thread: Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._main_task: asyncio.Task | None = None

        self._connected = False
        self._events_received = 0
        self._events_dropped = 0
        self._reconnects = 0
        self._parse_errors = 0
        self._last_event_type: str | None = None
        self._last_event_time: str | None = None
        self._last_connected_at: str | None = None
        self._last_disconnected_at: str | None = None
        self._last_error: str | None = None

    @property
    def listen_key(self) -> str:
        return self._listen_key

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = Thread(
            name="binance_pm_user_stream",
            target=self._thread_main,
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._state_lock:
            loop = self._loop
            task = self._main_task
        if loop is not None and task is not None and not task.done():
            loop.call_soon_threadsafe(task.cancel)
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5)
            if thread.is_alive():
                logger.warning(
                    "Binance PM user stream thread did not stop within timeout; "
                    "event loop may not be closed."
                )
                return
        self._thread = None
        self._loop = None
        self._main_task = None
        with self._state_lock:
            self._connected = False

    def pop_events(self, max_events: int = 1000) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        with self._state_lock:
            while self._events and len(events) < max_events:
                events.append(self._events.popleft())
        return events

    def stats(self) -> dict[str, Any]:
        with self._state_lock:
            return {
                "enabled": True,
                "running": bool(self._thread and self._thread.is_alive()),
                "connected": self._connected,
                "listen_key_set": bool(self._listen_key),
                "queued_events": len(self._events),
                "events_received": self._events_received,
                "events_dropped": self._events_dropped,
                "reconnects": self._reconnects,
                "next_reconnect_delay": self._next_reconnect_delay,
                "last_reconnect_delay": self._last_reconnect_delay,
                "parse_errors": self._parse_errors,
                "last_event_type": self._last_event_type,
                "last_event_time": self._last_event_time,
                "last_connected_at": self._last_connected_at,
                "last_disconnected_at": self._last_disconnected_at,
                "last_error": self._last_error,
            }

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._main_task = loop.create_task(self._run_forever())
        try:
            loop.run_until_complete(self._main_task)
        except asyncio.CancelledError:
            pass
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    async def _run_forever(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._consume_connection()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._record_disconnect(error=f"{e.__class__.__name__}: {e}")
                logger.warning(f"Binance PM user stream disconnected: {e}")

            if not self._stop_event.is_set():
                delay = self._record_reconnect()
                await asyncio.sleep(delay)

    async def _consume_connection(self) -> None:
        async with websockets.connect(
            f"{self._base_url}/{self._listen_key}",
            ping_interval=None,
            max_size=self._max_message_size,
        ) as ws:
            self._record_connect()
            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=self._receive_timeout)
                    self._handle_message(message)
                except TimeoutError:
                    try:
                        pong = await ws.ping()
                        await asyncio.wait_for(pong, timeout=self._ping_timeout)
                    except (ConnectionClosed, OSError) as e:
                        self._record_disconnect(error=f"Ping failed: {e}")
                        break
                except ConnectionClosed as e:
                    self._record_disconnect(error=f"ConnectionClosed: {e}")
                    break

    def _handle_message(self, message: str | bytes) -> None:
        if isinstance(message, bytes):
            message = message.decode("utf-8")
        try:
            event = json.loads(message)
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            with self._state_lock:
                self._parse_errors += 1
                self._last_error = f"{e.__class__.__name__}: {e}"
            logger.warning("Could not decode Binance PM user stream event.")
            return

        event_type = str(event.get("e") or "unknown")
        event_time = event.get("E")
        if event_time is None:
            event_time = event.get("T")
        with self._state_lock:
            if len(self._events) == self._events.maxlen:
                self._events_dropped += 1
            self._events.append(event)
            self._events_received += 1
            self._last_event_type = event_type
            self._last_event_time = str(event_time) if event_time is not None else self._now()

    def _record_connect(self) -> None:
        with self._state_lock:
            self._connected = True
            self._next_reconnect_delay = self._reconnect_delay
            self._last_connected_at = self._now()
            self._last_error = None
        logger.info("Binance PM user data stream connected.")

    def _record_disconnect(self, *, error: str | None = None) -> None:
        with self._state_lock:
            self._connected = False
            self._last_disconnected_at = self._now()
            self._last_error = error

    def _record_reconnect(self) -> float:
        with self._state_lock:
            self._reconnects += 1
            delay = self._next_reconnect_delay
            self._last_reconnect_delay = delay
            self._next_reconnect_delay = min(delay * 2, self._max_reconnect_delay)
            return delay

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()
