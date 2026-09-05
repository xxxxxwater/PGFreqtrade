import asyncio
import logging
import time
from copy import deepcopy
from concurrent.futures import Future
from functools import partial
from threading import Thread

import ccxt

from freqtrade.constants import Config, PairWithTimeframe
from freqtrade.enums.candletype import CandleType
from freqtrade.exceptions import TemporaryError
from freqtrade.exchange.common import retrier
from freqtrade.exchange.exchange import timeframe_to_seconds
from freqtrade.exchange.exchange_types import OHLCVResponse
from freqtrade.util import dt_ts, format_ms_time, format_ms_time_det


logger = logging.getLogger(__name__)


class ExchangeWS:
    WATCH_TIMEOUT = 60.0
    CLOSE_TIMEOUT = 5.0
    RETRY_DELAY = 5.0

    def __init__(self, config: Config, ccxt_object: ccxt.Exchange) -> None:
        self.config = config
        self._ccxt_object = ccxt_object
        self._background_tasks: set[asyncio.Task] = set()

        self._klines_watching: set[PairWithTimeframe] = set()
        self._klines_scheduled: set[PairWithTimeframe] = set()
        self.klines_last_refresh: dict[PairWithTimeframe, float] = {}
        self.klines_last_request: dict[PairWithTimeframe, float] = {}
        self._retry_after: dict[PairWithTimeframe, float] = {}
        self._reset_future: Future | None = None
        self._resetting = False
        self._shutdown = False
        # Create the loop before publishing the object to the main thread.
        self._loop = asyncio.new_event_loop()
        self._thread = Thread(name="ccxt_ws", target=self._start_forever, daemon=True)
        self._thread.start()

    def _start_forever(self) -> None:
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        finally:
            self._loop.close()

    def cleanup(self) -> None:
        logger.debug("Cleanup called - stopping")
        self._shutdown = True
        if hasattr(self, "_loop") and not self._loop.is_closed():
            if self._loop.is_running():
                self.reset_connections()
                if self._reset_future:
                    try:
                        self._reset_future.result(timeout=self.CLOSE_TIMEOUT * 3)
                    except TimeoutError:
                        logger.error("WS cleanup timed out; background loop is not responsive.")
                self._loop.call_soon_threadsafe(self._loop.stop)
            else:
                self._loop.close()
        self._thread.join(timeout=self.CLOSE_TIMEOUT)
        logger.debug("Stopped")

    def reset_connections(self) -> None:
        """
        Reset on the owning loop without blocking REST refresh or order management.
        """
        if self._reset_future and not self._reset_future.done():
            return
        if hasattr(self, "_loop") and not self._loop.is_closed():
            logger.info("Resetting WS connections.")
            self._resetting = True
            self._reset_future = asyncio.run_coroutine_threadsafe(
                self._cleanup_async(), loop=self._loop
            )

    async def _cleanup_async(self) -> None:
        try:
            # Cancel and join watchers BEFORE closing CCXT. Their finalizers must
            # not send UNSUBSCRIBE through a freshly reopened connection.
            tasks = list(self._background_tasks)
            for task in tasks:
                task.cancel()
            if tasks:
                _, pending = await asyncio.wait(tasks, timeout=self.CLOSE_TIMEOUT)
                if pending:
                    raise TimeoutError("WS watcher cancellation did not complete")
            await asyncio.wait_for(self._ccxt_object.close(), timeout=self.CLOSE_TIMEOUT)
            self._ccxt_object.ohlcvs.clear()
            self.klines_last_refresh.clear()
            self._retry_after.clear()
            self._resetting = False
            if self._shutdown:
                self._klines_watching.clear()
            else:
                await self._schedule_while_true()
        except Exception:
            # Keep WS unusable on failed cleanup; the independent REST client
            # stays available and a later reset can retry the close.
            logger.exception("Exception in _cleanup_async")

    def _pop_history(self, paircomb: PairWithTimeframe) -> None:
        """
        Remove history for a pair/timeframe combination from ccxt cache
        """
        self._ccxt_object.ohlcvs.get(paircomb[0], {}).pop(paircomb[1], None)
        self.klines_last_refresh.pop(paircomb, None)

    @retrier(retries=3)
    def ohlcvs(self, pair: str, timeframe: str) -> list[list]:
        """
        Returns a copy of the klines for a pair/timeframe combination
        Note: this will only contain the data received from the websocket
            so the data will build up over time.
        """
        try:
            if self._resetting or self._shutdown:
                return []
            return deepcopy(self._ccxt_object.ohlcvs.get(pair, {}).get(timeframe, []))
        except RuntimeError as e:
            # Capture runtime errors and retry
            # TemporaryError does not cause backoff - so we're essentially retrying immediately
            raise TemporaryError(f"Error deepcopying: {e}") from e

    def cleanup_expired(self) -> None:
        """
        Remove pairs from watchlist if they've not been requested within
        the last timeframe (+ offset)
        """
        changed = False
        for p in list(self._klines_watching):
            _, timeframe, _ = p
            timeframe_s = timeframe_to_seconds(timeframe)
            last_refresh = self.klines_last_request.get(p, 0)
            if last_refresh > 0 and (dt_ts() - last_refresh) > ((timeframe_s + 20) * 1000):
                logger.info(f"Removing {p} from websocket watchlist.")
                self._klines_watching.discard(p)
                # Pop history to avoid getting stale data
                self._pop_history(p)
                changed = True
        if changed:
            logger.info(f"Removal done: new watch list ({len(self._klines_watching)})")

    async def _schedule_while_true(self) -> None:
        if self._resetting or self._shutdown:
            return
        # For the ones we should be watching
        for p in list(self._klines_watching):
            # Check if they're already scheduled
            if p not in self._klines_scheduled and time.monotonic() >= self._retry_after.get(p, 0):
                self._klines_scheduled.add(p)
                pair, timeframe, candle_type = p
                task = asyncio.create_task(
                    self._continuously_async_watch_ohlcv(pair, timeframe, candle_type)
                )
                self._background_tasks.add(task)
                task.add_done_callback(
                    partial(
                        self._continuous_stopped,
                        pair=pair,
                        timeframe=timeframe,
                        candle_type=candle_type,
                    )
                )

    async def _unwatch_ohlcv(self, pair: str, timeframe: str, candle_type: CandleType) -> None:
        try:
            await asyncio.wait_for(
                self._ccxt_object.un_watch_ohlcv_for_symbols([[pair, timeframe]]),
                timeout=self.CLOSE_TIMEOUT,
            )
        except ccxt.NotSupported as e:
            logger.debug("un_watch_ohlcv_for_symbols not supported: %s", e)
        except TimeoutError:
            logger.warning("WS unsubscribe timed out for %s, %s; resetting transport.", pair, timeframe)
            self.reset_connections()
        except Exception:
            logger.exception("Exception in _unwatch_ohlcv")

    def _continuous_stopped(
        self, task: asyncio.Task, pair: str, timeframe: str, candle_type: CandleType
    ):
        self._background_tasks.discard(task)
        result = "done"
        if task.cancelled():
            result = "cancelled"
        else:
            if error := task.exception():
                logger.error("WS task failed for %s, %s: %s", pair, timeframe, type(error).__name__)
                result = "error"

        logger.info(f"{pair}, {timeframe}, {candle_type} - Task finished - {result}")
        # The task owns its bounded unsubscribe. Do not release the scheduling
        # slot until it finishes, or a late unsubscribe can kill its replacement.
        self._klines_scheduled.discard((pair, timeframe, candle_type))
        self._pop_history((pair, timeframe, candle_type))

    @staticmethod
    def watch_timeout_for(timeframe: str) -> float:
        """
        Watchdog per timeframe.

        A quiet pair only receives a frame at candle close: a 5m pair can
        legitimately wait ~5 minutes between frames.  Scale the watchdog so a
        normally-silent market is not mistaken for a dead transport (which
        would re-subscribe every minute and flood the log).  The 60s floor
        keeps short timeframes snappy.
        """
        return max(ExchangeWS.WATCH_TIMEOUT, timeframe_to_seconds(timeframe) * 2 + 30)

    async def _continuously_async_watch_ohlcv(
        self, pair: str, timeframe: str, candle_type: CandleType
    ) -> None:
        try:
            while (pair, timeframe, candle_type) in self._klines_watching:
                start = dt_ts()
                data = await asyncio.wait_for(
                    self._ccxt_object.watch_ohlcv(pair, timeframe),
                    timeout=self.watch_timeout_for(timeframe),
                )
                if data:
                    if (pair, timeframe, candle_type) not in self.klines_last_refresh:
                        logger.info("WS candle data received for %s, %s.", pair, timeframe)
                    self.klines_last_refresh[(pair, timeframe, candle_type)] = dt_ts()
                logger.debug(
                    f"watch done {pair}, {timeframe}, data {len(data)} "
                    f"in {(dt_ts() - start) / 1000:.3f}s"
                )
                # Cached/immediately-resolved futures must not starve resets,
                # ping/pong handling or other subscriptions on this event loop.
                await asyncio.sleep(0.01)
        except TimeoutError:
            logger.warning("WS candle wait timed out for %s, %s; REST fallback active.", pair, timeframe)
        except ccxt.ExchangeClosedByUser:
            logger.debug("Exchange connection closed by user")
        except ccxt.BaseError:
            logger.exception(f"Exception in continuously_async_watch_ohlcv for {pair}, {timeframe}")
        finally:
            self.klines_last_refresh.pop((pair, timeframe, candle_type), None)
            self._retry_after[(pair, timeframe, candle_type)] = time.monotonic() + self.RETRY_DELAY
            if not self._resetting and not self._shutdown:
                await self._unwatch_ohlcv(pair, timeframe, candle_type)

    def schedule_ohlcv(self, pair: str, timeframe: str, candle_type: CandleType) -> None:
        """
        Schedule a pair/timeframe combination to be watched
        """
        self.schedule_ohlcvs([(pair, timeframe, candle_type)])

    def schedule_ohlcvs(self, pairs: list[PairWithTimeframe]) -> None:
        """Renew the entire requested batch before expiring previous subscriptions."""
        if not self._shutdown and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._register_pairs, list(pairs))

    def _register_pairs(self, pairs: list[PairWithTimeframe]) -> None:
        if self._shutdown:
            return
        now = dt_ts()
        for paircomb in pairs:
            self._klines_watching.add(paircomb)
            self.klines_last_request[paircomb] = now
        self.cleanup_expired()
        task = self._loop.create_task(self._schedule_while_true())
        # This coroutine never suspends and cannot race a reset on the same loop.
        task.add_done_callback(self._schedule_done)

    @staticmethod
    def _schedule_done(task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception():
            logger.error("WS scheduling failed: %s", type(task.exception()).__name__)

    async def get_ohlcv(
        self,
        pair: str,
        timeframe: str,
        candle_type: CandleType,
        candle_ts: int,
    ) -> OHLCVResponse:
        """
        Returns cached klines from ccxt's "watch" cache.
        :param candle_ts: timestamp of the end-time of the candle we expect.
        """
        # Deepcopy the response - as it might be modified in the background as new messages arrive
        candles = self.ohlcvs(pair, timeframe)
        refresh_date = self.klines_last_refresh.get((pair, timeframe, candle_type), 0)
        received_ts = candles[-1][0] if candles else 0
        drop_hint = received_ts >= candle_ts
        if received_ts > refresh_date:
            logger.warning(
                f"{pair}, {timeframe} - Candle date > last refresh "
                f"({format_ms_time(received_ts)} > {format_ms_time_det(refresh_date)}). "
                "This usually suggests a problem with time synchronization."
            )
        logger.debug(
            f"watch result for {pair}, {timeframe} with length {len(candles)}, "
            f"r_ts={format_ms_time(received_ts)}, "
            f"lref={format_ms_time_det(refresh_date)}, "
            f"candle_ts={format_ms_time(candle_ts)}, {drop_hint=}"
        )
        return pair, timeframe, candle_type, candles, drop_hint
