import asyncio
import logging
import threading
import time
from datetime import timedelta
from time import sleep
from unittest.mock import AsyncMock, MagicMock

from ccxt import NotSupported

from freqtrade.enums import CandleType
from freqtrade.exceptions import TemporaryError
from freqtrade.exchange.exchange import timeframe_to_msecs
from freqtrade.exchange.exchange_ws import ExchangeWS
from ft_client.test_client.test_rest_client import log_has_re
from tests.conftest import get_patched_exchange


def test_exchangews_init(mocker):
    config = MagicMock()
    ccxt_object = MagicMock()
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())

    exchange_ws = ExchangeWS(config, ccxt_object)
    sleep(0.1)

    assert exchange_ws.config == config
    assert exchange_ws._ccxt_object == ccxt_object
    assert exchange_ws._thread.name == "ccxt_ws"
    assert exchange_ws._background_tasks == set()
    assert exchange_ws._klines_watching == set()
    assert exchange_ws._klines_scheduled == set()
    assert exchange_ws.klines_last_refresh == {}
    assert exchange_ws.klines_last_request == {}
    # Cleanup
    exchange_ws.cleanup()


def test_exchangews_cleanup_error(mocker, caplog):
    config = MagicMock()
    ccxt_object = MagicMock()
    ccxt_object.close = AsyncMock(side_effect=Exception("Test"))
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())

    exchange_ws = ExchangeWS(config, ccxt_object)
    patch_eventloop_threading(exchange_ws)

    sleep(0.1)
    exchange_ws.reset_connections()

    # The reset now runs on the loop thread without blocking the caller;
    # wait for the future to settle before asserting the logged error.
    try:
        exchange_ws._reset_future.result(timeout=2.0)
    except Exception:
        pass
    assert log_has_re("Exception in _cleanup_async", caplog)

    exchange_ws.cleanup()


def patch_eventloop_threading(exchange):
    init_event = threading.Event()

    def thread_func():
        exchange._loop = asyncio.new_event_loop()
        init_event.set()
        exchange._loop.run_forever()

    x = threading.Thread(target=thread_func, daemon=True)
    x.start()
    # Wait for thread to be properly initialized with timeout
    if not init_event.wait(timeout=5.0):
        raise RuntimeError("Failed to initialize event loop thread")


async def test_exchangews_ohlcv(mocker, time_machine, caplog):
    config = MagicMock()
    ccxt_object = MagicMock()
    caplog.set_level(logging.DEBUG)

    async def controlled_sleeper(*args, **kwargs):
        # Sleep to pass control back to the event loop
        await asyncio.sleep(0.1)
        return MagicMock()

    async def wait_for_condition(condition_func, timeout_=5.0, check_interval=0.01):
        """Wait for a condition to be true with timeout."""
        try:
            async with asyncio.timeout(timeout_):
                while True:
                    if condition_func():
                        return True
                    await asyncio.sleep(check_interval)
        except TimeoutError:
            return False

    ccxt_object.un_watch_ohlcv_for_symbols = AsyncMock(side_effect=[NotSupported, ValueError])
    ccxt_object.watch_ohlcv = AsyncMock(side_effect=controlled_sleeper)
    ccxt_object.close = AsyncMock()
    time_machine.move_to("2024-11-01 01:00:02 +00:00")

    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())

    exchange_ws = ExchangeWS(config, ccxt_object)
    patch_eventloop_threading(exchange_ws)
    try:
        assert exchange_ws._klines_watching == set()
        assert exchange_ws._klines_scheduled == set()

        exchange_ws.schedule_ohlcv("ETH/BTC", "1m", CandleType.SPOT)
        exchange_ws.schedule_ohlcv("XRP/BTC", "1m", CandleType.SPOT)

        # Wait for both pairs to be properly scheduled and watching
        await wait_for_condition(
            lambda: (
                len(exchange_ws._klines_watching) == 2 and len(exchange_ws._klines_scheduled) == 2
            ),
            timeout_=2.0,
        )

        assert exchange_ws._klines_watching == {
            ("ETH/BTC", "1m", CandleType.SPOT),
            ("XRP/BTC", "1m", CandleType.SPOT),
        }
        assert exchange_ws._klines_scheduled == {
            ("ETH/BTC", "1m", CandleType.SPOT),
            ("XRP/BTC", "1m", CandleType.SPOT),
        }

        # Wait for the expected number of watch calls
        await wait_for_condition(lambda: ccxt_object.watch_ohlcv.call_count >= 6, timeout_=3.0)
        assert ccxt_object.watch_ohlcv.call_count >= 6
        ccxt_object.watch_ohlcv.reset_mock()

        time_machine.shift(timedelta(minutes=5))
        exchange_ws.schedule_ohlcv("ETH/BTC", "1m", CandleType.SPOT)

        # Wait for log message
        await wait_for_condition(
            lambda: log_has_re("un_watch_ohlcv_for_symbols not supported: ", caplog), timeout_=2.0
        )
        assert log_has_re("un_watch_ohlcv_for_symbols not supported: ", caplog)

        # XRP/BTC should be cleaned up.
        assert exchange_ws._klines_watching == {
            ("ETH/BTC", "1m", CandleType.SPOT),
        }

        # Cleanup happened.
        exchange_ws.schedule_ohlcv("ETH/BTC", "1m", CandleType.SPOT)

        # Verify final state
        assert exchange_ws._klines_watching == {
            ("ETH/BTC", "1m", CandleType.SPOT),
        }
        assert exchange_ws._klines_scheduled == {
            ("ETH/BTC", "1m", CandleType.SPOT),
        }

    finally:
        # Cleanup
        exchange_ws.cleanup()
    # Only the XRP eviction unsubscribed. A reset deliberately skips
    # unsubscribing (late ACKs must never reach a fresh connection), so the
    # second side-effect (ValueError) is never triggered.
    assert ccxt_object.un_watch_ohlcv_for_symbols.call_count == 1
    assert not log_has_re("Exception in _unwatch_ohlcv", caplog)


async def test_exchangews_get_ohlcv(mocker, caplog):
    config = MagicMock()
    ccxt_object = MagicMock()
    ccxt_object.ohlcvs = {
        "ETH/USDT": {
            "1m": [
                [1635840000000, 100, 200, 300, 400, 500],
                [1635840060000, 101, 201, 301, 401, 501],
                [1635840120000, 102, 202, 302, 402, 502],
            ],
            "5m": [
                [1635840000000, 100, 200, 300, 400, 500],
                [1635840300000, 105, 201, 301, 401, 501],
                [1635840600000, 102, 202, 302, 402, 502],
            ],
        }
    }
    mocker.patch("freqtrade.exchange.exchange_ws.ExchangeWS._start_forever", MagicMock())

    exchange_ws = ExchangeWS(config, ccxt_object)
    exchange_ws.klines_last_refresh = {
        ("ETH/USDT", "1m", CandleType.SPOT): 1635840120000,
        ("ETH/USDT", "5m", CandleType.SPOT): 1635840600000,
    }

    # Matching last candle time - drop hint is true
    resp = await exchange_ws.get_ohlcv("ETH/USDT", "1m", CandleType.SPOT, 1635840120000)
    assert resp[0] == "ETH/USDT"
    assert resp[1] == "1m"
    assert resp[3] == [
        [1635840000000, 100, 200, 300, 400, 500],
        [1635840060000, 101, 201, 301, 401, 501],
        [1635840120000, 102, 202, 302, 402, 502],
    ]
    assert resp[4] is True

    # expected time > last candle time - drop hint is false
    resp = await exchange_ws.get_ohlcv("ETH/USDT", "1m", CandleType.SPOT, 1635840180000)
    assert resp[0] == "ETH/USDT"
    assert resp[1] == "1m"
    assert resp[3] == [
        [1635840000000, 100, 200, 300, 400, 500],
        [1635840060000, 101, 201, 301, 401, 501],
        [1635840120000, 102, 202, 302, 402, 502],
    ]
    assert resp[4] is False

    # Change "received" times to be before the candle starts.
    # This should trigger the "time sync" warning.
    exchange_ws.klines_last_refresh = {
        ("ETH/USDT", "1m", CandleType.SPOT): 1635840110000,
        ("ETH/USDT", "5m", CandleType.SPOT): 1635840600000,
    }
    msg = r".*Candle date > last refresh.*"
    assert not log_has_re(msg, caplog)
    resp = await exchange_ws.get_ohlcv("ETH/USDT", "1m", CandleType.SPOT, 1635840120000)
    assert resp[0] == "ETH/USDT"
    assert resp[1] == "1m"
    assert resp[3] == [
        [1635840000000, 100, 200, 300, 400, 500],
        [1635840060000, 101, 201, 301, 401, 501],
        [1635840120000, 102, 202, 302, 402, 502],
    ]
    assert resp[4] is True

    assert log_has_re(msg, caplog)

    exchange_ws.cleanup()


# ---- WS resilience regression tests (silent transport, batch renewal, resets) ----


def test_watch_timeout_scales_with_timeframe():
    """Quiet pairs wait for candle-close frames: the watchdog must scale."""
    assert ExchangeWS.watch_timeout_for("15s") == ExchangeWS.WATCH_TIMEOUT  # floor
    assert ExchangeWS.watch_timeout_for("1m") == 150
    assert ExchangeWS.watch_timeout_for("5m") == 630


class _SilentCcx:
    """A transport that never delivers a frame (routing split / dead stream)."""

    def __init__(self):
        self.unwatch_calls = 0
        self.ohlcvs = {}
        self._closed = asyncio.Event()

    async def watch_ohlcv(self, pair, timeframe):
        await self._closed.wait()
        return []

    async def un_watch_ohlcv_for_symbols(self, symbols):
        self.unwatch_calls += 1

    async def close(self):
        pass


class _HangingCloseCcx(_SilentCcx):
    async def close(self):
        await self._closed.wait()


def _bare_ws() -> ExchangeWS:
    ws = ExchangeWS.__new__(ExchangeWS)
    ccx = MagicMock()
    ccx.ohlcvs = {}
    ws._ccxt_object = ccx
    ws._klines_watching = set()
    ws._klines_scheduled = set()
    ws.klines_last_refresh = {}
    ws.klines_last_request = {}
    ws._retry_after = {}
    ws._background_tasks = set()
    ws._resetting = False
    ws._shutdown = False
    ws._reset_future = None
    return ws


def test_silent_transport_times_out_unwatches_and_retries(mocker):
    """
    A watch that never receives a frame must time out, unsubscribe in a
    bounded way and become reschedulable instead of hanging the slot forever.
    """
    ccx = _SilentCcx()
    ws = _bare_ws()
    ws._ccxt_object = ccx
    p = ("ETH/BTC", "5m", CandleType.SPOT)
    ws._klines_watching.add(p)
    ws._klines_scheduled.add(p)
    mocker.patch.object(ExchangeWS, "watch_timeout_for", return_value=0.2)
    mocker.patch.object(ExchangeWS, "RETRY_DELAY", 0)

    async def scenario():
        from functools import partial

        ws._loop = asyncio.get_running_loop()
        task = asyncio.create_task(ws._continuously_async_watch_ohlcv(*p))
        ws._background_tasks.add(task)
        task.add_done_callback(partial(ws._continuous_stopped, pair=p[0], timeframe=p[1], candle_type=p[2]))
        await task

    asyncio.run(scenario())

    assert ccx.unwatch_calls == 1  # task unsubscribed on its way out
    assert p in ws._klines_watching  # subscription intent kept for retry
    assert p in ws._retry_after  # backoff recorded
    assert p not in ws._klines_scheduled  # slot released for the reschedule pass


def test_batch_schedule_does_not_expire_peers():
    """
    Registering the whole batch before expiry keeps slow-loop peers alive;
    only genuinely stale pairs are evicted.
    """
    ws = _bare_ws()
    p1 = ("ETH/BTC", "1m", CandleType.SPOT)
    p2 = ("XRP/BTC", "1m", CandleType.SPOT)
    p3 = ("LTC/BTC", "1m", CandleType.SPOT)
    stale = ("BNB/BTC", "1m", CandleType.SPOT)

    async def scenario():
        ws._loop = asyncio.get_running_loop()
        old = time.monotonic() - 1000
        for p in (p1, p2, p3, stale):
            ws._klines_watching.add(p)
            ws.klines_last_request[p] = old
        ws._register_pairs([p1, p2, p3])
        assert set(ws._klines_watching) == {p1, p2, p3}
        assert all(ws.klines_last_request[p] > old for p in (p1, p2, p3))

    asyncio.run(scenario())


def test_reset_connections_is_nonblocking():
    """The reset must not block the calling (bot) thread on a stuck loop."""
    ccx = MagicMock()
    ccx.close = AsyncMock()
    ccx.ohlcvs = {}
    ws = _bare_ws()
    ws._ccxt_object = ccx

    async def scenario():
        ws._loop = asyncio.get_running_loop()
        ws.reset_connections()
        assert ws._reset_future is not None and not ws._reset_future.done()
        # concurrent.futures.Future is not awaitable - wrap it for the loop
        await asyncio.wait_for(asyncio.wrap_future(ws._reset_future), timeout=2)
        assert ws._resetting is False

    asyncio.run(scenario())


def test_cleanup_bounded_when_transport_hangs(mocker, caplog):
    """cleanup() must return even when the WS transport hangs on close."""
    ccx = _HangingCloseCcx()
    mocker.patch.object(ExchangeWS, "CLOSE_TIMEOUT", 0.2)
    ws = ExchangeWS(MagicMock(), ccx)
    sleep(0.2)  # let the loop thread start
    start = time.monotonic()
    ws.cleanup()
    elapsed = time.monotonic() - start
    assert elapsed < 5
    assert log_has_re("Exception in _cleanup_async", caplog) or log_has_re(
        "WS cleanup timed out", caplog
    )


# ---- Exchange-level WS snapshot validation regression tests ----


def test_ws_candles_cover_refresh_bridge_and_latest(mocker, default_conf):
    exchange = get_patched_exchange(mocker, default_conf)
    pair, tf, ct = "ETH/USDT", "5m", CandleType.SPOT
    interval = timeframe_to_msecs(tf)
    candle_ts = 1635840600000
    candles = [
        [candle_ts - 2 * interval, 1, 2, 3, 4, 5],
        [candle_ts - interval, 1, 2, 3, 4, 5],
    ]

    # REST cache refreshed at the previous closed candle: bridge holds.
    exchange._pairs_last_refresh_time[(pair, tf, ct)] = candle_ts - interval
    assert exchange._ws_candles_cover_refresh(pair, tf, ct, candles, candle_ts) is True

    # Missing the latest closed candle -> unusable.
    assert exchange._ws_candles_cover_refresh(pair, tf, ct, candles[:-1], candle_ts) is False

    # Only a forming candle -> unusable.
    assert (
        exchange._ws_candles_cover_refresh(pair, tf, ct, [[candle_ts, 1, 2, 3, 4, 5]], candle_ts)
        is False
    )

    # A hole inside the bridge -> unusable.
    gapped = [[candle_ts - 3 * interval, 1, 2, 3, 4, 5], candles[-1]]
    exchange._pairs_last_refresh_time[(pair, tf, ct)] = candle_ts - 3 * interval
    assert exchange._ws_candles_cover_refresh(pair, tf, ct, gapped, candle_ts) is False


def test_async_ws_ohlcv_or_rest_revalidates_and_falls_back(mocker, default_conf):
    exchange = get_patched_exchange(mocker, default_conf)
    pair, tf, ct = "ETH/USDT", "5m", CandleType.SPOT
    candle_ts = 1635840600000
    ws = MagicMock()
    ws.get_ohlcv = AsyncMock(return_value=(pair, tf, ct, [], True))
    ws.klines_last_refresh = {}
    exchange._exchange_ws = ws
    exchange._pairs_last_refresh_time[(pair, tf, ct)] = candle_ts - timeframe_to_msecs(tf)
    rest = mocker.patch.object(
        exchange, "_async_get_candle_history", new=AsyncMock(return_value="REST")
    )

    # A reconnect cleared the buffer AFTER job selection: the same refresh
    # must fetch REST instead of skipping the pair.
    assert asyncio.run(exchange._async_ws_ohlcv_or_rest(pair, tf, ct, candle_ts)) == "REST"
    rest.assert_awaited_once()

    # A consumer-side error (snapshot invalidated) also falls back to REST.
    ws.get_ohlcv = AsyncMock(side_effect=TemporaryError("boom"))
    rest.reset_mock()
    assert asyncio.run(exchange._async_ws_ohlcv_or_rest(pair, tf, ct, candle_ts)) == "REST"
    rest.assert_awaited_once()
