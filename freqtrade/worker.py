"""
Main Freqtrade worker class.
"""

import logging
import os
import time
import traceback
from collections.abc import Callable
from os import getpid
from pathlib import Path
from typing import Any

import sdnotify

from freqtrade import __version__
from freqtrade.configuration import Configuration
from freqtrade.constants import PROCESS_THROTTLE_SECS, RETRY_TIMEOUT, Config
from freqtrade.enums import RPCMessageType, State
from freqtrade.exceptions import OperationalException, TemporaryError
from freqtrade.exchange import timeframe_to_next_date
from freqtrade.freqtradebot import FreqtradeBot
from freqtrade.state_persistence import persist_state
from freqtrade.util import PeriodicCache


logger = logging.getLogger(__name__)


class Worker:
    """
    Freqtradebot worker class
    """

    def __init__(self, args: dict[str, Any], config: Config | None = None) -> None:
        """
        Init all variables and objects the bot needs to work
        """
        logger.info(f"Starting worker {__version__}")

        self._args = args
        self._config = config
        self._init(False)

        self._heartbeat_msg: float = 0
        self._telegram_heartbeat_msg: float = 0
        # Keep the liveness marker on the container-local tmpfs so it is always
        # writable by the unprivileged ftuser and never depends on host bind-mount
        # ownership. The host watchdog reads it through ``docker exec``.
        self._heartbeat_file = Path("/tmp/pgfreqtrade-worker-heartbeat")

        # Tell systemd that we completed initialization phase
        self._notify("READY=1")

    def _init(self, reconfig: bool) -> None:
        """
        Also called from the _reconfigure() method (with reconfig=True).
        """
        if reconfig or self._config is None:
            # Load configuration
            self._config = Configuration(self._args, None).get_config()

        # Init the instance of the bot
        self.freqtrade = FreqtradeBot(self._config)

        internals_config = self._config.get("internals", {})
        self._throttle_secs = internals_config.get("process_throttle_secs", PROCESS_THROTTLE_SECS)
        self._heartbeat_interval = internals_config.get("heartbeat_interval", 60)
        risk_cfg = self._config.get("exchange", {}).get("portfolio_margin_risk", {})
        heartbeat_interval = int(self._heartbeat_interval or 60)
        heartbeat_risk_cache_seconds = int(
            risk_cfg.get("heartbeat_risk_cache_seconds", max(heartbeat_interval, 300))
        )
        self._pm_heartbeat_risk_cache = PeriodicCache(
            maxsize=1, ttl=max(heartbeat_risk_cache_seconds, 1)
        )

        self._sd_notify = (
            sdnotify.SystemdNotifier()
            if self._config.get("internals", {}).get("sd_notify", False)
            else None
        )

    def _retry_state_persist(self) -> None:
        """Self-heal a previously failed state persist (see FreqtradeBot.set_state).

        Called every worker iteration while the failure flag is set; the
        state file was invalidated by set_state, so this re-persists the
        CURRENT in-memory state until the disk write succeeds again.
        """
        if getattr(self.freqtrade, "_state_persist_failed", False):
            if persist_state(self._config, self.freqtrade.state):
                self.freqtrade._state_persist_failed = False
                logger.warning("Recovered persisted bot state after an earlier write failure.")

    def _notify(self, message: str) -> None:
        """
        Removes the need to verify in all occurrences if sd_notify is enabled
        :param message: Message to send to systemd if it's enabled.
        """
        if self._sd_notify:
            logger.debug(f"sd_notify: {message}")
            self._sd_notify.notify(message)

    def _pm_cached_heartbeat_risk(self) -> dict[str, Any]:
        cache_key = "risk"
        if cache_key in self._pm_heartbeat_risk_cache:
            return self._pm_heartbeat_risk_cache[cache_key]

        risk = self.freqtrade.exchange.get_pm_risk_summary()
        self._pm_heartbeat_risk_cache[cache_key] = risk
        return risk

    def run(self) -> None:
        state = None
        while True:
            state = self._worker(old_state=state)
            if state == State.RELOAD_CONFIG:
                self._reconfigure()

    def _pm_heartbeat_suffix(self) -> str:
        try:
            if getattr(
                self.freqtrade.exchange, "_is_portfolio_margin", lambda: False
            )() and not self.freqtrade.config.get("dry_run", True):
                risk = self._pm_cached_heartbeat_risk()
                if risk.get("enabled"):
                    stream_suffix = ""
                    if hasattr(self.freqtrade.exchange, "get_pm_user_stream_stats"):
                        stream = self.freqtrade.exchange.get_pm_user_stream_stats()
                        stream_suffix = (
                            f", stream_connected={stream.get('connected')}, "
                            f"stream_queue={stream.get('queued_events')}, "
                            f"stream_dropped={stream.get('events_dropped')}"
                        )
                    return (
                        f", uniMMR={risk.get('uni_mmr')}, "
                        f"equity={risk.get('account_equity')}, "
                        f"status={risk.get('account_status')}"
                        f"{stream_suffix}"
                    )
        except Exception as e:
            logger.debug(f"PM heartbeat risk fetch failed: {e}")
        return ""

    def _write_worker_heartbeat(self, state: State) -> None:
        """Write an independent liveness marker consumed by the host watchdog."""
        try:
            heartbeat_file = self._heartbeat_file
            heartbeat_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = heartbeat_file.with_name(f".{heartbeat_file.name}.{os.getpid()}.tmp")
            tmp.write_text(
                f"timestamp={time.time():.6f}\npid={getpid()}\nstate={state.name}\n",
                encoding="utf-8",
            )
            os.replace(tmp, heartbeat_file)
        except OSError as exc:
            logger.debug(f"Worker heartbeat marker write failed: {exc}")

    def _worker(self, old_state: State | None) -> State:
        """
        The main routine that runs each throttling iteration and handles the states.
        :param old_state: the previous service state from the previous call
        :return: current service state
        """
        state = self.freqtrade.state

        # Log state transition
        if state != old_state:
            if old_state != State.RELOAD_CONFIG:
                self.freqtrade.notify_status(f"{state.name.lower()}")

            logger.info(
                f"Changing state{f' from {old_state.name}' if old_state else ''} to: {state.name}"
            )
            if state in (State.RUNNING, State.PAUSED) and old_state not in (
                State.RUNNING,
                State.PAUSED,
            ):
                self.freqtrade.startup()

            if state == State.STOPPED:
                self.freqtrade.check_for_open_trades()

            # Reset heartbeat timestamp to log the heartbeat message at
            # first throttling iteration when the state changes
            self._heartbeat_msg = 0

        # Self-heal a previously failed state persist: retry every iteration
        # until the current state is durably on disk again.
        self._retry_state_persist()

        if state == State.STOPPED:
            # Ping systemd watchdog before sleeping in the stopped state
            self._notify("WATCHDOG=1\nSTATUS=State: STOPPED.")

            self._throttle(func=self._process_stopped, throttle_secs=self._throttle_secs)

        elif state in (State.RUNNING, State.PAUSED):
            state_str = "RUNNING" if state == State.RUNNING else "PAUSED"
            # Ping systemd watchdog before throttling
            self._notify(f"WATCHDOG=1\nSTATUS=State: {state_str}.")

            # Use an offset of 1s to ensure a new candle has been issued
            self._throttle(
                func=self._process_running,
                throttle_secs=self._throttle_secs,
                timeframe=self._config["timeframe"] if self._config else None,
                timeframe_offset=1,
            )

        now = time.time()
        if self._heartbeat_interval:
            if (now - self._heartbeat_msg) > self._heartbeat_interval:
                version = __version__
                strategy_version = self.freqtrade.strategy.version()
                if strategy_version is not None:
                    version += ", strategy_version: " + strategy_version
                heartbeat_msg = (
                    f"Bot heartbeat. PID={getpid()}, version='{version}', state='{state.name}'"
                )
                heartbeat_msg += self._pm_heartbeat_suffix()
                logger.info(heartbeat_msg)
                self._heartbeat_msg = now
                self._write_worker_heartbeat(state)

        # Record the five-minute PM live status in the logfile only.
        # Routine liveness must never spam Telegram; alerts/trades remain RPC-driven.
        if now - self._telegram_heartbeat_msg >= 300:
            self._telegram_heartbeat_msg = now
            try:
                if (
                    not self._config.get("dry_run", True)
                    and getattr(self.freqtrade.exchange, "_is_portfolio_margin", lambda: False)()
                ):
                    pm_status = self._pm_heartbeat_suffix() or ", PM risk=UNAVAILABLE"
                    logger.info(
                        f"PM heartbeat: strategy={self._config.get('strategy', 'unknown')}, "
                        f"framework={__version__}, PID={getpid()}, state={state.name}"
                        f"{pm_status}"
                    )
            except Exception:
                logger.exception("Could not write PM heartbeat")

        return state

    def _throttle(
        self,
        func: Callable[..., Any],
        throttle_secs: float,
        timeframe: str | None = None,
        timeframe_offset: float = 1.0,
        *args,
        **kwargs,
    ) -> Any:
        """
        Throttles the given callable that it
        takes at least `min_secs` to finish execution.
        :param func: Any callable
        :param throttle_secs: throttling iteration execution time limit in seconds
        :param timeframe: ensure iteration is executed at the beginning of the next candle.
        :param timeframe_offset: offset in seconds to apply to the next candle time.
        :return: Any (result of execution of func)
        """
        last_throttle_start_time = time.time()
        logger.debug("========================================")
        result = func(*args, **kwargs)
        time_passed = time.time() - last_throttle_start_time
        sleep_duration = throttle_secs - time_passed
        if timeframe:
            next_tf = timeframe_to_next_date(timeframe)
            # Maximum throttling should be until new candle arrives
            # Offset is added to ensure a new candle has been issued.
            next_tft = next_tf.timestamp() - time.time()
            next_tf_with_offset = next_tft + timeframe_offset
            if next_tft < sleep_duration and sleep_duration < next_tf_with_offset:
                # Avoid hitting a new loop between the new candle and the candle with offset
                sleep_duration = next_tf_with_offset
            sleep_duration = min(sleep_duration, next_tf_with_offset)
        sleep_duration = max(sleep_duration, 0.0)
        # next_iter = datetime.now(timezone.utc) + timedelta(seconds=sleep_duration)

        logger.debug(
            f"Throttling with '{func.__name__}()': sleep for {sleep_duration:.2f} s, "
            f"last iteration took {time_passed:.2f} s."
            #  f"next: {next_iter}"
        )
        self._sleep(sleep_duration)
        return result

    @staticmethod
    def _sleep(sleep_duration: float) -> None:
        """Local sleep method - to improve testability"""
        time.sleep(sleep_duration)

    def _process_stopped(self) -> None:
        self.freqtrade.process_stopped()

    def _process_running(self) -> None:
        try:
            self.freqtrade.process()
        except TemporaryError as error:
            logger.warning(f"Error: {error}, retrying in {RETRY_TIMEOUT} seconds...")
            time.sleep(RETRY_TIMEOUT)
        except OperationalException:
            tb = traceback.format_exc()
            hint = "Issue `/start` if you think it is safe to restart."

            self.freqtrade.notify_status(
                f"*OperationalException:*\n```\n{tb}```\n {hint}", msg_type=RPCMessageType.EXCEPTION
            )

            logger.exception("OperationalException. Stopping trader ...")
            self.freqtrade.set_state(State.STOPPED)

    def _reconfigure(self) -> None:
        """
        Cleans up current freqtradebot instance, reloads the configuration and
        replaces it with the new instance
        """
        # Tell systemd that we initiated reconfiguration
        self._notify("RELOADING=1")

        # Clean up current freqtrade modules
        self.freqtrade.cleanup()

        # Load and validate config and create new instance of the bot
        self._init(True)

        self.freqtrade.notify_status(f"{State(self.freqtrade.state)} after config reloaded")

        # Tell systemd that we completed reconfiguration
        self._notify("READY=1")

    def exit(self) -> None:
        # Tell systemd that we are exiting now
        self._notify("STOPPING=1")

        if self.freqtrade:
            self.freqtrade.notify_status("process died")
            self.freqtrade.cleanup()
