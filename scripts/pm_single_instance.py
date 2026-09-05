#!/usr/bin/env python3
"""
Cross-platform single-instance lock wrapper for the PM bot.

The WRAPPER process holds the OS lock for its entire lifetime. The bot is
started as a CHILD process (``subprocess.Popen`` - never ``os.execvp``) and the
wrapper waits for it. The OS releases the lock when the wrapper exits (normal
exit, crash, SIGTERM or SIGKILL), so a lock can never remain permanently held.

Why not exec? ``os.execvp`` replaces the process image, but file descriptors
are non-inheritable by default on POSIX (PEP 446): a lock fd created before
exec would be closed during exec and a second instance could then acquire the
same lock. Keeping the lock inside a long-lived wrapper sidesteps the problem
on BOTH Windows and Linux.

Signal handling:
- POSIX: SIGTERM/SIGINT received by the wrapper are forwarded to the child;
  the wrapper reaps the child and exits with the child's exit code.
- Linux: the child additionally arms ``PR_SET_PDEATHSIG(SIGTERM)`` so a
  SIGKILLed wrapper can never leave an orphaned bot running without the lock.
- Windows: when the wrapper is terminated the OS releases the byte-range lock.

Usage:

    # Wrapper mode (production): hold the lock, run the bot as a child.
    python scripts/pm_single_instance.py --lock-file user_data/.pm_instance.lock \
        --env-from-file /run/secrets/pm_db_url -- \
        freqtrade trade --config user_data/config_pm_live.json ...

    # Direct acquire mode (tests / other scripts): hold the lock until killed.
    python scripts/pm_single_instance.py --lock-file user_data/.pm_instance.lock
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


LOCK_BUSY_EXIT_CODE = 1


def account_lock_path(env: dict[str, str], lock_dir: Path) -> Path | None:
    """
    Host-wide, ACCOUNT-level lock path derived from the exchange API key.

    Any wrapper launched with the same API key resolves to the same lock file
    regardless of the deployment directory, so a stale/duplicate deployment on
    this host can never run a second bot against the same account.  Returns
    None when no API key is present (e.g. dry-run test invocations).
    """
    key = env.get("FREQTRADE__EXCHANGE__KEY") or env.get("BINANCE_PM_API_KEY")
    if not key:
        return None
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return lock_dir / f"account-{digest}.lock"


def acquire(lock_file: Path) -> int:
    """
    Acquire the single-instance lock. Returns the open fd which MUST stay open
    for the lifetime of the caller. Raises SystemExit(1) when another process
    already holds the lock.
    """
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_file), os.O_CREAT | os.O_RDWR)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError as e:
            os.close(fd)
            raise SystemExit(
                f"Another freqtrade PM instance is already running "
                f"(lock: {lock_file}). Refusing to start a second instance."
            ) from e
    else:
        import fcntl

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            os.close(fd)
            raise SystemExit(
                f"Another freqtrade PM instance is already running "
                f"(lock: {lock_file}). Refusing to start a second instance."
            ) from e
    # Record the owner so operators can diagnose stale locks.
    os.ftruncate(fd, 0)
    os.write(fd, f"{os.getpid()}".encode())
    return fd


def read_env_file(path: str) -> dict[str, str]:
    """Read KEY=VALUE lines (e.g. a Docker secret) into an env dict."""
    env: dict[str, str] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            # Skip empty keys/values so a blank secret can never override a
            # real environment variable with an empty string.
            continue
        env[key] = value
    return env


# --- child management (wrapper mode) ---

_CHILD: subprocess.Popen | None = None


def _forward_signal(signum, _frame) -> None:
    if _CHILD is not None and _CHILD.poll() is None:
        try:
            _CHILD.send_signal(signum)
        except (ProcessLookupError, OSError):
            pass


def _install_signal_forwarding() -> None:
    if os.name == "nt":
        return  # Windows: the OS releases the lock when the wrapper dies.
    for sig in (signal.SIGTERM, signal.SIGINT, getattr(signal, "SIGHUP", None)):
        if sig is not None:
            signal.signal(sig, _forward_signal)


def _arm_pdeathsig() -> None:
    """Linux only: child receives SIGTERM when the wrapper dies unexpectedly."""
    if os.name == "nt":
        return
    try:
        import ctypes

        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        libc.prctl(1, int(signal.SIGTERM))  # PR_SET_PDEATHSIG = 1
    except Exception as exc:  # noqa: BLE001 - best-effort hardening
        logging.getLogger(__name__).debug("prctl(PR_SET_PDEATHSIG) unavailable: %s", exc)


def run_child(cmd: list[str], env_extra: dict[str, str]) -> int:
    """Run the bot as a child process while THIS process keeps the lock fd open."""
    global _CHILD
    env = dict(os.environ)
    env.update(env_extra)
    kwargs: dict = {}
    if os.name == "posix":
        kwargs["preexec_fn"] = _arm_pdeathsig
    # close_fds defaults to True on POSIX: the lock fd is NOT leaked to the child
    # (the wrapper itself keeps it open - that is what holds the lock).
    _CHILD = subprocess.Popen(cmd, env=env, **kwargs)
    _install_signal_forwarding()
    try:
        return _CHILD.wait()
    except KeyboardInterrupt:
        if _CHILD.poll() is None:
            _CHILD.terminate()
        return _CHILD.wait()


def main() -> None:
    parser = argparse.ArgumentParser(description="Single-instance lock wrapper for freqtrade PM.")
    parser.add_argument(
        "--lock-file",
        default="user_data/.pm_instance.lock",
        help="Path to the lock file (default: user_data/.pm_instance.lock).",
    )
    parser.add_argument(
        "--env-from-file",
        action="append",
        default=[],
        metavar="FILE",
        help="Inject KEY=VALUE environment from FILE (e.g. a Docker secret). "
        "Repeatable. Values are passed to the child process env only - never "
        "on the command line.",
    )
    parser.add_argument(
        "--account-lock-dir",
        default=os.environ.get("PM_ACCOUNT_LOCK_DIR", "/pm_account_locks"),
        help="Host-wide directory for the account-level lock (derived from the "
        "exchange API key). Default: /pm_account_locks.",
    )
    parser.add_argument(
        "--exec",
        nargs=argparse.REMAINDER,
        metavar="CMD...",
        help="Command to run as a child while this wrapper holds the lock. "
        "Everything after '--exec' is passed to the child as argv.",
    )
    args = parser.parse_args()

    env_extra: dict[str, str] = {}
    for env_file in args.env_from_file:
        env_extra.update(read_env_file(env_file))

    fds: list[int] = []

    def release_all() -> None:
        for held in reversed(fds):
            try:
                os.close(held)
            except OSError:
                pass

    # Account-level lock first: it is the strongest guard (see docstring of
    # account_lock_path).  The per-directory lock file is kept as a second,
    # human-readable guard.
    account_lock = account_lock_path(env_extra, Path(args.account_lock_dir))
    if account_lock is not None:
        fds.append(acquire(account_lock))
        print(f"PM account-level lock acquired: {account_lock}", flush=True)

    lock_file = Path(args.lock_file)
    fds.append(acquire(lock_file))
    print(f"PM single-instance lock acquired: {lock_file}", flush=True)

    if args.exec:
        cmd = list(args.exec)
        if not cmd:
            release_all()
            raise SystemExit("--exec requires a command")
        rc = run_child(cmd, env_extra)
        release_all()  # release the locks after the child exits
        sys.exit(rc)

    # Direct acquire mode: hold the lock until terminated (used by tests /
    # scripts that only need the gate).
    print("Lock held (no --exec command). Waiting for termination ...", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        release_all()


if __name__ == "__main__":
    main()
