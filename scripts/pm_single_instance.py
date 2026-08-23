#!/usr/bin/env python3
"""
Cross-platform single-instance lock for the PM bot.

Guarantees that only ONE live PM bot process can run against the same database
(and therefore the same Binance account) at a time. Used as a pre-start gate in
production deployments:

    python scripts/pm_single_instance.py --lock-file user_data/.pm_instance.lock \
        && freqtrade trade ...

Behavior:
- Locks are acquired non-blocking (fail fast): if another process holds the
  lock, this script exits with code 1 and a clear error.
- The lock is automatically released when the process exits (OS-level lock).
- On Windows the lock is byte-range based (msvcrt); on POSIX it is fcntl based.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def acquire(lock_file: Path):
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
    # Truncate and record the owner so operators can diagnose stale locks.
    os.ftruncate(fd, 0)
    os.write(fd, f"{os.getpid()}".encode())
    # NOTE: keep the fd open for the lifetime of this process. The lock is
    # released by the OS when the process exits (or when the subprocess below
    # exits, if we exec into it).
    return fd


def main() -> None:
    parser = argparse.ArgumentParser(description="Single-instance lock gate for freqtrade PM.")
    parser.add_argument(
        "--lock-file",
        default="user_data/.pm_instance.lock",
        help="Path to the lock file (default: user_data/.pm_instance.lock).",
    )
    parser.add_argument(
        "--exec",
        nargs=argparse.REMAINDER,
        metavar="CMD...",
        help="Optional command to exec after acquiring the lock.",
    )
    args = parser.parse_args()

    lock_file = Path(args.lock_file)
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    acquire(lock_file)
    print(f"PM single-instance lock acquired: {lock_file}")

    if args.exec:
        os.execvp(args.exec[0], args.exec)  # noqa: S606 - exec'ing the passed command


if __name__ == "__main__":
    main()
