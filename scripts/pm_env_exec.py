#!/usr/bin/env python3
"""Run a command with ``KEY=VALUE`` Docker-secret settings loaded into its environment.

This helper deliberately does not acquire a process or account lock.  It only
keeps the PM credentials and PostgreSQL URL in a Docker secret file instead of
embedding them in the Compose command or image configuration.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def read_env_file(path: str) -> dict[str, str]:
    """Read non-empty ``KEY=VALUE`` lines without logging sensitive values."""
    values: dict[str, str] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if key and value:
            values[key] = value
    return values


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a command with PM secret-file environment values.")
    parser.add_argument("--env-from-file", required=True)
    parser.add_argument("--exec", dest="command", nargs=argparse.REMAINDER, required=True)
    args = parser.parse_args(argv)
    if not args.command:
        parser.error("--exec requires a command")

    environment = os.environ.copy()
    try:
        environment.update(read_env_file(args.env_from_file))
    except OSError as exc:
        print(f"Cannot read PM environment secret file: {exc}", file=sys.stderr)
        return 2
    os.execvpe(args.command[0], args.command, environment)
    return 127


if __name__ == "__main__":
    raise SystemExit(main())
