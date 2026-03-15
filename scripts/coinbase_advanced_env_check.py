#!/usr/bin/env python3
"""Environment audit for the Coinbase Advanced Futures test branch.

Goal: distinguish between
1. tests not written
2. tests written but environment not prepared
"""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

MODULES = ["pytest", "ccxt", "freqtrade", "pandas", "numpy", "talib"]
TEST_FILES = sorted((ROOT / "tests").rglob("test_coinbase_advanced*.py"))


def has_module(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def main() -> None:
    print("== Coinbase Advanced Futures Test Branch Environment Check ==")
    print(f"root: {ROOT}")
    print(f"pytest_cmd: {shutil.which('pytest')}")
    print("\nModules:")
    for mod in MODULES:
        print(f"- {mod}: {'OK' if has_module(mod) else 'MISSING'}")

    print("\nCoinbase Advanced specific tests present:")
    if not TEST_FILES:
        print("- none")
    else:
        for t in TEST_FILES:
            print(f"- {t.relative_to(ROOT)}")

    print("\nInterpretation:")
    if TEST_FILES and (not has_module('pytest') or shutil.which('pytest') is None):
        print("- Tests exist, but the environment is not prepared to run them.")
    elif not TEST_FILES:
        print("- No Coinbase Advanced specific tests found in tree.")
    else:
        print("- Tests exist and pytest is present; next step is executing them.")


if __name__ == '__main__':
    main()
