#!/usr/bin/env python3
"""Summarize Coinbase Advanced test-branch work products."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

KEYS = [
    'freqtrade/exchange/coinbase.py',
    'freqtrade/exchange/coinbase_advanced_compat.py',
    'freqtrade/exchange/coinbase_advanced_models.py',
    'user_data/strategies/VWAP_V4_CoinbaseAdvancedFutures.py',
    'user_data/strategies/CoinbaseAdvancedDirectionalFutures.py',
    'docs/coinbase_advanced_futures_test.md',
    'docs/coinbase_advanced_api_mapping.md',
    'TEST_README.md',
    'TEST_BRANCH_COINBASE_ADVANCED_CHANGELOG.md',
]


def main() -> None:
    print('== Coinbase Advanced Test Branch Report ==')
    print(f'root: {ROOT}')
    for item in KEYS:
        p = ROOT / item
        print(f'- {item}: {"OK" if p.exists() else "MISSING"}')


if __name__ == '__main__':
    main()
