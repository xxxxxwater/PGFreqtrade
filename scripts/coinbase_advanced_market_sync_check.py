#!/usr/bin/env python3
"""Static audit for Coinbase Advanced futures market sync assumptions in the test branch."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
USER_DATA = ROOT / 'user_data'

CONFIGS = sorted(USER_DATA.glob('config.coinbase_advanced_futures*.json'))


def main() -> None:
    print('== Coinbase Advanced Futures Market Sync Check ==')
    for cfg_path in CONFIGS:
        cfg = json.loads(cfg_path.read_text())
        pairs = (cfg.get('exchange') or {}).get('pair_whitelist', [])
        bad = [p for p in pairs if ':' not in p]
        print(f'\n[{cfg_path.name}]')
        print(f'- strategy: {cfg.get("strategy")}')
        print(f'- pair_count: {len(pairs)}')
        print(f'- normalized_futures_symbols: {"YES" if not bad else "NO"}')
        if bad:
            print(f'- non_normalized_pairs: {bad}')


if __name__ == '__main__':
    main()
