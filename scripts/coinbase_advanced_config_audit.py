#!/usr/bin/env python3
"""Audit Coinbase Advanced futures configs in the test branch."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
USER_DATA = ROOT / 'user_data'

CONFIGS = sorted(USER_DATA.glob('config.coinbase_advanced_futures*.json'))


def main() -> None:
    print('== Coinbase Advanced futures config audit ==')
    for cfg_path in CONFIGS:
        cfg = json.loads(cfg_path.read_text())
        exchange = cfg.get('exchange', {})
        print(f'\n[{cfg_path.name}]')
        print(f"strategy              : {cfg.get('strategy')}")
        print(f"trading_mode          : {cfg.get('trading_mode')}")
        print(f"margin_mode           : {cfg.get('margin_mode')}")
        print(f"stake_currency        : {cfg.get('stake_currency')}")
        print(f"stake_amount          : {cfg.get('stake_amount')}")
        print(f"max_open_trades       : {cfg.get('max_open_trades')}")
        print(f"dry_run               : {cfg.get('dry_run')}")
        print(f"pair_count            : {len(exchange.get('pair_whitelist', []))}")
        print(f"pairs                 : {', '.join(exchange.get('pair_whitelist', []))}")
        options = (exchange.get('ccxt_config') or {}).get('options', {})
        print(f"ccxt.defaultType      : {options.get('defaultType')}")
        print(f"ccxt.defaultSubType   : {options.get('defaultSubType')}")


if __name__ == '__main__':
    main()
