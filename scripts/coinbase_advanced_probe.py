#!/usr/bin/env python3
"""Small helper to inspect Coinbase Advanced / CCXT capabilities in the test tree.

Usage examples:
    python scripts/coinbase_advanced_probe.py --config user_data/config.coinbase_advanced_futures.example.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_config(path: str) -> dict:
    return json.loads(Path(path).read_text())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    cfg = load_config(args.config)
    ex_cfg = cfg.get("exchange", {})

    print("== Coinbase Advanced probe ==")
    print(f"trading_mode       : {cfg.get('trading_mode', 'spot')}")
    print(f"margin_mode        : {cfg.get('margin_mode', 'none')}")
    print(f"stake_currency     : {cfg.get('stake_currency')}")
    print(f"exchange.name      : {ex_cfg.get('name')}")
    print(f"pair_whitelist     : {ex_cfg.get('pair_whitelist', [])}")
    print(f"ccxt defaultType   : {(ex_cfg.get('ccxt_config') or {}).get('options', {}).get('defaultType')}")
    print(f"ccxt defaultSubType: {(ex_cfg.get('ccxt_config') or {}).get('options', {}).get('defaultSubType')}")
    print("\nNext manual checks:")
    print("1. freqtrade list-markets --exchange coinbase --trading-mode futures")
    print("2. freqtrade test-pairlist -c <config>")
    print("3. freqtrade show-config -c <config>")
    print("4. Verify returned symbols match CCXT style, e.g. BTC/USDC:USDC")


if __name__ == "__main__":
    main()
