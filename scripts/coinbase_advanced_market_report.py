#!/usr/bin/env python3
"""Generate a human-readable Coinbase Advanced market migration checklist.

This script does not call the exchange directly. It is a project-side reporting
helper for the test branch.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    cfg_path = ROOT / "user_data" / "config.coinbase_advanced_futures.example.json"
    cfg = json.loads(cfg_path.read_text())
    pairs = cfg["exchange"].get("pair_whitelist", [])

    print("== Coinbase Advanced Futures Test Branch Report ==")
    print(f"project: {ROOT}")
    print(f"strategy: {cfg.get('strategy')}")
    print(f"trading_mode: {cfg.get('trading_mode')}")
    print(f"margin_mode: {cfg.get('margin_mode')}")
    print(f"stake_currency: {cfg.get('stake_currency')}")
    print("\nPairs configured:")
    for pair in pairs:
        print(f"- {pair}")

    print("\nManual validation checklist:")
    print("- Verify each pair exists in Coinbase Advanced derivatives account")
    print("- Verify CCXT returns same symbol format as config")
    print("- Verify fetch_positions returns contracts/leverage/side fields")
    print("- Verify market orders and reduceOnly semantics on close orders")
    print("- Verify funding fee / mark price support before production use")


if __name__ == '__main__':
    main()
