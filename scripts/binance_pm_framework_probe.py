#!/usr/bin/env python3
"""
Probe Binance Portfolio Margin through the Freqtrade Binance adapter.

This validates framework-level parsing instead of only raw HTTP responses:
- Binance.get_pm_risk_summary()
- Binance.get_balances()
- Binance.fetch_positions()
- Binance listenKey lifecycle methods
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


class FrameworkProbeError(RuntimeError):
    pass


def env_required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise FrameworkProbeError(f"Missing required environment variable: {name}")
    return value


def nonzero_balances(balances: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    return {
        currency: balance
        for currency, balance in balances.items()
        if balance.get("free", 0.0) or balance.get("used", 0.0) or balance.get("total", 0.0)
    }


def nonzero_positions(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "symbol": position.get("symbol"),
            "side": position.get("side"),
            "contracts": position.get("contracts"),
            "leverage": position.get("leverage"),
            "collateral": position.get("collateral"),
            "liquidationPrice": position.get("liquidationPrice"),
        }
        for position in positions
        if position.get("contracts", 0.0) or position.get("collateral", 0.0)
    ]


def build_config(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "dry_run": False,
        "trading_mode": "futures",
        "margin_mode": "cross",
        "stake_currency": args.stake_currency,
        "liquidation_buffer": 0.05,
        "datadir": Path("user_data/data/binance"),
        "user_data_dir": Path("user_data"),
        "exchange": {
            "name": "binance",
            "key": env_required("BINANCE_PM_API_KEY"),
            "secret": env_required("BINANCE_PM_API_SECRET"),
            "portfolio_margin": True,
            "portfolio_margin_risk": {
                "min_uni_mmr": args.min_uni_mmr,
                "warning_uni_mmr": args.warning_uni_mmr,
            },
            "ccxt_config": {},
            "ccxt_sync_config": {},
            "ccxt_async_config": {},
            "pair_whitelist": [],
            "pair_blacklist": [],
        },
    }


def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    try:
        from freqtrade.exchange.binance import Binance
    except ModuleNotFoundError as exc:
        raise FrameworkProbeError(
            "Missing Python dependency while importing Freqtrade Binance adapter. "
            "Install project requirements before running this framework probe."
        ) from exc

    exchange = Binance(build_config(args), validate=False, load_leverage_tiers=False)
    try:
        result: dict[str, Any] = {
            "adapter": "freqtrade.exchange.binance.Binance",
            "pm_enabled": exchange._is_portfolio_margin(),
            "risk": exchange.get_pm_risk_summary(),
        }

        balances = exchange.get_balances()
        result["balances"] = {
            "nonzero": nonzero_balances(balances),
            "count": len(balances),
        }

        if args.positions:
            exchange.reload_markets(True, load_leverage_tiers=False)
            positions = exchange.fetch_positions()
            result["positions"] = {
                "nonzero": nonzero_positions(positions),
                "count": len(positions),
            }

        if args.listen_key_test or args.user_stream_test:
            listen_key = exchange.create_pm_listen_key()
            try:
                if args.listen_key_test:
                    exchange.keepalive_pm_listen_key(listen_key)
                    result["listen_key"] = {
                        "created": bool(listen_key),
                        "lifecycle": "CREATE->KEEPALIVE OK",
                    }
                if args.user_stream_test:
                    exchange.start_pm_user_stream(listen_key)
                    time.sleep(args.stream_wait_seconds)
                    result["user_stream"] = exchange.get_pm_user_stream_stats()
                    exchange.stop_pm_user_stream()
            finally:
                exchange.delete_pm_listen_key(listen_key)
                if args.listen_key_test:
                    result["listen_key"]["deleted"] = True

        if args.assert_risk:
            exchange.assert_pm_risk_allows_order()
            result["assert_risk_allows_order"] = True

        return result
    finally:
        exchange.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe Binance PM through Freqtrade's Binance adapter."
    )
    parser.add_argument("--stake-currency", default="USDT")
    parser.add_argument(
        "--positions", action="store_true", help="Also fetch and parse UM positions."
    )
    parser.add_argument(
        "--listen-key-test",
        action="store_true",
        help="Create, keepalive and delete a PM listenKey through the adapter.",
    )
    parser.add_argument(
        "--user-stream-test",
        action="store_true",
        help="Create a PM listenKey, start the adapter user stream and report stream stats.",
    )
    parser.add_argument(
        "--stream-wait-seconds",
        type=float,
        default=5.0,
        help="How long to wait for the PM user stream connection before reading stats.",
    )
    parser.add_argument(
        "--assert-risk",
        action="store_true",
        help="Run assert_pm_risk_allows_order() without an order amount.",
    )
    parser.add_argument("--min-uni-mmr", type=float, default=None)
    parser.add_argument("--warning-uni-mmr", type=float, default=None)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    try:
        result = run_probe(args)
        print(json.dumps(result, indent=2 if args.pretty else None, default=str, sort_keys=True))
        return 0
    except FrameworkProbeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
