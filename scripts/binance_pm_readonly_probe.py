#!/usr/bin/env python3
# ruff: noqa: S310
"""
Read-only Binance Portfolio Margin probe.

This script intentionally avoids ccxt and reads credentials only from environment variables.
It calls USER_DATA endpoints that are safe for read-only API keys and prints a sanitized
summary suitable for implementation verification.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


BASE_URL = "https://papi.binance.com"
DEFAULT_RECV_WINDOW = 5000


class ProbeError(RuntimeError):
    pass


def env_required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ProbeError(f"Missing required environment variable: {name}")
    return value


def sign_params(secret: str, params: dict[str, Any]) -> str:
    query = urllib.parse.urlencode(params, doseq=True)
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={signature}"


def signed_get(path: str, params: dict[str, Any] | None = None) -> Any:
    api_key = env_required("BINANCE_PM_API_KEY")
    secret = env_required("BINANCE_PM_API_SECRET")
    payload = {
        "recvWindow": int(os.environ.get("BINANCE_PM_RECV_WINDOW", DEFAULT_RECV_WINDOW)),
        "timestamp": int(time.time() * 1000),
    }
    payload.update(params or {})
    query = sign_params(secret, payload)
    request = urllib.request.Request(
        f"{BASE_URL}{path}?{query}",
        headers={"X-MBX-APIKEY": api_key},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise ProbeError(f"{path} failed with HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise ProbeError(f"{path} failed: {exc}") from exc


def as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def nonzero_balances(balances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for balance in balances:
        total = as_float(balance.get("totalWalletBalance")) or 0.0
        free = as_float(balance.get("crossMarginFree")) or 0.0
        locked = as_float(balance.get("crossMarginLocked")) or 0.0
        if total or free or locked:
            result.append(
                {
                    "asset": balance.get("asset"),
                    "totalWalletBalance": balance.get("totalWalletBalance"),
                    "crossMarginFree": balance.get("crossMarginFree"),
                    "crossMarginLocked": balance.get("crossMarginLocked"),
                }
            )
    return result


def nonzero_positions(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for position in positions:
        amount = as_float(position.get("positionAmt")) or 0.0
        margin = as_float(position.get("initialMargin")) or 0.0
        unrealized = as_float(position.get("unRealizedProfit")) or 0.0
        if amount or margin or unrealized:
            result.append(
                {
                    "symbol": position.get("symbol"),
                    "positionSide": position.get("positionSide"),
                    "positionAmt": position.get("positionAmt"),
                    "entryPrice": position.get("entryPrice"),
                    "markPrice": position.get("markPrice"),
                    "unRealizedProfit": position.get("unRealizedProfit"),
                    "initialMargin": position.get("initialMargin"),
                    "leverage": position.get("leverage"),
                }
            )
    return result


def build_summary(account: dict[str, Any], balances: list[dict], positions: list[dict]) -> dict:
    return {
        "account": {
            "accountStatus": account.get("accountStatus"),
            "uniMMR": account.get("uniMMR"),
            "accountEquity": account.get("accountEquity")
            or account.get("actualEquity")
            or account.get("totalEquity"),
            "accountInitialMargin": account.get("accountInitialMargin")
            or account.get("totalInitialMargin"),
            "accountMaintMargin": account.get("accountMaintMargin")
            or account.get("totalMaintMargin"),
        },
        "balances": nonzero_balances(balances),
        "um_positions": nonzero_positions(positions),
        "counts": {
            "balances": len(balances),
            "nonzero_balances": len(nonzero_balances(balances)),
            "um_positions": len(positions),
            "nonzero_um_positions": len(nonzero_positions(positions)),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe Binance PM read-only endpoints.")
    parser.add_argument(
        "--symbol",
        help="Optional UM symbol for /papi/v1/um/positionRisk, e.g. BTCUSDT.",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    args = parser.parse_args()

    position_params = {"symbol": args.symbol.upper()} if args.symbol else None
    account = signed_get("/papi/v1/account")
    balances = signed_get("/papi/v1/balance")
    positions = signed_get("/papi/v1/um/positionRisk", position_params)
    summary = build_summary(account, balances, positions)
    print(json.dumps(summary, indent=2 if args.pretty else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
