#!/usr/bin/env python3
# ruff: noqa: S310
"""
Binance Portfolio Margin production probe & verification tool.

Read-only endpoints compatible with restricted API keys.
Covers account status, risk (uniMMR), balances, positions, listen key lifecycle,
and order/position reconciliation.

Usage:
  export BINANCE_PM_API_KEY="..."
  export BINANCE_PM_API_SECRET="..."
  python scripts/binance_pm_probe.py --all
  python scripts/binance_pm_probe.py --account --balance --positions --pretty
  python scripts/binance_pm_probe.py --listen-key-test
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
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
    # The live Compose wrapper injects Freqtrade's standard secret names.  The
    # aliases let this read-only probe run inside that container without copying
    # credentials into a second environment file.
    if not value:
        aliases = {
            "BINANCE_PM_API_KEY": "FREQTRADE__EXCHANGE__KEY",
            "BINANCE_PM_API_SECRET": "FREQTRADE__EXCHANGE__SECRET",
        }
        value = os.environ.get(aliases.get(name, ""))
    if not value:
        raise ProbeError(f"Missing required environment variable: {name}")
    return value


def sign_params(secret: str, params: dict[str, Any]) -> str:
    query = urllib.parse.urlencode(params, doseq=True)
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={signature}"


def _request_ip_from_body(body: str) -> str | None:
    match = re.search(r"request ip:\s*([0-9a-fA-F:.]+)", body)
    return match.group(1) if match else None


def _format_http_error(path: str, method: str, status: int, body: str) -> ProbeError:
    hints: list[str] = []
    if status in (401, 403) or '"code":-2015' in body or '"code": -2015' in body:
        request_ip = _request_ip_from_body(body)
        if request_ip:
            hints.append(f"Binance saw request IP {request_ip}; verify API IP whitelist.")
        hints.extend(
            [
                "verify this exact key/secret pair is loaded",
                "verify Portfolio Margin/PAPI permission is enabled for this key",
                "verify this is a standard Portfolio Margin account for /papi/v1/*; "
                "Portfolio Margin Pro account queries use Binance portfolio SAPI endpoints",
            ]
        )
    suffix = f" Checks: {'; '.join(hints)}." if hints else ""
    return ProbeError(f"{path} {method} failed with HTTP {status}: {body}{suffix}")


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
        raise _format_http_error(path, "GET", exc.code, body) from exc
    except urllib.error.URLError as exc:
        raise ProbeError(f"{path} failed: {exc}") from exc


def signed_post(path: str, params: dict[str, Any] | None = None) -> Any:
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
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise _format_http_error(path, "POST", exc.code, body) from exc
    except urllib.error.URLError as exc:
        raise ProbeError(f"{path} POST failed: {exc}") from exc


def signed_put(path: str, params: dict[str, Any] | None = None) -> Any:
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
        method="PUT",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise _format_http_error(path, "PUT", exc.code, body) from exc
    except urllib.error.URLError as exc:
        raise ProbeError(f"{path} PUT failed: {exc}") from exc


def signed_delete(path: str, params: dict[str, Any] | None = None) -> Any:
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
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise _format_http_error(path, "DELETE", exc.code, body) from exc
    except urllib.error.URLError as exc:
        raise ProbeError(f"{path} DELETE failed: {exc}") from exc


def as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _nonzero_balances(balances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for balance in balances:
        total = as_float(balance.get("totalWalletBalance")) or 0.0
        free = as_float(balance.get("crossMarginFree")) or 0.0
        locked = as_float(balance.get("crossMarginLocked")) or 0.0
        borrowed = as_float(balance.get("crossMarginBorrowed")) or 0.0
        interest = as_float(balance.get("crossMarginInterest")) or 0.0
        if total or free or locked or borrowed or interest:
            result.append(
                {
                    "asset": balance.get("asset"),
                    "totalWalletBalance": total,
                    "crossMarginFree": free,
                    "crossMarginLocked": locked,
                    "crossMarginBorrowed": borrowed,
                    "crossMarginInterest": interest,
                }
            )
    return result


def _nonzero_positions(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
                    "liquidationPrice": position.get("liquidationPrice"),
                    "initialMargin": position.get("initialMargin"),
                    "leverage": position.get("leverage"),
                }
            )
    return result


def probe_account() -> dict[str, Any]:
    print("→ GET /papi/v1/account ...")
    account = signed_get("/papi/v1/account")
    return {
        "accountStatus": account.get("accountStatus"),
        "uniMMR": as_float(account.get("uniMMR")),
        "accountEquity": as_float(
            account.get("accountEquity")
            or account.get("actualEquity")
            or account.get("totalEquity")
        ),
        "totalCollateralValue": as_float(account.get("totalCollateralValue")),
        "accountInitialMargin": as_float(
            account.get("accountInitialMargin") or account.get("totalInitialMargin")
        ),
        "accountMaintMargin": as_float(
            account.get("accountMaintMargin") or account.get("totalMaintMargin")
        ),
    }


def probe_balance() -> dict[str, Any]:
    print("→ GET /papi/v1/balance ...")
    balances = signed_get("/papi/v1/balance")
    non_zero = _nonzero_balances(balances)
    return {
        "total_count": len(balances),
        "nonzero_count": len(non_zero),
        "nonzero": non_zero,
        "raw": balances[:5],
    }


def probe_positions(symbol: str | None = None) -> dict[str, Any]:
    print("→ GET /papi/v1/um/positionRisk ...")
    params = {"symbol": symbol.upper()} if symbol else {}
    positions = signed_get("/papi/v1/um/positionRisk", params or None)
    non_zero = _nonzero_positions(positions)
    return {
        "total_count": len(positions),
        "nonzero_count": len(non_zero),
        "nonzero": non_zero,
    }


def probe_api_trading_status() -> dict[str, Any]:
    print("→ GET /papi/v1/um/apiTradingStatus ...")
    status = signed_get("/papi/v1/um/apiTradingStatus")
    return {
        "isLocked": status.get("isLocked"),
        "triggerCondition": status.get("triggerCondition"),
        "indicators": status.get("indicators", {}),
        "updateTime": status.get("updateTime"),
    }


def probe_account_config() -> dict[str, Any]:
    print("→ GET /papi/v1/um/accountConfig ...")
    config = signed_get("/papi/v1/um/accountConfig")
    return {
        "dualSidePosition": config.get("dualSidePosition"),
        "multiAssetsMargin": config.get("multiAssetsMargin"),
    }


def probe_symbol_config(symbol: str | None = None) -> dict[str, Any]:
    symbol = symbol.upper() if symbol else None
    qualifier = f" (symbol={symbol})" if symbol else ""
    print(f"→ GET /papi/v1/um/symbolConfig{qualifier} ...")
    response = signed_get("/papi/v1/um/symbolConfig", {"symbol": symbol} if symbol else None)
    configs = response if isinstance(response, list) else [response] if isinstance(response, dict) else []
    if not configs:
        raise ProbeError("/papi/v1/um/symbolConfig returned no usable symbol configuration.")

    selected = next(
        (item for item in configs if isinstance(item, dict) and item.get("symbol") == symbol),
        configs[0],
    )
    if not isinstance(selected, dict):
        raise ProbeError("/papi/v1/um/symbolConfig returned an invalid symbol configuration.")
    return {
        "returned_count": len(configs),
        "symbol": selected.get("symbol"),
        "isTradingEnabled": selected.get("isTradingEnabled"),
        "marginAsset": selected.get("marginAsset"),
        "marginType": selected.get("marginType"),
        "maxNotionalValue": selected.get("maxNotionalValue"),
        "leverage": selected.get("leverage"),
    }


def probe_algo_stoploss() -> dict[str, Any]:
    """Verify the read-only UM Algo channel used for exchange-side stoplosses."""
    print("→ GET /papi/v1/um/algo/openAlgoOrders?algoType=CONDITIONAL ...")
    response = signed_get("/papi/v1/um/algo/openAlgoOrders", {"algoType": "CONDITIONAL"})
    if not isinstance(response, list):
        raise ProbeError("/papi/v1/um/algo/openAlgoOrders returned an invalid response.")
    return {
        "channel": "UM Algo CONDITIONAL",
        "open_count": len(response),
        "symbols": sorted(
            {
                item.get("symbol")
                for item in response
                if isinstance(item, dict) and isinstance(item.get("symbol"), str)
            }
        ),
    }


def probe_listen_key_lifecycle() -> dict[str, Any]:
    print("→ POST /papi/v1/listenKey (create) ...")
    result = signed_post("/papi/v1/listenKey")
    listen_key = result.get("listenKey", "")
    print(f"  Created listenKey: {listen_key[:8]}...")

    print("→ PUT /papi/v1/listenKey (keepalive) ...")
    signed_put("/papi/v1/listenKey", {"listenKey": listen_key})
    print("  Keepalive OK")

    print("→ DELETE /papi/v1/listenKey (delete) ...")
    signed_delete("/papi/v1/listenKey", {"listenKey": listen_key})
    print("  Deleted OK")

    return {"listenKey_created": bool(listen_key), "lifecycle": "CREATE->KEEPALIVE->DELETE OK"}


def probe_client_order_id(client_id: str, symbol: str | None = None) -> dict[str, Any]:
    """
    Read-only resolution of a PM client id: regular orders via
    ``origClientOrderId`` (needs ``--symbol``), conditional algo orders via
    ``algoId``. Used to explain unknown user-stream order events before
    clearing a fail-closed block - never to cancel or modify anything.
    """
    outcome: dict[str, Any] = {"client_id": client_id}
    if symbol:
        try:
            outcome["regular_order"] = signed_get(
                "/papi/v1/um/order",
                {"symbol": symbol, "origClientOrderId": client_id},
            )
        except ProbeError as exc:
            outcome["regular_order"] = {"error": str(exc)}
    else:
        outcome["regular_order"] = {
            "skipped": "pass --symbol to query /um/order by origClientOrderId"
        }
    try:
        outcome["conditional_order"] = signed_get(
            "/papi/v1/um/algo/algoOrder", {"algoId": client_id}
        )
    except ProbeError as exc:
        outcome["conditional_order"] = {"error": str(exc)}
    return outcome


def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    results: dict[str, Any] = {"endpoint": BASE_URL, "timestamp": int(time.time() * 1000)}

    if args.list_all or args.account:
        results["account"] = probe_account()

    if args.list_all or args.balance:
        results["balance"] = probe_balance()

    if args.list_all or args.positions:
        results["positions"] = probe_positions(args.symbol)

    if args.list_all or args.api_trading_status:
        results["api_trading_status"] = probe_api_trading_status()

    if args.list_all or args.account_config:
        results["account_config"] = probe_account_config()

    if args.list_all or args.symbol_config:
        results["symbol_config"] = probe_symbol_config(args.symbol)

    if args.list_all or args.algo_stoploss:
        results["algo_stoploss"] = probe_algo_stoploss()

    if args.list_all or args.listen_key_test:
        results["listen_key"] = probe_listen_key_lifecycle()

    if args.resolve_client_id:
        results["client_order"] = probe_client_order_id(args.resolve_client_id, args.symbol)

    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Binance PM production probe (read-only safe for restricted API keys)."
    )
    parser.add_argument(
        "--all", dest="list_all", action="store_true", help="Probe all available endpoints."
    )
    parser.add_argument(
        "--account", action="store_true", help="Probe /papi/v1/account (PM risk metrics)."
    )
    parser.add_argument(
        "--balance", action="store_true", help="Probe /papi/v1/balance (cross-margin balances)."
    )
    parser.add_argument(
        "--positions", action="store_true", help="Probe /papi/v1/um/positionRisk (UM positions)."
    )
    parser.add_argument("--symbol", help="Filter positions by symbol, e.g. BTCUSDT.")
    parser.add_argument(
        "--api-trading-status",
        action="store_true",
        help="Probe /papi/v1/um/apiTradingStatus (trading lock/restriction).",
    )
    parser.add_argument(
        "--account-config",
        action="store_true",
        help="Probe /papi/v1/um/accountConfig (position mode / asset mode).",
    )
    parser.add_argument(
        "--symbol-config",
        action="store_true",
        help="Probe /papi/v1/um/symbolConfig (symbol trading/margin config).",
    )
    parser.add_argument(
        "--algo-stoploss",
        action="store_true",
        help="Read-only check of /papi/v1/um/algo/openAlgoOrders for exchange stoplosses.",
    )
    parser.add_argument(
        "--listen-key-test",
        action="store_true",
        help="Test listenKey create/keepalive/delete lifecycle.",
    )
    parser.add_argument(
        "--resolve-client-id",
        metavar="CLIENT_ID",
        help="Read-only lookup of a client order id (origClientOrderId / algoId) "
        "to explain an unknown user-stream event. Use with --symbol for "
        "regular orders.",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser.add_argument(
        "--minimal", action="store_true", help="Only show non-zero balances and non-zero positions."
    )
    args = parser.parse_args()

    if not any(
        [
            args.list_all,
            args.account,
            args.balance,
            args.positions,
            args.api_trading_status,
            args.account_config,
            args.symbol_config,
            args.algo_stoploss,
            args.listen_key_test,
            args.resolve_client_id,
        ]
    ):
        args.list_all = True

    try:
        results = run_probe(args)
        if args.minimal:
            if "balance" in results:
                results["balance"].pop("raw", None)
                del results["balance"]["total_count"]

        print(json.dumps(results, indent=2 if args.pretty else None, sort_keys=True))
        return 0
    except ProbeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
