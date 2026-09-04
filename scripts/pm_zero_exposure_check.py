#!/usr/bin/env python3
"""Zero-exposure precheck for the PM live stack (read-only, prints counts only).

Reads credentials from the mounted Docker secret file directly (never prints
them) and queries the three things that must all be zero before a bot
container restart:
  1. nonzero UM positions        /papi/v1/um/positionRisk
  2. open UM orders              /papi/v1/um/openOrders
  3. open UM algo (stop) orders  /papi/v1/um/algo/openAlgoOrders
Exit code 0 when exposure is zero everywhere, 3 otherwise, 2 on errors.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = "https://papi.binance.com"
SECRET_FILE = "/run/secrets/pm_env"


def load_credentials(path: str) -> tuple[str, str]:
    api_key = ""
    api_secret = ""
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            name, value = name.strip(), value.strip()
            upper = name.upper()
            if not api_key and upper.endswith("KEY") and ("EXCHANGE" in upper or "API" in upper):
                api_key = value
            elif not api_secret and "SECRET" in upper:
                api_secret = value
    if not api_key or not api_secret:
        raise RuntimeError("credentials not found in secret file")
    return api_key, api_secret


def signed_get(api_key: str, api_secret: str, path: str):
    params = {"recvWindow": 5000, "timestamp": int(time.time() * 1000)}
    query = urllib.parse.urlencode(params)
    signature = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request(
        f"{BASE_URL}{path}?{query}&signature={signature}",
        headers={"X-MBX-APIKEY": api_key},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode())


def main() -> int:
    try:
        api_key, api_secret = load_credentials(SECRET_FILE)
        positions = signed_get(api_key, api_secret, "/papi/v1/um/positionRisk")
        open_orders = signed_get(api_key, api_secret, "/papi/v1/um/openOrders")
        try:
            algo_orders = signed_get(api_key, api_secret, "/papi/v1/um/algo/openAlgoOrders")
            algo_count = len(algo_orders) if isinstance(algo_orders, list) else -1
        except urllib.error.HTTPError as exc:
            algo_count = -1
            print(f"WARN algo endpoint HTTP {exc.code}", file=sys.stderr)
        nonzero_positions = [
            p for p in positions
            if float(p.get("positionAmt") or 0) != 0.0
        ]
        result = {
            "nonzero_positions": len(nonzero_positions),
            "position_symbols": sorted({p.get("symbol") for p in nonzero_positions}),
            "open_orders": len(open_orders) if isinstance(open_orders, list) else -1,
            "open_algo_orders": algo_count,
        }
        print(json.dumps(result, sort_keys=True))
        # Any channel the script could NOT verify (value -1) makes the whole
        # answer "unknown": production stop/upgrade flows must treat unknown as
        # NOT safe - never as zero exposure.
        exposure_unknown = any(
            result[key] < 0 for key in ("open_orders", "open_algo_orders")
        )
        if exposure_unknown:
            print(
                "ERROR: exposure could not be fully verified "
                f"(result={result}); refusing to report zero exposure.",
                file=sys.stderr,
            )
            return 2
        exposure = (
            result["nonzero_positions"]
            + result["open_orders"]
            + result["open_algo_orders"]
        )
        return 0 if exposure == 0 else 3
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
