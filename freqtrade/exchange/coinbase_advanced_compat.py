"""Coinbase Advanced compatibility helpers for the test branch.

Purpose:
- Keep Coinbase Advanced specific normalization separate from the generic
  Freqtrade exchange layer.
- Make future CCXT upgrades easier to adapt in one place.
- Capture assumptions derived from Coinbase Developer Platform / Advanced Trade
  docs in a project-local compatibility module.

This module does not replace CCXT. It wraps / normalizes CCXT unified outputs so
Freqtrade's internal futures flow can consume them more consistently.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


def is_coinbase_futures_market(market: dict[str, Any], stake_currency: str | None = None) -> bool:
    if not isinstance(market, dict):
        return False
    if market.get("inverse"):
        return False
    if not (market.get("swap") or market.get("future") or market.get("contract")):
        return False
    if stake_currency:
        settle = market.get("settle") or market.get("quote")
        if settle and settle != stake_currency:
            return False
    return True


def build_coinbase_symbol_candidates(pair: str, stake_currency: str | None = None) -> list[str]:
    candidates: list[str] = []
    if not pair:
        return candidates

    candidates.append(pair)
    if ":" not in pair and "/" in pair:
        _, quote = pair.split("/", 1)
        candidates.append(f"{pair}:{quote}")
        if stake_currency:
            candidates.append(f"{pair}:{stake_currency}")

    # preserve order while deduplicating
    return list(dict.fromkeys(candidates))


def normalize_coinbase_position(position: dict[str, Any], default_margin_mode: str) -> dict[str, Any]:
    p = deepcopy(position)

    contracts = p.get("contracts")
    if contracts is None:
        contracts = p.get("contractSize") or p.get("info", {}).get("number_of_contracts")
    if contracts is None and p.get("amount") is not None:
        contracts = p.get("amount")
    p["contracts"] = float(contracts or 0.0)

    leverage = p.get("leverage")
    if leverage in (None, ""):
        leverage = p.get("info", {}).get("leverage") or 1.0
    try:
        p["leverage"] = float(leverage)
    except Exception:
        p["leverage"] = 1.0

    margin_mode = p.get("marginMode") or p.get("info", {}).get("margin_mode")
    p["marginMode"] = margin_mode or default_margin_mode

    side = p.get("side") or p.get("info", {}).get("side")
    if side:
        p["side"] = str(side).lower()

    # Standardized collateral-style fields commonly used by Freqtrade futures flow.
    for key in ("collateral", "initialMargin", "liquidationPrice"):
        if key not in p:
            p[key] = p.get("info", {}).get(key)

    return p


def normalize_coinbase_balances(raw_balances: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for currency, value in raw_balances.items():
        if currency in {"info", "free", "used", "total", "timestamp", "datetime"}:
            continue
        if not isinstance(value, dict):
            continue
        normalized[currency] = {
            "free": float(value.get("free") or 0.0),
            "used": float(value.get("used") or 0.0),
            "total": float(value.get("total") or 0.0),
        }
    return normalized


def normalize_coinbase_order_params(
    *,
    trading_mode: str,
    margin_mode: str,
    time_in_force: str,
    leverage: float,
    reduce_only: bool,
    params: dict[str, Any],
) -> dict[str, Any]:
    p = deepcopy(params)

    if time_in_force == "PO":
        p.pop("timeInForce", None)
        p["postOnly"] = True

    if trading_mode == "futures":
        p["reduceOnly"] = reduce_only
        p["marginMode"] = margin_mode
        if leverage and leverage > 1.0:
            p["leverage"] = leverage

    return p


def infer_coinbase_max_leverage(market: dict[str, Any], default: float = 3.0) -> float:
    limits = market.get("limits", {}) if isinstance(market, dict) else {}
    lev = (limits.get("leverage") or {}).get("max")
    if lev:
        return float(lev)

    info = market.get("info", {}) if isinstance(market, dict) else {}
    for key in ("max_leverage", "maxLeverage", "intraday_margin_rate"):
        val = info.get(key)
        if val not in (None, ""):
            try:
                if key == "intraday_margin_rate":
                    rate = float(val)
                    return round(1.0 / rate, 8) if rate > 0 else 1.0
                return float(val)
            except Exception:
                continue
    return default
