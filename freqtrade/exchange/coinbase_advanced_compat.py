"""Coinbase Advanced compatibility helpers for the test branch."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from freqtrade.exchange.coinbase_advanced_models import (
    CoinbaseAdvancedPositionView,
    CoinbaseAdvancedProductDetails,
)


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
    return list(dict.fromkeys(candidates))


def normalize_coinbase_position(position: dict[str, Any], default_margin_mode: str) -> dict[str, Any]:
    return CoinbaseAdvancedPositionView.from_ccxt(position, default_margin_mode).to_ccxt_position()


def normalize_coinbase_positions(positions: list[dict[str, Any]], default_margin_mode: str) -> list[dict[str, Any]]:
    return [normalize_coinbase_position(p, default_margin_mode) for p in positions]


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


def build_coinbase_close_position_params(*, side: str | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {"close_position": True}
    if side:
        params["side"] = side
    return params


def normalize_coinbase_entry_params(*, margin_mode: str, leverage: float, time_in_force: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    return normalize_coinbase_order_params(
        trading_mode="futures",
        margin_mode=margin_mode,
        time_in_force=time_in_force,
        leverage=leverage,
        reduce_only=False,
        params=params or {},
    )


def normalize_coinbase_exit_params(*, margin_mode: str, leverage: float, time_in_force: str, params: dict[str, Any] | None = None, allow_close_position: bool = False, side: str | None = None) -> dict[str, Any]:
    merged = deepcopy(params or {})
    if allow_close_position:
        merged.update(build_coinbase_close_position_params(side=side))
    return normalize_coinbase_order_params(
        trading_mode="futures",
        margin_mode=margin_mode,
        time_in_force=time_in_force,
        leverage=leverage,
        reduce_only=not allow_close_position,
        params=merged,
    )


def get_coinbase_product_details(market: dict[str, Any]) -> CoinbaseAdvancedProductDetails:
    return CoinbaseAdvancedProductDetails.from_market(market)


def infer_coinbase_max_leverage(market: dict[str, Any], default: float = 3.0) -> float:
    limits = market.get("limits", {}) if isinstance(market, dict) else {}
    lev = (limits.get("leverage") or {}).get("max")
    if lev:
        return float(lev)
    details = get_coinbase_product_details(market)
    if details.max_leverage:
        return details.max_leverage
    return default


def infer_coinbase_maintenance_ratio(market: dict[str, Any], default: float = 0.02) -> float:
    details = get_coinbase_product_details(market)
    if details.maintenance_margin_rate is not None:
        return details.maintenance_margin_rate
    return default


def normalize_coinbase_close_order_side(is_short: bool) -> str:
    return "buy" if is_short else "sell"


def normalize_coinbase_open_order_side(is_short: bool) -> str:
    return "sell" if is_short else "buy"
