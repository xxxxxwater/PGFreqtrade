"""Typed helper models / normalizers for Coinbase Advanced test-branch futures support."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class CoinbaseAdvancedPositionView:
    symbol: str
    side: str
    contracts: float
    leverage: float
    collateral: float | None = None
    initial_margin: float | None = None
    maintenance_margin: float | None = None
    liquidation_price: float | None = None
    margin_mode: str | None = None
    notional: float | None = None

    @classmethod
    def from_ccxt(cls, position: dict[str, Any], default_margin_mode: str) -> "CoinbaseAdvancedPositionView":
        info = position.get("info", {}) if isinstance(position, dict) else {}

        def _f(v, default=None):
            try:
                return float(v)
            except Exception:
                return default

        contracts = position.get("contracts")
        if contracts is None:
            contracts = position.get("contractSize") or info.get("number_of_contracts") or position.get("amount")

        leverage = position.get("leverage")
        if leverage in (None, ""):
            leverage = info.get("leverage") or 1.0

        collateral = position.get("collateral", info.get("collateral"))
        initial_margin = position.get("initialMargin", info.get("initial_margin"))
        maintenance_margin = position.get("maintenanceMargin", info.get("maintenance_margin"))
        liquidation_price = position.get("liquidationPrice", info.get("liquidation_price"))
        notional = position.get("notional", info.get("notional"))
        side = str(position.get("side") or info.get("side") or "").lower()

        return cls(
            symbol=position.get("symbol", ""),
            side=side,
            contracts=_f(contracts, 0.0) or 0.0,
            leverage=_f(leverage, 1.0) or 1.0,
            collateral=_f(collateral),
            initial_margin=_f(initial_margin),
            maintenance_margin=_f(maintenance_margin),
            liquidation_price=_f(liquidation_price),
            margin_mode=position.get("marginMode") or info.get("margin_mode") or default_margin_mode,
            notional=_f(notional),
        )

    def to_ccxt_position(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "contracts": self.contracts,
            "leverage": self.leverage,
            "collateral": self.collateral,
            "initialMargin": self.initial_margin,
            "maintenanceMargin": self.maintenance_margin,
            "liquidationPrice": self.liquidation_price,
            "marginMode": self.margin_mode,
            "notional": self.notional,
        }
