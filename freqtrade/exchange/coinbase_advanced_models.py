"""Typed helper models / normalizers for Coinbase Advanced test-branch futures support."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class CoinbaseAdvancedProductDetails:
    product_type: str | None = None
    contract_expiry_type: str | None = None
    contract_size: float | None = None
    underlying_type: str | None = None
    venue: str | None = None
    region_enabled: bool | None = None
    intraday_margin_rate: float | None = None
    overnight_margin_rate: float | None = None
    max_leverage: float | None = None
    maintenance_margin_rate: float | None = None

    @classmethod
    def from_market(cls, market: dict[str, Any]) -> "CoinbaseAdvancedProductDetails":
        info = (market or {}).get("info", {}) if isinstance(market, dict) else {}
        future = info.get("future_product_details") or {}
        perp = info.get("perpetual_details") or {}
        details = future or perp

        def _f(v):
            try:
                return float(v)
            except Exception:
                return None

        intraday_margin_rate = _f(details.get("intraday_margin_rate") or info.get("intraday_margin_rate"))
        overnight_margin_rate = _f(details.get("overnight_margin_rate") or info.get("overnight_margin_rate"))
        maintenance_margin_rate = _f(
            details.get("maintenance_margin_rate")
            or details.get("maintenanceMarginRate")
            or info.get("maintenance_margin_rate")
            or info.get("maintenanceMarginRate")
            or info.get("mmr")
        )

        max_leverage = None
        if intraday_margin_rate and intraday_margin_rate > 0:
            max_leverage = round(1.0 / intraday_margin_rate, 8)
        else:
            max_leverage = _f(details.get("max_leverage") or details.get("maxLeverage") or info.get("max_leverage") or info.get("maxLeverage"))

        return cls(
            product_type=info.get("product_type") or details.get("product_type"),
            contract_expiry_type=details.get("contract_expiry_type") or info.get("contract_expiry_type"),
            contract_size=_f(market.get("contractSize") or details.get("contract_size") or info.get("contract_size")),
            underlying_type=details.get("underlying_type") or info.get("underlying_type"),
            venue=details.get("venue") or info.get("venue"),
            region_enabled=details.get("region_enabled") if isinstance(details.get("region_enabled"), bool) else None,
            intraday_margin_rate=intraday_margin_rate,
            overnight_margin_rate=overnight_margin_rate,
            max_leverage=max_leverage,
            maintenance_margin_rate=maintenance_margin_rate,
        )


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
    net_size: float | None = None

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
            contracts = (
                info.get("number_of_contracts")
                or info.get("num_contracts")
                or info.get("net_size")
                or position.get("amount")
            )

        leverage = position.get("leverage")
        if leverage in (None, ""):
            leverage = info.get("leverage") or 1.0

        collateral = position.get("collateral", info.get("collateral"))
        initial_margin = position.get("initialMargin", info.get("initial_margin"))
        maintenance_margin = position.get("maintenanceMargin", info.get("maintenance_margin"))
        liquidation_price = position.get("liquidationPrice", info.get("liquidation_price"))
        notional = position.get("notional", info.get("notional"))
        net_size = position.get("net_size", info.get("net_size"))
        side = str(
            position.get("side")
            or info.get("position_side")
            or info.get("side")
            or ("long" if (_f(net_size, 0.0) or 0.0) > 0 else "short" if (_f(net_size, 0.0) or 0.0) < 0 else "")
        ).lower()

        return cls(
            symbol=position.get("symbol", info.get("product_id", "")),
            side=side,
            contracts=abs(_f(contracts, 0.0) or 0.0),
            leverage=_f(leverage, 1.0) or 1.0,
            collateral=_f(collateral),
            initial_margin=_f(initial_margin),
            maintenance_margin=_f(maintenance_margin),
            liquidation_price=_f(liquidation_price),
            margin_mode=position.get("marginMode") or info.get("margin_mode") or default_margin_mode,
            notional=_f(notional),
            net_size=_f(net_size),
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
            "net_size": self.net_size,
        }
