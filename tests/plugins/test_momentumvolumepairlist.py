from typing import Any

import pytest

from freqtrade.exceptions import OperationalException
from freqtrade.plugins.pairlist.MomentumVolumePairList import MomentumVolumePairList
from freqtrade.resolvers import PairListResolver


class DummyExchange:
    name = "Binance"

    def __init__(
        self,
        markets: dict[str, dict[str, Any]],
        *,
        supports_tickers: bool = True,
        pm_pairs: set[str] | None = None,
    ) -> None:
        self.markets = markets
        self._supports_tickers = supports_tickers
        self._pm_pairs = pm_pairs

    def exchange_has(self, endpoint: str) -> bool:
        return endpoint == "fetchTickers" and self._supports_tickers

    def get_option(self, option: str) -> bool:
        return option == "tickers_have_quoteVolume"

    def market_is_future(self, market: dict[str, Any]) -> bool:
        return (
            market.get("future") is True
            and market.get("type") == "swap"
            and market.get("swap") is True
            and market.get("linear") is True
            and market.get("settle") in {"USDT", "USDC"}
        )

    def market_is_tradable(self, market: dict[str, Any]) -> bool:
        return bool(market.get("tradable", True)) and self.market_is_future(market)

    def get_pair_quote_currency(self, pair: str) -> str:
        return self.markets[pair]["quote"]

    def get_pm_tradable_pairs(self) -> set[str]:
        return self._pm_pairs or set()

    def get_markets(
        self,
        base_currencies=None,
        quote_currencies=None,
        spot_only=False,
        margin_only=False,
        futures_only=False,
        tradable_only=True,
        active_only=False,
    ) -> dict[str, dict[str, Any]]:
        markets = self.markets.copy()
        if quote_currencies:
            markets = {
                pair: market
                for pair, market in markets.items()
                if market["quote"] in quote_currencies
            }
        if futures_only:
            markets = {
                pair: market for pair, market in markets.items() if self.market_is_future(market)
            }
        if tradable_only:
            markets = {
                pair: market for pair, market in markets.items() if self.market_is_tradable(market)
            }
        if active_only:
            markets = {
                pair: market for pair, market in markets.items() if market.get("active", True)
            }
        return markets


class DummyPairListManager:
    def __init__(self, blacklist: list[str] | None = None) -> None:
        self._blacklist = blacklist or []

    def verify_blacklist(self, pairlist: list[str], _logmethod) -> list[str]:
        return [pair for pair in pairlist if pair not in self._blacklist]


def future_market(
    symbol: str,
    *,
    quote: str = "USDT",
    settle: str = "USDT",
    active: bool = True,
    tradable: bool = True,
    future: bool = True,
    swap: bool = True,
    linear: bool = True,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "base": symbol.split("/")[0],
        "quote": quote,
        "settle": settle,
        "active": active,
        "tradable": tradable,
        "future": future,
        "type": "swap" if swap else "future",
        "swap": swap,
        "linear": linear,
        "precision": {"price": 0.01},
    }


def build_pairlist(
    markets: dict[str, dict[str, Any]],
    pairlist_config: dict[str, Any] | None = None,
    *,
    trading_mode: str = "futures",
    supports_tickers: bool = True,
    pm_pairs: set[str] | None = None,
) -> MomentumVolumePairList:
    config = {
        "stake_currency": "USDT",
        "trading_mode": trading_mode,
        "exchange": {"pair_blacklist": []},
    }
    config_pairlist = {
        "method": "MomentumVolumePairList",
        "number_assets": 20,
        "min_quote_volume": 200_000_000,
        "min_momentum": None,
        "refresh_period": 900,
    }
    config_pairlist.update(pairlist_config or {})
    return MomentumVolumePairList(
        DummyExchange(markets, supports_tickers=supports_tickers, pm_pairs=pm_pairs),
        DummyPairListManager(),
        config,
        config_pairlist,
        0,
    )


def test_selects_top_momentum_linear_usdt_perpetuals_after_volume_screen() -> None:
    markets = {
        "BTC/USDT:USDT": future_market("BTC/USDT:USDT"),
        "ETH/USDT:USDT": future_market("ETH/USDT:USDT"),
        # The selector intentionally does not hard-code an underlying asset category.
        "AAPL/USDT:USDT": future_market("AAPL/USDT:USDT"),
        "SOL/USDT:USDT": future_market("SOL/USDT:USDT"),
        "LOWVOL/USDT:USDT": future_market("LOWVOL/USDT:USDT"),
        "DOWN/USDT:USDT": future_market("DOWN/USDT:USDT"),
        "USDC/USDT:USDC": future_market("USDC/USDT:USDC", settle="USDC"),
        "DELIVERY/USDT:USDT": future_market("DELIVERY/USDT:USDT", swap=False),
        "INACTIVE/USDT:USDT": future_market("INACTIVE/USDT:USDT", active=False),
    }
    pairlist = build_pairlist(markets, {"number_assets": 3})

    tickers = {
        "BTC/USDT:USDT": {"quoteVolume": "200000000", "percentage": "5"},
        "ETH/USDT:USDT": {"quoteVolume": "300000000", "percentage": "5"},
        "AAPL/USDT:USDT": {"quoteVolume": "220000000", "percentage": "7"},
        # Verify the documented open/last fallback when an adapter does not map percentage.
        "SOL/USDT:USDT": {"quoteVolume": "400000000", "open": "100", "last": "104"},
        "LOWVOL/USDT:USDT": {"quoteVolume": "199999999.99", "percentage": "99"},
        "DOWN/USDT:USDT": {"quoteVolume": "500000000", "percentage": "-1"},
        "USDC/USDT:USDC": {"quoteVolume": "900000000", "percentage": "100"},
        "DELIVERY/USDT:USDT": {"quoteVolume": "900000000", "percentage": "100"},
        "INACTIVE/USDT:USDT": {"quoteVolume": "900000000", "percentage": "100"},
    }

    assert pairlist.gen_pairlist(tickers) == [
        "AAPL/USDT:USDT",
        "ETH/USDT:USDT",
        "BTC/USDT:USDT",
    ]


def test_allows_negative_momentum_unless_an_explicit_floor_is_configured() -> None:
    markets = {"DOWN/USDT:USDT": future_market("DOWN/USDT:USDT")}
    tickers = {"DOWN/USDT:USDT": {"quoteVolume": 200_000_000, "percentage": -1.5}}

    assert build_pairlist(markets).gen_pairlist(tickers) == [
        "DOWN/USDT:USDT"
    ]
    assert build_pairlist(markets, {"min_momentum": 0}).gen_pairlist(tickers) == []


def test_pm_mode_keeps_only_account_enabled_perpetuals_without_asset_type_filter() -> None:
    markets = {
        "BTC/USDT:USDT": future_market("BTC/USDT:USDT"),
        "AAPL/USDT:USDT": future_market("AAPL/USDT:USDT"),
    }
    config = {
        "stake_currency": "USDT",
        "trading_mode": "futures",
        "exchange": {"pair_blacklist": [], "portfolio_margin": True},
    }
    pairlist = MomentumVolumePairList(
        DummyExchange(markets, pm_pairs={"AAPL/USDT:USDT"}),
        DummyPairListManager(),
        config,
        {"method": "MomentumVolumePairList", "number_assets": 20},
        0,
    )
    tickers = {
        "BTC/USDT:USDT": {"quoteVolume": 900_000_000, "percentage": 20},
        "AAPL/USDT:USDT": {"quoteVolume": 200_000_000, "percentage": 1},
    }

    assert pairlist.gen_pairlist(tickers) == ["AAPL/USDT:USDT"]


def test_rejects_non_futures_and_unsupported_ticker_sources() -> None:
    markets = {"BTC/USDT:USDT": future_market("BTC/USDT:USDT")}

    with pytest.raises(OperationalException, match="requires trading_mode='futures'"):
        build_pairlist(markets, trading_mode="spot")
    with pytest.raises(OperationalException, match="does not support fetchTickers"):
        build_pairlist(markets, supports_tickers=False)


def test_resolver_loads_builtin_pairlist() -> None:
    markets = {"BTC/USDT:USDT": future_market("BTC/USDT:USDT")}
    config = {
        "stake_currency": "USDT",
        "trading_mode": "futures",
        "exchange": {"pair_blacklist": []},
    }
    pairlist = PairListResolver.load_pairlist(
        "MomentumVolumePairList",
        exchange=DummyExchange(markets),
        pairlistmanager=DummyPairListManager(),
        config=config,
        pairlistconfig={"number_assets": 20},
        pairlist_pos=0,
    )

    # Resolvers import builtin plugins by file path, so the resolved class lives in a
    # separately loaded module from the direct import used by the other unit tests.
    assert pairlist.name == "MomentumVolumePairList"
