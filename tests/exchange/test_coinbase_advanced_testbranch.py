from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exchange.coinbase import Coinbase


def test_coinbase_testbranch_supports_spot_and_isolated_futures():
    assert (TradingMode.SPOT, MarginMode.NONE) in Coinbase._supported_trading_mode_margin_pairs
    assert (TradingMode.FUTURES, MarginMode.ISOLATED) in Coinbase._supported_trading_mode_margin_pairs


def test_coinbase_testbranch_has_futures_capabilities():
    assert "order_time_in_force" in Coinbase._ft_has
    assert "order_time_in_force" in Coinbase._ft_has_futures
    assert Coinbase._ft_has_futures["uses_leverage_tiers"] is False
    assert Coinbase._ft_has_futures["mark_ohlcv_price"] == "mark"
