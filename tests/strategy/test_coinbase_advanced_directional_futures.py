from pathlib import Path


def test_directional_futures_strategy_exists():
    p = Path('/data/test_coinbaseadvanced/PGFreqtrade/user_data/strategies/CoinbaseAdvancedDirectionalFutures.py')
    assert p.exists()
    text = p.read_text()
    assert 'can_short = True' in text
    assert 'cbadv_fut_long_pullback' in text
    assert 'cbadv_fut_short_reject' in text
