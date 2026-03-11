from pathlib import Path


def test_coinbase_advanced_market_sync_check_exists():
    p = Path('/data/test_coinbaseadvanced/PGFreqtrade/scripts/coinbase_advanced_market_sync_check.py')
    assert p.exists()
    assert 'Market Sync Check' in p.read_text()
