from pathlib import Path


def test_wallets_coinbase_futures_consumption_updated():
    p = Path('/data/test_coinbaseadvanced/PGFreqtrade/freqtrade/wallets.py')
    text = p.read_text()
    assert 'effective_contracts = contracts or net_size' in text
    assert 'maintenanceMargin' in text


def test_freqtradebot_coinbase_exit_flow_note_present():
    p = Path('/data/test_coinbaseadvanced/PGFreqtrade/freqtrade/freqtradebot.py')
    text = p.read_text()
    assert 'Coinbase futures exit path' in text
    assert 'close_position semantics' in text
