from pathlib import Path


def test_coinbase_advanced_config_audit_script_exists():
    p = Path('/data/test_coinbaseadvanced/PGFreqtrade/scripts/coinbase_advanced_config_audit.py')
    assert p.exists()
    text = p.read_text()
    assert 'Coinbase Advanced futures config audit' in text
