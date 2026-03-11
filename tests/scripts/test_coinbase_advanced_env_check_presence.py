from pathlib import Path


def test_coinbase_advanced_env_check_script_exists():
    p = Path('/data/test_coinbaseadvanced/PGFreqtrade/scripts/coinbase_advanced_env_check.py')
    assert p.exists()
    text = p.read_text()
    assert 'environment is not prepared to run them' in text
