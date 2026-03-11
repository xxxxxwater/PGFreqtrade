from pathlib import Path


def test_gap_check_script_exists():
    p = Path('/data/test_coinbaseadvanced/PGFreqtrade/scripts/coinbase_advanced_gap_check.py')
    assert p.exists()
    assert 'Known Gaps' in p.read_text()
