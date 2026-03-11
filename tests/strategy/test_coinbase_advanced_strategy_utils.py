from coinbase_advanced_strategy_utils import (
    default_coinbase_btc_reference_pair,
    normalize_coinbase_futures_pair,
    normalize_coinbase_futures_pairs,
)


def test_normalize_coinbase_futures_pair_adds_settle_suffix():
    assert normalize_coinbase_futures_pair('BTC/USDC') == 'BTC/USDC:USDC'


def test_normalize_coinbase_futures_pairs_deduplicates():
    vals = normalize_coinbase_futures_pairs(['BTC/USDC', 'BTC/USDC:USDC'])
    assert vals == ['BTC/USDC:USDC']


def test_default_coinbase_btc_reference_pair():
    assert default_coinbase_btc_reference_pair('USDC') == 'BTC/USDC:USDC'
