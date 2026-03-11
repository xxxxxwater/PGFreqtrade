from freqtrade.exchange.coinbase_advanced_compat import (
    build_coinbase_symbol_candidates,
    infer_coinbase_max_leverage,
    is_coinbase_futures_market,
    normalize_coinbase_order_params,
    normalize_coinbase_position,
)


def test_build_coinbase_symbol_candidates_adds_settle_suffix():
    vals = build_coinbase_symbol_candidates('BTC/USDC', 'USDC')
    assert 'BTC/USDC' in vals
    assert 'BTC/USDC:USDC' in vals


def test_is_coinbase_futures_market_filters_inverse_and_settle():
    assert is_coinbase_futures_market({'swap': True, 'inverse': False, 'settle': 'USDC'}, 'USDC')
    assert not is_coinbase_futures_market({'swap': True, 'inverse': True, 'settle': 'USDC'}, 'USDC')
    assert not is_coinbase_futures_market({'swap': True, 'inverse': False, 'settle': 'USD'}, 'USDC')


def test_normalize_coinbase_position_sets_defaults():
    pos = normalize_coinbase_position({'info': {'number_of_contracts': '2', 'leverage': '3', 'side': 'LONG'}}, 'isolated')
    assert pos['contracts'] == 2.0
    assert pos['leverage'] == 3.0
    assert pos['side'] == 'long'
    assert pos['marginMode'] == 'isolated'


def test_normalize_coinbase_order_params_adds_futures_fields():
    params = normalize_coinbase_order_params(
        trading_mode='futures',
        margin_mode='isolated',
        time_in_force='PO',
        leverage=2.0,
        reduce_only=True,
        params={'timeInForce': 'PO'},
    )
    assert params['postOnly'] is True
    assert params['reduceOnly'] is True
    assert params['marginMode'] == 'isolated'
    assert params['leverage'] == 2.0


def test_infer_coinbase_max_leverage_uses_intraday_margin_rate():
    lev = infer_coinbase_max_leverage({'info': {'intraday_margin_rate': '0.2'}}, default=3.0)
    assert lev == 5.0
