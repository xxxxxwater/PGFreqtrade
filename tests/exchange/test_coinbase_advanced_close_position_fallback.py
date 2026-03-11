from freqtrade.exchange.coinbase_advanced_compat import (
    build_coinbase_close_position_params,
    should_use_coinbase_close_position_fallback,
)


def test_should_use_close_position_fallback_on_preview_error():
    assert should_use_coinbase_close_position_fallback('PREVIEW_REDUCE_ONLY_NOT_ALLOWED_ON_VENUE')


def test_build_close_position_params_contains_flag_and_side():
    params = build_coinbase_close_position_params(side='BUY')
    assert params['close_position'] is True
    assert params['side'] == 'BUY'
