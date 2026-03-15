from freqtrade.exchange.coinbase_advanced_compat import (
    build_coinbase_close_position_params,
    get_coinbase_product_details,
    infer_coinbase_max_leverage,
    normalize_coinbase_exit_params,
)
from freqtrade.exchange.coinbase_advanced_models import CoinbaseAdvancedPositionView



def test_product_details_parse_future_product_details_intraday_margin_rate():
    market = {
        'info': {
            'future_product_details': {
                'intraday_margin_rate': '0.1',
                'overnight_margin_rate': '0.2',
                'contract_expiry_type': 'PERPETUAL',
            }
        }
    }
    details = get_coinbase_product_details(market)
    assert details.intraday_margin_rate == 0.1
    assert details.overnight_margin_rate == 0.2
    assert details.contract_expiry_type == 'PERPETUAL'
    assert details.max_leverage == 10.0



def test_position_view_parses_net_size_and_position_side():
    pos = CoinbaseAdvancedPositionView.from_ccxt(
        {
            'info': {
                'product_id': 'BTC/USDC:USDC',
                'net_size': '-2',
                'position_side': 'SHORT',
                'liquidation_price': '50000',
            }
        },
        'isolated',
    )
    assert pos.symbol == 'BTC/USDC:USDC'
    assert pos.side == 'short'
    assert pos.contracts == 2.0
    assert pos.liquidation_price == 50000.0



def test_close_position_params_supported():
    params = build_coinbase_close_position_params(side='SELL')
    assert params['close_position'] is True
    assert params['side'] == 'sell'



def test_exit_params_can_switch_to_close_position_flow():
    params = normalize_coinbase_exit_params(
        margin_mode='isolated',
        leverage=2.0,
        time_in_force='GTC',
        allow_close_position=True,
        side='BUY',
    )
    assert params['close_position'] is True
    assert params['side'] == 'buy'
    assert params['reduceOnly'] is False



def test_infer_max_leverage_uses_nested_product_details():
    market = {
        'info': {
            'perpetual_details': {
                'intraday_margin_rate': '0.05'
            }
        }
    }
    assert infer_coinbase_max_leverage(market, default=3.0) == 20.0
