from freqtrade.exchange.coinbase_advanced_models import CoinbaseAdvancedPositionView


def test_coinbase_position_view_from_ccxt():
    pos = CoinbaseAdvancedPositionView.from_ccxt(
        {
            'symbol': 'BTC/USDC:USDC',
            'info': {
                'number_of_contracts': '3',
                'leverage': '2',
                'side': 'SHORT',
                'liquidation_price': '45000',
            },
        },
        'isolated',
    )
    assert pos.symbol == 'BTC/USDC:USDC'
    assert pos.contracts == 3.0
    assert pos.leverage == 2.0
    assert pos.side == 'short'
    assert pos.margin_mode == 'isolated'
    assert pos.liquidation_price == 45000.0
