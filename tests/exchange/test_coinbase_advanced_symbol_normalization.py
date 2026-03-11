from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exchange.coinbase import Coinbase


class _DummyCoinbase(Coinbase):
    @property
    def markets(self):
        return {
            'BTC/USDC': {'spot': True, 'quote': 'USDC'},
            'BTC/USDC:USDC': {'swap': True, 'contract': True, 'quote': 'USDC', 'settle': 'USDC'},
        }


def test_normalize_pair_for_futures_suffix():
    inst = object.__new__(_DummyCoinbase)
    inst._config = {'stake_currency': 'USDC'}
    inst.trading_mode = TradingMode.FUTURES
    inst.margin_mode = MarginMode.ISOLATED
    assert inst.normalize_pair_for_trading('BTC/USDC') == 'BTC/USDC:USDC'


def test_normalize_pair_spot_keeps_plain_pair():
    inst = object.__new__(_DummyCoinbase)
    inst._config = {'stake_currency': 'USDC'}
    inst.trading_mode = TradingMode.SPOT
    inst.margin_mode = MarginMode.NONE
    assert inst.normalize_pair_for_trading('BTC/USDC') == 'BTC/USDC'
