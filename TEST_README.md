# PGFreqtrade Test Branch Notes

Root:
- `/data/test_coinbaseadvanced/PGFreqtrade`

This tree is the **experimental Coinbase Advanced integration branch**.

## What to read first

1. `TEST_BRANCH_COINBASE_ADVANCED_CHANGELOG.md`
2. `docs/coinbase_advanced_futures_test.md`
3. `freqtrade/exchange/coinbase.py`
4. `user_data/config.coinbase_advanced_futures.example.json`
5. `user_data/config.coinbase_advanced_futures.live-template.json`
6. `user_data/strategies/VWAP_V4_CoinbaseAdvancedFutures.py`

## Helpful scripts

- `scripts/coinbase_advanced_probe.py`
- `scripts/coinbase_advanced_market_report.py`

## Important boundary

Do **not** treat this tree as production-ready.
This tree exists for:
- interface extension
- Coinbase Advanced futures adaptation
- strategy migration experiments
- test-only container/devcontainer work
