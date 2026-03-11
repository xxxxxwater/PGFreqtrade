# Test Branch Coinbase Advanced Change Log

Project copy:
- `/data/test_coinbaseadvanced/PGFreqtrade`

Source copy from:
- `/data/projects_coinbaseadvanced/PGFreqtrade`

## Main modifications

### Exchange layer
- Expanded `freqtrade/exchange/coinbase.py`
- Added test futures support scaffolding
- Added CCXT `defaultType` / `defaultSubType` routing
- Added balance / position normalization
- Added futures order param shaping
- Added leverage prep hooks
- Added conservative liquidation and funding-fee helpers

### User configs / strategies
- Added `user_data/config.coinbase_advanced_futures.example.json`
- Added `user_data/config.coinbase_advanced_futures.live-template.json`
- Added `user_data/strategies/VWAP_V4_CoinbaseAdvancedFutures.py`
- Extended futures strategy with optional short entries/exits

### Dev / runtime
- Reworked `docker-compose.yml` with:
  - spot service
  - futures dev service profile
- Updated `.devcontainer/devcontainer.json`

### Documentation
- Added `docs/coinbase_advanced_futures_test.md`
- Added this changelog file
- Added reporting / probe scripts for test-branch validation

### Tests
- Added `tests/exchange/test_coinbase_advanced_testbranch.py`

## Status
- This is a test-tree structural refactor / extension.
- It is intended for further validation, not immediate production futures trading.
