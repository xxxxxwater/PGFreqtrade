# Test Branch Coinbase Advanced Change Log

Project copy:
- `/data/test_coinbaseadvanced/PGFreqtrade`

Source copy from:
- `/data/projects_coinbaseadvanced/PGFreqtrade`

## Main modifications

### Exchange layer
- Expanded `freqtrade/exchange/coinbase.py`
- Added `freqtrade/exchange/coinbase_advanced_compat.py`
- Added `freqtrade/exchange/coinbase_advanced_models.py`
- Added test futures support scaffolding
- Added CCXT `defaultType` / `defaultSubType` routing
- Added balance / position normalization
- Added futures order param shaping
- Added leverage prep hooks
- Added conservative liquidation and funding-fee helpers
- Split Coinbase Advanced / CCXT compatibility logic into a project-local helper module

### User configs / strategies
- Added `user_data/config.coinbase_advanced_futures.example.json`
- Added `user_data/config.coinbase_advanced_futures.live-template.json`
- Added `user_data/config.coinbase_advanced_futures.directional-template.json`
- Added `user_data/strategies/VWAP_V4_CoinbaseAdvancedFutures.py`
- Added `user_data/strategies/CoinbaseAdvancedDirectionalFutures.py`
- Extended futures strategy with optional short entries/exits
- Added a cleaner directional futures strategy for deeper test-branch experiments

### Dev / runtime
- Reworked `docker-compose.yml` with:
  - spot service
  - futures dev service profile
  - directional futures dev service profile
- Added `docker/docker-compose-coinbase-advanced-futures-test.yml`
  for dedicated futures strategy tracks
- Updated `.devcontainer/devcontainer.json`

### Documentation
- Added `docs/coinbase_advanced_futures_test.md`
- Added `docs/coinbase_advanced_api_mapping.md`
- Added this changelog file
- Added reporting / probe scripts for test-branch validation
- Added branch gap reporting script
- Added environment audit script to distinguish missing tests vs missing environment
- Tightened product schema parsing and close-position compatibility modeling
- Added main order-path fallback from reduceOnly to close_position for Coinbase futures errors
- Tightened wallet-side consumption of normalized Coinbase futures positions
- Added market-sync oriented strategy helpers and sync audit script for futures symbols

### Tests
- Added `tests/exchange/test_coinbase_advanced_testbranch.py`

## Status
- This is a test-tree structural refactor / extension.
- It is intended for further validation, not immediate production futures trading.
