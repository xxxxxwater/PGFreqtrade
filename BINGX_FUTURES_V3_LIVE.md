# BingX Futures Support V3

## Positioning

`chris/bingx-futures-support-v3` is the live-trading branch for the BingX futures adaptation.

## Base

- Base branch: `chris/bingx-futures-support-v1`
- Live branch: `chris/bingx-futures-support-v3`
- Core live commit: `c28da7da9`

## Actual Code Changes

### 1. Exchange Runtime Logic

- `freqtrade/exchange/bingx.py`
  - Expanded BingX futures exchange handling.
  - Added and adjusted runtime logic for futures order flow and exchange-specific behavior.
- `freqtrade/exchange/exchange.py`
  - Updated shared exchange-layer integration points used by the BingX futures path.

### 2. Runtime Deployment Entry

- `docker-compose.yml`
  - Adjusted deployment-side configuration for the BingX futures live path.

### 3. Removed Non-Live Templates and Old Strategy Files

The branch currently removes the following files from the tracked tree:

- `user_data/config.bingx_futures.example.json`
- `user_data/config.bingx_futures.live-template.json`
- `user_data/config.coinbase_advanced_futures.directional-template.json`
- `user_data/config.coinbase_advanced_futures.example.json`
- `user_data/config.coinbase_advanced_futures.live-template.json`
- `user_data/config.coinbase_advanced_futures.matrix-template.json`
- `user_data/strategies/CoinbaseAdvancedDirectionalFutures.py`
- `user_data/strategies/VWAP_V4_CoinbaseAdvancedFutures.py`
- `user_data/strategies/coinbase_advanced_strategy_utils.py`

## Diff Summary

Compared with `chris/bingx-futures-support-v1`, `v3` contains:

- 12 changed files
- 505 insertions
- 863 deletions

## Operational Note

This branch reflects the current live-trading code state that was synchronized from:

- Server: `8.216.46.230`
- Path: `/root/ft_bingxfuture_userdata/PGFreqtrade`

Untracked local backup files such as `.bak` artifacts were intentionally excluded from the Git commit.
