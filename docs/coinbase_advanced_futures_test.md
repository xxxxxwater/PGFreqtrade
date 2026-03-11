# Coinbase Advanced Futures Test Integration

This document describes the **test-tree** Coinbase Advanced integration work under:

- `/data/test_coinbaseadvanced/PGFreqtrade`

It is intentionally separate from the original live spot tree.

## Goals

1. Keep current Coinbase Advanced **spot** support intact.
2. Add a Coinbase exchange subclass capable of running through Freqtrade's
   **futures mode plumbing**.
3. Align the adapter with Coinbase Developer Platform / Advanced Trade docs and
   CCXT unified exchange conventions where practical.
4. Add example futures config + strategy templates for further iteration.

## What changed

### 1. `freqtrade/exchange/coinbase.py`
Extended from a placeholder to a test exchange adapter with:

- `spot` + `futures` mode declarations
- isolated futures support enabled at Freqtrade layer
- Coinbase-specific CCXT `defaultType` routing
- position normalization
- balance normalization
- futures order parameter normalization
- leverage preparation hooks
- max leverage fallback logic
- dry-run liquidation estimate for isolated linear contracts
- funding fee fallback handling

### 2. `user_data/config.coinbase_advanced_futures.example.json`
Added a futures-mode example config using CCXT-style linear symbols such as:

- `BTC/USDC:USDC`
- `ETH/USDC:USDC`
- `SOL/USDC:USDC`

### 3. `user_data/strategies/VWAP_V4_CoinbaseAdvancedFutures.py`
Added a futures-oriented strategy layer which reuses the existing VWAP logic
but also introduces:

- more conservative derivatives defaults
- optional short entries / short exits
- explicit futures BTC reference pair handling
- reduced capital amplification vs. the spot DCA profile

## Important limitations

This is a **test integration**, not a production-certified derivatives adapter.
The biggest open items are:

1. Verifying exact CCXT symbol / market metadata behavior on the deployed
   Coinbase Advanced derivatives account.
2. Validating live unified methods availability:
   - `fetch_positions`
   - leverage settings
   - funding fee history
   - mark/index price sources
3. Confirming exact liquidation math against real Coinbase Advanced docs and
   account responses.
4. Adding short-entry logic if the strategy is intended to trade both sides.

## Suggested next validation steps

1. Install project dependencies inside the dev container.
2. Run:
   - `freqtrade list-markets --exchange coinbase --trading-mode futures`
   - `freqtrade show-config -c user_data/config.coinbase_advanced_futures.example.json`
3. Validate returned symbols and market metadata.
4. Add/adjust live derivatives config with real credentials in a separate local
   secret file (do **not** commit secrets).
5. Iterate on the strategy for:
   - contract symbols
   - leverage
   - shorting
   - funding-fee-aware exits

## Safety note

Do not point the original live spot deployment at this test-tree futures config
until market metadata, order placement, balances, and position reporting are
validated on a paper / low-risk account.
