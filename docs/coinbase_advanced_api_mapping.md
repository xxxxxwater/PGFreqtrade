# Coinbase Advanced API / CCXT / Freqtrade Mapping (Test Branch)

This document maps three conceptual layers in the test branch:

1. **Coinbase Advanced Trade API concepts**
2. **CCXT unified exchange model**
3. **Freqtrade internal exchange / strategy expectations**

Project root:
- `/data/test_coinbaseadvanced/PGFreqtrade`

---

## 1. Market / Product model

### Coinbase Advanced docs perspective
Relevant concepts typically include:
- product / instrument
- product id
- quote currency
- base currency
- derivatives / perpetual / futures-like product metadata
- margin / leverage metadata

### CCXT unified perspective
Relevant fields usually become:
- `symbol`
- `base`
- `quote`
- `settle`
- `spot`
- `swap`
- `future`
- `contract`
- `inverse`
- `linear`
- `limits`
- `precision`

### Freqtrade perspective
Freqtrade expects exchange adapters to answer:
- is this market tradable?
- is this market a futures market?
- what is the contract size?
- what is the leverage ceiling?
- what symbol format should be used internally?

### Test-branch implementation
Files involved:
- `freqtrade/exchange/coinbase.py`
- `freqtrade/exchange/coinbase_advanced_compat.py`
- `freqtrade/exchange/coinbase_advanced_models.py`

Key behavior:
- Spot uses plain symbols like `BTC/USDC`
- Futures attempts to normalize to CCXT-style linear symbols like `BTC/USDC:USDC`
- Futures markets are filtered by:
  - non-inverse
  - contract/swap/future true
  - settle currency matching configured stake currency

---

## 2. Balance model

### Coinbase Advanced docs perspective
Common balances concepts:
- available
- hold / pending
- total balance
- account / portfolio scoping
- quote currency vs. stablecoin differences

### CCXT unified perspective
Usually normalized to:
- `free`
- `used`
- `total`

### Freqtrade perspective
Freqtrade wallets logic expects per-currency objects with:
- `free`
- `used`
- `total`

### Test-branch implementation
The compatibility layer strips helper keys such as:
- `info`
- aggregate `free/used/total`

and keeps only per-currency balance objects.

---

## 3. Position model

### Coinbase Advanced docs perspective
Common derivatives position concepts:
- side
- number of contracts / size
- leverage
- collateral / initial margin
- liquidation price
- margin mode

### CCXT unified perspective
Usually exposed as:
- `symbol`
- `side`
- `contracts`
- `leverage`
- `collateral`
- `initialMargin`
- `liquidationPrice`

### Freqtrade perspective
Freqtrade futures flow needs consistent position data to:
- build wallets view
- evaluate open positions
- calculate liquidation and funding logic
- manage exits

### Test-branch implementation
`CoinbaseAdvancedPositionView` / `normalize_coinbase_position()` attempt to normalize:
- contracts from `contracts`, `contractSize`, `amount`, or `info.number_of_contracts`
- leverage from `leverage` or `info.leverage`
- margin mode from `marginMode` / `info.margin_mode`
- side to lower-case string

---

## 4. Order parameter model

### Coinbase Advanced docs perspective
Advanced Trade derivatives order concepts may include:
- side
- order type
- post-only / time in force
- reduce-only semantics
- leverage and margin mode context

### CCXT unified perspective
Common order-create arguments:
- `symbol`
- `type`
- `side`
- `amount`
- `price`
- params dict for exchange-specific fields

### Freqtrade perspective
Freqtrade needs exchange adapters to translate:
- entry / exit order types
- TIF
- reduceOnly
- leverage
- margin mode

### Test-branch implementation
`normalize_coinbase_order_params()` currently shapes:
- `postOnly` when TIF = `PO`
- `reduceOnly` in futures mode
- `marginMode` in futures mode
- `leverage` if > 1

This is an evolving mapping layer and should be validated against real account responses.

---

## 5. Funding / mark / liquidation model

### Coinbase Advanced docs perspective
Derivatives docs commonly distinguish:
- mark price
- index price
- funding rate / funding fee
- liquidation rules

### CCXT unified perspective
May expose:
- mark price OHLCV
- funding history / funding rates
- liquidation fields in positions

### Freqtrade perspective
Used for:
- dry-run liquidation math
- funding fee accrual
- futures analytics

### Test-branch implementation
Current status:
- conservative dry-run liquidation estimate exists
- funding fee helper falls back safely when data is unavailable
- exact production-grade parity still requires live validation

---

## 6. Strategy implications

Two strategy tracks exist in the test branch:

### Track A — migration track
- `user_data/strategies/VWAP_V4_CoinbaseAdvancedFutures.py`
- Goal: preserve as much existing behavior as possible while making it futures-compatible

### Track B — directional futures track
- `user_data/strategies/CoinbaseAdvancedDirectionalFutures.py`
- Goal: cleaner long/short futures-native research strategy

---

## 7. Current architectural stance

This test branch does **not** attempt to replace CCXT.
Instead it uses this stack:

Coinbase Advanced docs → CCXT unified output → project-local compat layer → Freqtrade exchange subclass → strategy/config/dev tooling

That keeps all experimental integration work inside the test project tree.
