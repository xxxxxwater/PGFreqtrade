# Binance Portfolio Margin

This adapter supports Binance Portfolio Margin for USDT/USDC linear perpetual trading.
BTC and ETH can be used as Portfolio Margin collateral, but this implementation does not
trade coin-margined inverse contracts.

## Scope

Supported:

- PM account balance via `GET /papi/v1/balance`
- PM account risk summary via `GET /papi/v1/account`
- PM UM positions via `GET /papi/v1/um/positionRisk`
- PM UM order create, fetch and cancel via `/papi/v1/um/order`
- PM UM leverage setting via `/papi/v1/um/leverage`
- PM listenKey lifecycle (create / keepalive / delete) for user data streams
- PM user data stream via `wss://fstream.binance.com/pm/ws/<listenKey>`
- Event-driven order/account reconciliation for `ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`,
  `ACCOUNT_CONFIG_UPDATE`, `POSITION_HISTORY_UPDATE`, `riskLevelChange`,
  balance/liability events, and `listenKeyExpired`
- Telegram `/pm_status` — full account status, balances, positions
- Telegram `/pm_close all|USDT|USDC` — one-click close by contract settlement
- Telegram `/pm_risk` — quick risk check with alerts and new-order block status
- Telegram `/pm_recover` — reconcile order states between DB and exchange
- Scheduled PM risk monitoring with configurable thresholds and RPC alerts
- Scheduled PM order state reconciliation (DB vs. exchange)
- Worker heartbeat enriched with PM uniMMR, equity, and account status

Not supported:

- Coin-margined inverse contracts such as `BTC/USD:BTC`
- Treating USDT/USDC contracts as BTC/ETH-settled contracts
- Automatic collateral transfer or borrowing workflows

## API Key Permissions

PM mode is designed to work with Binance API keys that are enabled for Portfolio Margin
PAPI endpoints. Spot/SAPI permissions are not required for the USDT/USDC perpetual
workflow documented here.

During market reload, the adapter skips ccxt's Binance `fetch_currencies()` bootstrap in
PM mode. This prevents PM-only keys from failing on `GET /sapi/v1/capital/config/getall`
with `-2015 Invalid API-key, IP, or permissions`.

If `GET /papi/v1/account` itself returns `-2015`, the failure is no longer caused by
ccxt Spot/SAPI currency bootstrap. Check the exact API key loaded by the container,
the Binance API IP whitelist for the request IP shown in the error body, and whether
the account is standard Portfolio Margin or Portfolio Margin Pro. This adapter uses
standard PM PAPI endpoints for USDT/USDC UM perpetual trading.

For Docker Compose deployments, set credentials through environment variables or a
`.env` file:

```bash
BINANCE_PM_API_KEY="..."
BINANCE_PM_API_SECRET="..."
docker compose -f docker-compose-pm.yml up -d
```

The compose file intentionally fails fast when these variables are missing so empty
environment variables cannot silently override the config file credentials.

## Configuration

```json
{
  "trading_mode": "futures",
  "margin_mode": "cross",
  "stake_currency": "USDT",
  "exchange": {
    "name": "binance",
    "portfolio_margin": true,
    "portfolio_margin_risk": {
      "min_uni_mmr": 1.5,
      "warning_uni_mmr": 2.0,
      "emergency_stop_uni_mmr": 1.2,
      "user_stream_enabled": true,
      "user_stream_health_interval_minutes": 1,
      "user_stream_queue_warning_size": 500,
      "user_stream_disconnected_restart_seconds": 120,
      "user_stream_max_restarts_per_hour": 3,
      "user_stream_recover_unmatched_orders": true,
      "monitor_interval_minutes": 5,
      "order_recovery_interval_minutes": 5,
      "heartbeat_risk_cache_seconds": 300,
      "max_leverage": 10,
      "max_total_notional": 100000,
      "max_position_notional": {
        "BTC/USDT:USDT": 50000,
        "ETH/USDT:USDT": 30000
      },
      "max_daily_loss": 500
    }
  }
}
```

| Field | Description | Default |
|-------|-------------|---------|
| `portfolio_margin` | Enable Binance PM mode. | `false` |
| `min_uni_mmr` | Block new non-reduce-only orders when `uniMMR` falls below this. | _optional_ |
| `warning_uni_mmr` | Send RPC WARNING when `uniMMR` is below this. | _optional_ |
| `emergency_stop_uni_mmr` | Force-close ALL positions and stop trading when `uniMMR` drops below this. | _optional_ |
| `user_stream_enabled` | Enable PM WebSocket user data stream for order/account events. | `true` |
| `user_stream_health_interval_minutes` | How often to inspect PM stream liveness, queue size and error counters. | `1` |
| `user_stream_queue_warning_size` | Send RPC WARNING when queued stream events reach this count. | `500` |
| `user_stream_disconnected_restart_seconds` | Restart stream when it remains disconnected longer than this. | `120` |
| `user_stream_max_restarts_per_hour` | Cap automatic stream restarts per rolling hour. | `3` |
| `user_stream_recover_unmatched_orders` | Run order recovery when stream order event does not match a local open order. | `true` |
| `monitor_interval_minutes` | How often to check PM risk metrics. | `5` |
| `order_recovery_interval_minutes` | How often to reconcile order states DB↔exchange. | `5` |
| `heartbeat_risk_cache_seconds` | Minimum cache time for PM account risk data used in worker heartbeat logs. | `300` |
| `max_leverage` | Maximum allowed leverage for any position. | _optional_ |
| `max_total_notional` | Maximum total notional value across all positions (USD). | _optional_ |
| `max_position_notional` | Per-pair maximum notional value (dict: pair→cap). | _optional_ |
| `max_daily_loss` | Maximum loss per UTC day before emergency stop. By default **realized PnL only** (trades closed today); see `max_daily_loss_include_unrealized`. | _optional_ |
| `max_daily_loss_include_unrealized` | Also include unrealized PnL of open trades when evaluating `max_daily_loss`. | `false` |
| `risk_api_failure_action` | Action when the PM risk API fails during the scheduled risk monitor: `warn` (block new orders + alert), `pause` (enter PAUSED), `stop` (enter STOPPED). New orders are always blocked (fail-closed). | `warn` |
| `user_stream_fail_closed` | When `true`, block new orders whenever the PM user stream is unavailable (listenKey creation failed, stream not running, long disconnect, dropped/backlogged events). | `true` |
| `allow_degraded_rest_recovery` | When `user_stream_fail_closed=false`, allow opening new orders using REST-only recovery. **Default `false` - do not enable for real money.** | `false` |
| `wallet_mode` | Wallet funding model. `USDT_ONLY` (default): only actual USDT/USDC free balance is usable as opening capital; BTC/ETH collateral is never converted into stake. `PM_COLLATERAL_HAIRCUT`: derive strategy-usable stake from the PAPI risk account `available_balance` × `collateral_haircut`. | `USDT_ONLY` |
| `collateral_haircut` | Conservative discount `(0, 1]` applied to PAPI `available_balance` when `wallet_mode=PM_COLLATERAL_HAIRCUT`. | `1.0` |
| `startup_consistency_mode` | Startup mismatch handling between exchange PM positions/open orders and the local database. `pause` (default, safe): alert + enter PAUSED; `report`: alert only; `cancel`: cancel unmatched exchange open orders, and enter PAUSED if unmatched positions remain. | `pause` |
| `emergency_close_retries` | Maximum attempts to force-close each open position during a PM emergency close. | `3` |

## Fail-Closed Behavior

PM risk control is **fail-closed by default**:

- New non-reduce-only orders require `accountStatus == NORMAL` and a valid `uniMMR`
  when `min_uni_mmr` is configured. Missing/invalid data refuses the order.
- If an entry price cannot be fetched (or is 0) while notional caps are configured,
  the order is refused.
- If the PM risk API fails during the scheduled monitor, new orders are blocked and
  the configured `risk_api_failure_action` runs (`warn`/`pause`/`stop`).
- If the user data stream is unavailable and `user_stream_fail_closed=true`, new
  orders are blocked until the stream recovers AND a clean reconciliation
  completes (see User Stream Health State Machine).
- `max_daily_loss` counts realized PnL by default; set `max_daily_loss_include_unrealized`
  to include unrealized losses.

## Durable Order Intents (idempotency)

Every PM order (entry / DCA / exit / stoploss) is protected by a **durable order
intent** stored in the MAIN freqtrade database (new table `pm_order_intents`,
created automatically by `ModelBase.metadata.create_all`). The JSON-file based
intent store of earlier revisions is removed - there is no ad-hoc JSON file and
no silent fallback:

| Column | Meaning |
|--------|---------|
| `client_id` | Unique `newClientOrderId` (`ft...`) or `newClientStrategyId` (`st...`) |
| `kind` | `order` (normal) or `conditional` (stoploss) |
| `pair` / `side` / `order_type` / `amount` / `price` / `stop_price` | Order parameters |
| `reduce_only` | Whether the order is reduce-only |
| `created_at` / `state` / `last_error` | Timestamp, `PENDING`/`UNKNOWN`, error detail |

Rules:

1. The intent is durably committed **before** the exchange POST. Failing to
   write/read/commit the store => NO POST, bot `PAUSED` / orders blocked,
   RPC/Telegram alert (`intent_store_unavailable`).
2. On a transient POST failure the order is resolved with the **same** client id
   (`origClientOrderId` for normal orders; `newClientStrategyId` conditional
   openOrder/orderHistory for stoplosses). If the order exists it is returned
   and written into the local trade - never resubmitted.
3. If the same-id lookup also fails, the intent stays `UNKNOWN`. From that exact
   moment the unified entry gate (strategy entry, DCA, force-entry, recovery
   re-entry, stoploss creation) reads the durable store and blocks - no second
   client id is ever generated. reduceOnly exits / emergency closes are NOT
   blocked.
4. `/pm_recover` resolves all unresolved intents first; the fail-closed block is
   cleared only when every intent is definitively resolved and reconciliation
   completed without errors.

## Wallet Model (collateral)

The adapter supports two mutually exclusive wallet funding models, selected via
`portfolio_margin_risk.wallet_mode`:

### `USDT_ONLY` (default, recommended)

Only the actual `USDT`/`USDC` free balance (the stake currency) counts as available
opening capital. BTC/ETH held as Binance PM collateral are **not** converted into
available stake and are **never** double-counted into balances, position margin or
available funds. `stake_currency` must therefore be `USDT` or `USDC`, enforced at
startup. This is the safest model: the bot only ever opens what the USDT/USDC free
balance can cover.

### `PM_COLLATERAL_HAIRCUT`

The strategy-usable stake is derived from the PAPI risk account
`available_balance` (collateral already converted by Binance to account base
currency) multiplied by a conservative `collateral_haircut` discount:

```
available_stake = available_balance * collateral_haircut
```

`available_balance` already excludes used initial margin, so collateral, margin and
available funds are never double-counted. Use a haircut below `1.0` (for example
`0.8`) as a safety buffer. The bot still only ever opens what the discounted
available balance can cover, and it does **not** automatically transfer, borrow or
repay collateral.

## User Stream Health State Machine

The PM user data stream health is exposed as an explicit state (see `/pm_status` and
the API `/health` endpoint):

| State | Meaning |
|-------|---------|
| `SYNCING` | Initial state before the first health evaluation. |
| `HEALTHY` | Stream running and connected, no dropped/backlogged events. New orders allowed. |
| `DEGRADED` | Stream not running / disconnected too long / queue backlog / dropped events. New orders blocked (fail-closed). |
| `FAILED` | Restart limit reached and the stream cannot self-heal. New orders blocked. |

On reconnect or after dropped events, the bot first reconciles open orders against
PAPI **successfully** (no errors, all local open orders resolved, no unresolved
intents) before allowing new orders again. A connected WebSocket alone never
unblocks: a failed reconcile keeps/adds `reconciliation_incomplete` and new orders
stay blocked. `listenKeyExpired` rebuilds the listenKey, reconnects and reconciles
before trading resumes.

`/health` pm block exposes `last_reconcile_result`, `last_success_reconcile_time`,
`unresolved_intent_count`, `intent_store_ok` and `orders_blocked_reasons`.

## Startup Consistency Check

On startup in live PM mode the bot compares exchange (PAPI) positions, normal open
orders AND conditional open orders against the local database:

- Unmatched exchange positions/open orders/conditionals are detected and reported.
- `startup_consistency_mode=pause` (default): alert + `PAUSED` + new orders blocked.
- `startup_consistency_mode=report`: alert only (explicit operator opt-out).
- `startup_consistency_mode=cancel`: cancel unmatched normal orders via
  `/um/order` DELETE and unmatched conditionals via `/um/conditional/order`
  DELETE, then **re-query the exchange**. Only when the second check is clean
  (no unknown positions/orders/conditionals) and every cancel succeeded does the
  bot continue; otherwise `PAUSED` + `startup_consistency_mismatch`. Issuing a
  cancel request is never treated as proof of safety.

Before the user stream is released at startup, the bot also rebuilds the
real-order-id -> strategy-id map from the local stoploss orders (conditional
history resolution), so a child order event arriving right after restart is never
classified as foreign.

The bot never opens new positions while the local database is inconsistent with the
exchange. Manual reconciliation is required before resuming.

## Risk Alert Levels

| Condition | Alert | Effect |
|-----------|-------|--------|
| `accountStatus != NORMAL` | RPC WARNING | New orders blocked |
| `uniMMR < warning_uni_mmr` | RPC WARNING | Warning only |
| `uniMMR < min_uni_mmr` | RPC WARNING (CRITICAL) | New orders blocked |
| `uniMMR < emergency_stop_uni_mmr` | RPC WARNING + Emergency Close | ALL positions force-closed, bot STOPPED |
| `leverage > max_leverage` | OperationalException | Order refused |
| `total notional > max_total_notional` | OperationalException | Order refused |
| `pair notional > per-pair max` | OperationalException | Order refused |
| `daily PnL < -max_daily_loss` | RPC WARNING + Emergency Close | ALL positions force-closed, bot STOPPED |
| `riskLevelChange` stream event | RPC WARNING + immediate REST risk check | New orders may be blocked or positions force-closed by configured thresholds |
| Stream queue reaches `user_stream_queue_warning_size` | RPC WARNING | Operator should check bot loop latency |
| Stream events dropped | RPC WARNING + order recovery | Local DB reconciled against REST |
| Stream disconnected too long | RPC WARNING + stream restart + order recovery | Fast path rebuilt, REST fallback stays active |
| Order state mismatch detected | RPC WARNING | Auto-reconciled |

### Alternative Config Flags

The adapter accepts any of these equivalent forms:

```json
{ "exchange": { "portfolio_margin": true } }
{ "exchange": { "binance_portfolio_margin": true } }
{ "exchange": { "account_type": "pm" } }
{ "exchange": { "account_type": "portfolio_margin" } }
```

## Production-Grade Probe Script

A comprehensive read-only probe is available at `scripts/binance_pm_probe.py`.
It tests all PAPI endpoints including listenKey lifecycle.

```bash
export BINANCE_PM_API_KEY="..."
export BINANCE_PM_API_SECRET="..."
python scripts/binance_pm_probe.py --all --pretty
```

Individual endpoint probes:

```bash
python scripts/binance_pm_probe.py --account --pretty
python scripts/binance_pm_probe.py --balance --pretty
python scripts/binance_pm_probe.py --positions --symbol BTCUSDT --pretty
python scripts/binance_pm_probe.py --listen-key-test --pretty
python scripts/binance_pm_framework_probe.py --user-stream-test --stream-wait-seconds 5 --pretty
```

The original lightweight probe (`binance_pm_readonly_probe.py`) is also available
for quick verification. Both scripts call only signed read endpoints and are safe
for read-only restricted API keys.

### Verification Checklist

Before production deployment, run this sequence:

```bash
# 1. Verify API connectivity and account state
python scripts/binance_pm_probe.py --all --pretty

# 2. Verify listenKey lifecycle works (requires API key with PM listenKey permission)
python scripts/binance_pm_probe.py --listen-key-test --pretty
python scripts/binance_pm_framework_probe.py --user-stream-test --pretty

# 3. Start freqtrade in dry-run mode first, verify /pm_status in Telegram
# 4. Switch to live mode with small position size
# 5. Monitor /pm_risk alerts during live trading
```

## Telegram Commands

### `/pm_status`
Prints PM account status, risk fields, user stream state, balances, and open UM
positions (up to 20).

### `/pm_close [all|USDT|USDC] CONFIRM`
Creates market exit orders for open futures trades. Requires the explicit
`CONFIRM` token to execute (a single command must never liquidate the whole book
accidentally):
- `all CONFIRM` — close all trades regardless of settlement currency
- `USDT CONFIRM` — close only USDT-settled contracts
- `USDC CONFIRM` — close only USDC-settled contracts

BTC/ETH are supported as PM collateral assets, not as contract settlement filters in this
adapter. Coin-margined inverse contracts are intentionally out of scope.

### `/pm_risk`
Quick risk status check showing:
- uniMMR, equity, margin breakdown
- Configured thresholds (min_uni_mmr, warning_uni_mmr)
- Active alerts and whether new orders are currently blocked

### `/pm_recover`
Reconciles PM order states between local database and Binance PAPI.
Fixes mismatches automatically. Reports:
- Number of open trades checked
- Orders reconciled
- State mismatches found
- Errors encountered

## Scheduled PM Tasks

When PM mode is enabled and the bot is running in live mode, the following
background tasks are automatically registered:

| Task | Trigger | Description |
|------|---------|-------------|
| User data stream | Continuous WebSocket | Receives PM account and order events, then triggers targeted REST reconciliation in the bot thread |
| User stream health | configurable scheduler (default 1 min) | Detects dead stream thread, long disconnects, queue buildup, dropped events and parser errors |
| ListenKey keepalive | 30 min scheduler | Extends PM user data stream listenKey (expires after 60 min) |
| Risk monitor | configurable scheduler + account events | Checks uniMMR, account status, daily P&L; enforces all risk limits |
| Order recovery | configurable scheduler | Reconciles open order states between DB and Binance PAPI |

The WebSocket stream is the fast path. Scheduled REST recovery remains enabled as
the safety net for disconnects, missed events, process restarts, or delayed Binance
order state propagation.

## Deployment

The repository ships a runnable dry-run example:

- Strategy: `user_data/strategies/BtcUsdtPmStrategy.py` (example EMA cross - replace it).
- Config template: `user_data/config_pm_live.example.json` (non-sensitive, `dry_run: true`).
- Env template: `.env.pm.example`.
- Compose: `docker-compose-pm.yml`.

Startup is fail-fast: live mode (`dry_run: false`) refuses to start when the API
key/secret are missing, `exchange.portfolio_margin_risk` is empty, `stake_currency` is
not USDT/USDC, or the account type / pair whitelist is incompatible. A missing strategy
also fails at startup via the strategy resolver.

```bash
# 1. Prepare config and env
cp user_data/config_pm_live.example.json user_data/config_pm_live.json   # then edit
cp .env.pm.example .env                                                   # then fill secrets

# 2. Pre-flight checks
python scripts/binance_pm_probe.py --all --pretty
python scripts/binance_pm_probe.py --listen-key-test --pretty

# 3. Dry-run first
freqtrade trade --config user_data/config_pm_live.json --strategy BtcUsdtPmStrategy

# 4. Only then: flip dry_run to false (small size first), monitor /pm_status /pm_risk /pm_recover
```

## Database Backup / Restore

The default database is SQLite (`user_data/tradesv3.sqlite`). SQLite is single-writer
and not recommended as the only copy for a live bot. Recommended:

- **SQLite (simple):** take regular online backups while the bot runs:
  ```bash
  python scripts/backup_db.py --db-url sqlite:///user_data/tradesv3.sqlite \
      --backup-dir user_data/backups --keep 7
  ```
- **PostgreSQL (recommended for live):** migrate once, then run with `--db-url`:
  ```bash
  freqtrade convert-db --db-url sqlite:///user_data/tradesv3.sqlite \
      --db-url-out postgresql://user:pass@localhost:5432/freqtrade
  # consistent dumps:
  pg_dump 'postgresql://user:pass@localhost:5432/freqtrade' > backup.sql
  ```
- Keep a kill-switch: a separate process/alert that can stop the bot container
  independently of the bot's own risk checks (e.g. `docker stop freqtrade-pm`),
  since RPC notifications are best-effort and must never be the only emergency path.

## Production Notes

Before enabling live trading, verify:

- `/pm_status` works with a read-only key.
- `/pm_status` shows `user_stream.connected=True` after live startup.
- `/pm_risk` shows correct thresholds and alert states.
- `/pm_recover` returns zero mismatches for a healthy state.
- The bot starts with `portfolio_margin=true` and only USDT/USDC linear perpetual pairs.
- Order creation is tested with a small trading-enabled PM key.
- `min_uni_mmr` is set to a conservative threshold (1.5–2.0 recommended).
- `warning_uni_mmr` is set above `min_uni_mmr` for early warning.
- Risk monitor intervals are appropriate for your trading frequency.
- API keys are IP-restricted and rotated after every accidental exposure.
- Operational alerts cover rejected orders, cancelled orders, missing positions and low `uniMMR`.
- PM listenKey lifecycle is verified via `--listen-key-test` before production.

## PAPI Interface Mapping

Private trading operations are routed to Binance Portfolio Margin PAPI endpoints only.
No `/fapi/v1/*` private endpoint is used for the PM automation path.

| Operation | PAPI endpoint | Method |
|-----------|---------------|--------|
| Create entry / exit / adjust / reduceOnly order | `POST /papi/v1/um/order` | `POST` |
| Create stoploss (STOP_MARKET / STOP, reduceOnly) | `POST /papi/v1/um/conditional/order` | `POST` |
| Fetch single order | `GET /papi/v1/um/order` | `GET` |
| Resolve order by clientOrderId (idempotency) | `GET /papi/v1/um/order` (`origClientOrderId`) | `GET` |
| Cancel order | `DELETE /papi/v1/um/order` | `DELETE` |
| Fetch open conditional (stoploss) order | `GET /papi/v1/um/conditional/openOrder` | `GET` |
| Fetch ALL open conditional (stoploss) orders (startup consistency) | `GET /papi/v1/um/conditional/openOrders` | `GET` |
| Fetch conditional (stoploss) order history | `GET /papi/v1/um/conditional/orderHistory` | `GET` |
| Cancel conditional (stoploss) order | `DELETE /papi/v1/um/conditional/order` | `DELETE` |
| Fetch open orders | `GET /papi/v1/um/openOrders` | `GET` |
| Fetch order history | `GET /papi/v1/um/allOrders` | `GET` |
| Fetch executed trades (fills/fees) | `GET /papi/v1/um/userTrades` | `GET` |
| Fetch funding fees | `GET /papi/v1/um/income` (`incomeType=FUNDING_FEE`) | `GET` |
| Fetch positions | `GET /papi/v1/um/positionRisk` | `GET` |
| Fetch account risk / equity / uniMMR | `GET /papi/v1/account` | `GET` |
| Fetch balances | `GET /papi/v1/balance` | `GET` |
| Set leverage | `POST /papi/v1/um/leverage` | `POST` |
| Create / keepalive / delete listenKey | `POST/PUT/DELETE /papi/v1/listenKey` | |

## Deployment Readiness

This branch ships with a **safe, testable adapter** but has **not** completed live
PAPI order lifecycle validation on a real standard Portfolio Margin account. There is
no official Binance PM testnet; ordinary Futures testnet is **not** equivalent to PM.

| Conclusion | Meaning | Required before advancing |
|-----------|---------|---------------------------|
| `BLOCKED` | Do not run live. | Any failing P0/P1 test or missing credential/preflight verification. |
| `READ_ONLY_READY` | Read-only PAPI + user stream validation is safe to run. | All P0/P1 unit/integration tests pass; probe script verified against a read-only key. |
| `CANARY_READY` | Small, supervised live trading on a single BTC/USDT:USDT perpetual is acceptable. | Read-only validation passed; `dry_run=false` preflight passes; small size, low leverage, human supervision, rollback + emergency-close plan ready. |
| `LIVE_READY` | Unattended live trading is acceptable. | Full PAPI order lifecycle (entry/exit/adjust/cancel/stoploss) plus user-stream + order-recovery verified against the real PM account over multiple sessions. |

**Current status of this branch: `READ_ONLY_READY` (pending operator's real-account
read-only validation). Not `CANARY_READY` and not `LIVE_READY`.** Do not run
unattended live trading until the closed-loop acceptance steps below have been
executed on the real account.

## Conditional Stoploss Closed Loop (P0)

The conditional (stoploss) lifecycle is implemented end-to-end; `strategyStatus`
alone is never trusted to close a trade:

1. **Create**: `POST /um/conditional/order` with `strategyType=STOP|STOP_MARKET`,
   `reduceOnly=true` and a client-generated `newClientStrategyId`. The intent is
   durably committed to the main database table `pm_order_intents` (state
   `PENDING`) **before** the POST so a timeout/restart can never submit a
   duplicate stoploss for the same position. If the database cannot be written,
   the POST is refused (fail-closed, see Durable Order Intents above).
2. **State model**: `NEW` -> `open`. `TRIGGERED` -> **still open**: the exchange
   reports the real order id in `orderId` (official
   `QueryUmConditionalOrderHistoryResponse` field, only present after trigger);
   the real order is fetched through `GET /um/order` and its authoritative
   `filled`/`average`/`cost`/`fee`/`trades` are merged. Only a real `FILLED`
   closes the strategy order. `CANCELLED`/`EXPIRED` never fabricate fills.
3. **User stream**: order events are matched by local order id first, then by the
   actual-order map (real order id -> strategy id). An unmatched event with our
   `ft*`/`st*` client id triggers fail-closed blocking (`unmatched_stream_order`)
   + Telegram/API alert + full reconciliation; foreign orders are ignored.
4. **Recovery**: `_pm_reconcile_open_orders` branches per order side - stoploss
   orders go through `fetch_stoploss_order` (conditional lifecycle), normal
   orders through `fetch_order`. `/pm_recover` runs the same path and clears the
   fail-closed block only when it completes without errors.
5. **Startup**: pending order intents are resolved against the exchange first
   (unresolvable => block `pending_intent_unresolved`). The consistency check
   then compares positions, normal open orders AND conditional open orders
   (`GET /um/conditional/openOrders`); unknown conditional orders pause + block
   by default, and `cancel` mode deletes them through the conditional DELETE
   endpoint - never through `/um/order`.

## Minimal Live Acceptance Steps (real PM account, small size)

Before `CANARY_READY` can be considered, run this supervised sequence on the real
standard Portfolio Margin account with minimal BTCUSDT size and low leverage.
Stop after each step if anything deviates; reconcile via `/pm_recover`.

1. Read-only probe: `python scripts/binance_pm_probe.py --all` (account, balance,
   positions, api trading status, account config, symbol config, listenKey).
2. Manual entry: `POST /papi/v1/um/order` LIMIT BUY 0.001 BTCUSDT
   (`newClientOrderId` = `ft` + 30 hex). Verify via `GET /um/order`.
3. Filled entry appears in freqtrade (`/status` shows the trade and the entry order).
4. Place a `STOP_MARKET` stoploss: `POST /um/conditional/order` with
   `newClientStrategyId` = `st` + 30 hex, `stopPrice` slightly below entry.
   Verify `/pm_status` shows the stoploss order open.
5. Let the market hit the stop price (or move the stop price to trigger
   immediately). Verify: the conditional history returns `TRIGGERED` with a real
   `orderId`; freqtrade closes the trade with non-zero `filled`/`average`; the
   position is flat on the exchange.
6. Repeat with a `STOP` (limit) stoploss whose real order rests in the book
   (NOT filled). Verify the strategy stays open in freqtrade and cancel it via
   `/um/conditional/order` DELETE; verify freqtrade reflects `canceled`.
7. User-stream: with a trade open, restart the bot process; verify pending
   intents are resolved, the startup consistency check matches exchange state,
   and open stoploss orders are re-associated after reconnect.
8. Kill the network mid-POST (or simulate with `--pm-force-timeout` probe),
   restart, and verify no duplicate order is placed (same client id resolved).
9. Emergency: `/pm_close all CONFIRM` closes everything; `/pm_recover` shows a
   clean reconcile and clears any fail-closed block.

Until steps 1-9 complete successfully on the real account, this branch stays
`READ_ONLY_READY` (never `CANARY_READY` or `LIVE_READY`).
