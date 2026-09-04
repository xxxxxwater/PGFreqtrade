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

For Docker Compose deployments, use Docker secret files.  This avoids putting the
database URL or exchange secret into a compose environment block:

```bash
# secrets/db_password.txt: one PostgreSQL password
# secrets/pm_env.txt: FREQTRADE__EXCHANGE__KEY=..., FREQTRADE__EXCHANGE__SECRET=...,
#                     FREQTRADE__DB_URL=postgresql://freqtrade:<url-encoded-password>@db:5432/freqtrade
docker compose -f docker-compose-pm.yml up -d
```

The wrapper reads these files only inside the bot container and refuses to start
when the required values are missing.

## Configuration

```json
{
  "trading_mode": "futures",
  "margin_mode": "cross",
  "stake_currency": "USDT",
  "order_types": {
    "entry": "limit",
    "exit": "limit",
    "emergency_exit": "market",
    "stoploss": "market",
    "stoploss_on_exchange": true,
    "stoploss_on_exchange_interval": 60
  },
  "exchange": {
    "name": "binance",
    "portfolio_margin": true,
    "portfolio_margin_symbol_config_cache_seconds": 300,
    "portfolio_margin_stoploss_capability_cache_seconds": 300,
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
| `portfolio_margin_symbol_config_cache_seconds` | Cache duration for the signed PAPI `um/symbolConfig` account-permission check used by dynamic pairlists. | `300` |
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

## Dynamic PM Perpetual Universe (including TradFi Perps)

`MomentumVolumePairList` does **not** decide eligibility from an asset name or a
hard-coded crypto list.  Each refresh constructs the candidate universe as the
intersection of:

1. public exchange metadata: active, tradable, **linear** `swap` market with
   `quote=USDT` and `settle=USDT`;
2. the signed PM account response from `GET /papi/v1/um/symbolConfig`: the
   symbol must be mapped by loaded metadata, set to `CROSSED`, and have a
   positive `maxNotionalValue`.

The selector's `min_quote_volume` configuration controls the rolling 24-hour
quote-volume floor. The PM live configuration uses 600,000,000 USDT and ranks
the surviving contracts by 24-hour percentage momentum, keeping the top 20.
Consequently, a crypto, equity-index, commodity, or other underlying
is included automatically **only** when Binance exposes it to this account as a
PM-enabled USDⓈ-M linear perpetual.  A public contract or a `/sapi/v1/equity/*`
stock product is never assumed executable by the PM UM order route.

Binance documents `POST /papi/v1/um/stock/contract` as the TradFi Perps
agreement operation.  This deployment deliberately never calls it: it changes
the account's contractual state.  If an operator explicitly completes that
agreement in Binance, reloads the market metadata, and restarts dry-run, any
newly account-enabled linear perpetual is picked up by the next automated
pairlist refresh; no code or whitelist edit is needed.

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
- PM canary/live configurations must set `order_types.stoploss_on_exchange=true` (either in
  the configuration or strategy) so the conditional-stoploss lifecycle is actually enabled.

## Durable Order Intents (idempotency)

Every PM order (entry / DCA / exit / stoploss) is protected by a **durable order
intent** stored in the MAIN freqtrade database (table `pm_order_intents`) plus a
**transactional outbox** (table `pm_outbox`), created automatically by
`ModelBase.metadata.create_all` (plus `migrate_pm_tables` for databases created
by older PM builds). There is no ad-hoc JSON file and no silent fallback:

### Lifecycle: PREPARED -> ACKED -> LINKED -> RECONCILED

| State | Meaning |
|-------|---------|
| `PREPARED` | Intent + outbox row committed in ONE transaction, BEFORE the POST |
| `ACKED` | Exchange accepted the order; `exchange_order_id` + `raw_response` (full ACK evidence) persisted on the intent AND the outbox row |
| `LINKED` | The local `Trade`/`Order` rows were committed; the LINK transition happens IN THE SAME DATABASE TRANSACTION as that commit (`pm_link_intent_in_session`) |
| `RECONCILED` | Reconciliation confirmed the exchange agrees with the local order -> the intent is tombstoned and the outbox row is kept as permanent audit (state `RECONCILED`) |
| `UNKNOWN` | The POST outcome could not be established - blocks all exposure-increasing orders until resolved |

Key guarantees:

1. The intent is durably committed **before** the exchange POST. Failing to
   write/read/commit the store => NO POST, bot `PAUSED` / orders blocked,
   RPC/Telegram alert (`intent_store_unavailable`).
2. **Nothing is tombstoned on the ACK.** A crash between the exchange ACK and the
   local Trade/Order commit leaves the intent `ACKED` with the exchange order id
   and raw response preserved. Startup recovery reports this as an orphan
   ("order EXISTS on the exchange but has no local record") and keeps new
   exposure blocked until `/pm_recover` links or the operator resolves it.
3. On a transient POST failure the order is resolved with the **same** client id
   (`origClientOrderId` for normal orders; `clientAlgoId` on the UM Algo
   order/history endpoints for stoplosses). If the order exists it is returned
   and written into the local trade - never resubmitted. If both the submit and
   the lookup fail, the intent becomes `UNKNOWN`.
4. The outbox row (full request payload + client id) is the relay record: a
   crash between enqueue and dispatch leaves a `PENDING` row that startup
   recovery (`pm_drain_outbox`) re-dispatches exactly once (resolve-by-client-id
   first). Deterministic exchange rejections tombstone the intent and keep a
   `REJECTED` outbox row with the error.
5. The whole pipeline (enqueue, dispatch, recovery, reconciliation) runs under a
   PostgreSQL **advisory lock** (`pm_pipeline_lock`, key `PMPIPELI`): two bot
   processes can never dispatch or recover concurrently; the lock is
   session-scoped so a dead process can never leave it held.
6. Gates are scoped correctly:
   * exposure-increasing entries are blocked by ANY unresolved
     exposure-increasing intent;
   * a protective (reduce-only) stoploss is blocked only by unresolved intents
     FOR THE SAME PAIR - an unrelated pair's pending entry never blocks a stop.
7. `/pm_recover` resolves all unresolved intents first; the fail-closed block is
   cleared only when every intent is definitively resolved and reconciliation
   completed without errors.

## Startup Consistency Check (bidirectional)

The startup check compares BOTH directions, never just "exchange has, local
lacks":

* exchange positions / open orders / conditional orders without a local record
  (`unknown_positions` / `unknown_orders` / `unknown_conditional_orders`);
* local open trades that are flat on the exchange
  (`local_open_trades_flat_on_exchange`);
* local open orders missing on the exchange
  (`local_open_orders_missing_on_exchange`);
* recent (last 2h, newest 50) terminal orders whose exchange status disagrees
  with the local status (`recent_closed_order_mismatches`).

`startup_consistency_mode` applies to all categories: `report` only alerts,
`pause` pauses + blocks, `cancel` additionally cancels unmatched exchange orders
and re-verifies.

## Signal Decision Ledger

For every pair and closed candle, the bot records ONE durable row
(`pm_signal_ledger`): strategy version, factor hash (the strategy's curated
factor columns, see `VWAP_V4.pm_signal_snapshot`), data freshness (closed
contiguous candles, 1h alignment, non-zero volume), signal tag, the decision
and the exchange client id of any submitted order. Rows are write-once per
(pair, timeframe, candle, scope) - factors are evaluated AT MOST once per
closed candle, which makes signal -> decision -> order replayable and
auditable.

All FOUR decision classes are recorded:

* `no_signal` - the strategy produced no entry signal for this closed candle
  (recorded by `create_trade`, so "no decision" can never masquerade as
  "not evaluated");
* `blocked` - an execution gate refused the entry (PM block reasons,
  pair lock);
* `blocked_data` - the data was unhealthy / non-contiguous / gap-latched;
* `entry_submitted` - the order reached the exchange (client id recorded);
* exit decisions (`exit`, `exit_data_failure`) use a separate `exit` scope so
  they never overwrite the entry audit row of the same candle.

### Durable candle watermark (exactly-once cursor)

`pm_candle_watermarks` is the persistent progress cursor of the ledger, written
in the SAME transaction as each entry-scope decision row:

* the cursor advances only together with a new ledger row - a restart can
  neither re-evaluate a decided candle nor skip an audit row;
* when the closed-candle series jumps and the candle immediately before the
  evaluated one is MISSING from the dataframe, the gap is latched durably
  (`gap_expected_open_time` / `gap_reason`) and the decision is recorded as
  `blocked_data` - the pair stays fail-closed across restarts until the hole
  is backfilled (proven by the missing candle reappearing in the dataframe);
* a skip caused by downtime whose data WAS backfilled is not a gap and never
  blocks.

Data failures are handled by ONE unified policy instead of per-branch strategy
logic:

* entries on unhealthy/gapped data are ALWAYS blocked (fail-closed) and recorded
  as `blocked_data`;
* open positions are held by default; set
  `portfolio_margin_risk.data_failure_exit: true` to force-close them instead.

## PAPI Governor (rate limits)

All PAPI requests flow through one governor
(`freqtrade/exchange/pm_governor.py`):

* endpoint weight accounting (response header `X-MBX-USED-WEIGHT-1M` is
  authoritative, a local weight table is the fallback);
* exponential backoff with jitter after 429 (honours `Retry-After` from the raw
  fallback);
* circuit breaker on 418 (IP ban) with growing probe backoff;
* `critical` priority for reduce-only exits / stoplosses / emergency closes -
  they are never deferred by local backoff;
* TRUE single-flight snapshot fetches (`snapshot_get_or_fetch`) for the
  account / balance / position snapshots: concurrent callers share ONE PAPI
  request instead of stampeding on TTL expiry, leader failures propagate to
  every follower (no invented data), and a hung leader times out fail-closed
  (`portfolio_margin_risk.snapshot_cache_ttl_s`, default 3s).

The entry order path (idempotency gate -> risk check + open-order/intent
reservation computation -> durable intent/outbox commit -> POST) runs under ONE
PM pipeline lock boundary (PostgreSQL session-scoped advisory lock `PMPIPELI`,
process-wide reentrant lock elsewhere), so two threads or processes can never
pass the risk check on the same uncommitted exposure.

`totalCollateralValue` is the collateral metric; when absent it falls back to
the sum of wallet balances (a proxy) - NEVER to maintenance margin, which is not
a collateral value.

## Account-wide Emergency Close (FILLED-confirmed)

Emergency close enumerates EVERY open position on the PM account - including
manual / external positions without a local trade - closes them (local trades
via the normal exit path, foreign positions via reduce-only market orders) and
then **verifies each position is flat with a fresh per-symbol fetch** before
counting it closed. Creating an exit order is never reported as "closed".

The production zero-exposure precheck (`scripts/pm_zero_exposure_check.py`)
returns exit code 0 ONLY when all three channels (positions / open orders /
algo orders) are known and zero; any unverifiable channel yields exit code 2
(unknown) - stop/upgrade flows must never treat unknown as safe.

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

The check is BIDIRECTIONAL (see "Startup Consistency Check (bidirectional)"
above): unmatched exchange positions/orders/conditionals AND local records the
exchange does not confirm (open trade flat on the exchange, open order missing
on the exchange, recent terminal orders with disagreeing status) are all
reported.

- `startup_consistency_mode=pause` (default): alert + `PAUSED` + new orders blocked.
- `startup_consistency_mode=report`: alert only (explicit operator opt-out).
- `startup_consistency_mode=cancel`: cancel unmatched normal orders via
  `/um/order` DELETE and unmatched conditionals via `/um/algo/order`
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

Webhook delivery is asynchronous. It reserves a separate queue for `WARNING` and `EXCEPTION`
messages, and uses a last-resort synchronous attempt only if that safety queue is full. Keep an
independent operator channel (for example Telegram) enabled for live PM trading; a failing remote
webhook endpoint cannot be treated as guaranteed alert delivery.

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

## Market data: kline WebSocket vs REST

The code enables the kline WebSocket as the PRIMARY market-data channel for
Binance futures (`ws_enabled: true`, ccxt pro `watch_ohlcv`); REST remains for
startup backfill, gap repair and validation, and every refresh falls back to
REST automatically when the watch buffer is not fresh.

**Production deployment note (2026-09-04):** this was not a network-path
failure. Binance split the USD-M Futures stream host into `/public`, `/market`
and `/private` routes. Kline streams are regular **market** data. The old root
`wss://fstream.binance.com/ws` still completes its TLS/WS handshake and ACKs a
`SUBSCRIBE`, but it no longer pushes `@kline_*` frames. ccxt-pro 4.5.35 still
ships that legacy root URL.

The PM adapter therefore rewrites only that legacy CCXT-Pro futures URL to
`wss://fstream.binance.com/market/ws` before `watch_ohlcv` begins. It preserves
newer CCXT, proxy and testnet endpoints. Keep `exchange.enable_ws: true`: Kline
WS is again the primary source; REST remains only for startup backfill, gap
repair and validation. A local wire probe now receives a `@kline_5m` frame from
the `/market` endpoint, while the former root URL receives no frame.

Re-run the diagnostic on the host:

```bash
python - <<'EOF'
import asyncio, websockets
async def main():
    async with websockets.connect("wss://fstream.binance.com/market/ws/0", open_timeout=8) as ws:
        await ws.send('{"method":"SUBSCRIBE","params":["btcusdt@kline_5m"],"id":1}')
        await ws.recv()  # subscription ACK
        msg = await asyncio.wait_for(ws.recv(), timeout=10)  # kline event
        print("OK:", str(msg)[:120])
asyncio.run(main())
EOF
```

## Deployment

The repository ships a runnable dry-run example:

- Strategy: `user_data/strategies/VWAP_V4.py` (PM-compatible long-only VWAP/DCA strategy).
- Config template: `user_data/config_pm_live.example.json` (non-sensitive, `dry_run: true`).
- Compose: `docker-compose-pm.yml`.

Startup is fail-fast: live mode (`dry_run: false`) refuses to start when the API
key/secret are missing, `exchange.portfolio_margin_risk` is empty, `stake_currency` is
not USDT/USDC, or the account type / pair whitelist is incompatible. A missing strategy
also fails at startup via the strategy resolver.

```bash
# 1. Prepare config and Docker secret files (kept out of Git)
cp user_data/config_pm_live.example.json user_data/config_pm_live.json   # then edit
# create secrets/db_password.txt and secrets/pm_env.txt as described above

# 2. Pre-flight checks
python scripts/binance_pm_probe.py --all --pretty
python scripts/binance_pm_probe.py --listen-key-test --pretty

# 3. Dry-run first
freqtrade trade --config user_data/config_pm_live.json --strategy VWAP_V4

# 4. Only then: flip dry_run to false (small size first), monitor /pm_status /pm_risk /pm_recover
```

## Database Backup / Restore

The PM compose stacks use PostgreSQL 17. SQLite is single-writer and is not an
acceptable database for this PM deployment.

- **PostgreSQL:** migrate an existing SQLite history once if needed, then run with `--db-url`:
  ```bash
  freqtrade convert-db --db-url sqlite:///user_data/tradesv3.sqlite \
      --db-url-out postgresql://user:pass@localhost:5432/freqtrade
  # consistent dumps:
  pg_dump 'postgresql://user:pass@localhost:5432/freqtrade' > backup.sql
  ```
  `pg_dump` includes `pm_order_intents` (same database). The unique
  `client_id` constraint is enforced by PostgreSQL, so a duplicate intent can
  never be inserted even under a concurrent retry.
- **Fail-closed behavior:** if the database becomes unreadable/unwritable at any
  time, exposure-increasing orders are refused, the bot pauses/blocks with an
  `intent_store_unavailable` alert, and live startup fails when no `db_url` is
  configured (live PM never runs without database persistence).
- Keep a kill-switch: a separate process/alert that can stop the bot container
  independently of the bot's own risk checks (e.g. `docker stop freqtrade-pm`),
  since RPC notifications are best-effort and must never be the only emergency path.

## Production Deployment (PostgreSQL stack)

`docker-compose-pm-prod.yml` provides the production stack:

| Concern | Implementation |
|---------|----------------|
| Database | PostgreSQL 17 with healthcheck + persistent volume. Live PM refuses to start without a database (`db_url` check) and fails closed when the intent store is unreadable. |
| Auto restart | `restart: unless-stopped` on db, bot and backup services. |
| Log rotation | Docker `json-file` driver with `max-size`/`max-file` caps (bot: 10m × 10; db/backup: capped). |
| Database backup | Hourly `pg_dump` sidecar with rotation (10 kept) into `user_data/backups`. Restore: `psql -f user_data/backups/pg_<stamp>.sql`. Backups include `pm_order_intents`. |
| NTP | Run chrony/systemd-timesynced on the host; `/etc/localtime` is mounted read-only into the bot container (PAPI `recvWindow` and intent timestamps depend on clock accuracy). |
| API key IP whitelist | Configure on Binance (API Management) to the fixed public egress IP of this host. The bot refuses to start when the key is rejected (`-2015`) and prints the request IP hint. |
| Secrets | `./secrets/pm_env.txt` and `./secrets/db_password.txt` (git-ignored Docker secret files). `scripts/pm_single_instance.py` (the entrypoint wrapper) loads the PM secret file into the bot child process; exchange key/secret and the full `FREQTRADE__DB_URL` (URL-encoded password) do not appear in the Compose command. |
| Single instance | The entrypoint is `scripts/pm_single_instance.py --lock-file user_data/.pm_instance.lock --env-from-file /run/secrets/pm_env --exec freqtrade trade ...`. The wrapper holds an OS byte-range/flock for its WHOLE lifetime and runs the bot as a child; a second container exits immediately. A unified PM account must NEVER run two bots - the single-instance lock plus the PostgreSQL `PMPIPELI` advisory lock both enforce this. |
| Image pinning | `pull_policy: never` + immutable `sha256:` digest (placeholder `REPLACE_WITH_DIGEST` fails fast until filled in). |

### Production Image Pin Log

Production must only run a reviewed image digest. Record every promotion here:

| Date | Source commit | Image digest (`docker inspect --format='{{index .RepoDigests 0}}' ...`) | Python | CCXT | FastAPI | Notes |
|------|---------------|------------------------------------------------------------------------|--------|------|---------|-------|
| 2026-09-04 | `2c2f7bb94` | local build `pmbinancejp-native:release-2806f8982` image ID `29b1f65bf73e` (no registry; archive sha256 `6aecf382…`) | 3.13.11 | 4.5.35 | 0.128.0 | PM native release: P0 hardening + durable watermark + true single-flight + single-instance lock. Built on-host from the release tree; pre-cutover dump sha256 `b52e24fa…`. Gate: 4342 passed container suite, PG17 fault injection 4/4, migration rehearsal OK. |
| _example_ | `a627799b4` | `sha256:...` | 3.12 | 4.5.35 | 0.128.0 | reviewed; do not copy - fill in the real digest |

Upgrade procedure (never let a new image auto-replace production):

```bash
docker pull pgresearchchris/pgfreqtrade:pm-binance
docker inspect --format='{{index .RepoDigests 0}}' pgresearchchris/pgfreqtrade:pm-binance
# 1. edit docker-compose-pm-prod.yml image: ...@sha256:<new digest>
# 2. record the OLD digest above for rollback
docker compose -f docker-compose-pm-prod.yml up -d freqtrade-pm
# 3. verify: /health, /pm_status, order recovery tests - only then consider it live
# Rollback: edit image back to the previous digest and `up -d` again.
```

Bring-up:

```bash
cp user_data/config_pm_live.example.json user_data/config_pm_live.json
# create secrets/db_password.txt and secrets/pm_env.txt
docker compose -f docker-compose-pm-prod.yml up -d
docker compose -f docker-compose-pm-prod.yml logs -f freqtrade-pm
```

Pre-flight before live (fail-fast checks built into startup):

```bash
# credentials + risk config + db_url + stake_currency + account type are
# validated at startup; the bot refuses to start when any is missing.
freqtrade trade --config user_data/config_pm_live.json --strategy VWAP_V4  # dry-run
curl -s http://127.0.0.1:8080/api/v1/ping && curl -s http://127.0.0.1:8080/api/v1/health
```

## Production Notes

Before enabling live trading, verify:

- `/pm_status` works with a read-only key.
- `/pm_status` shows `user_stream.connected=True` after live startup.
- `/pm_risk` shows correct thresholds and alert states.
- `/pm_recover` returns zero mismatches for a healthy state.
- The dynamic whitelist is non-empty only after `GET /papi/v1/um/symbolConfig` confirms
  account-enabled USDT/USDC linear perpetuals; asset class is not hard-coded.
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
| Create stoploss (STOP_MARKET / STOP, reduceOnly) | `POST /papi/v1/um/algo/order` | `POST` |
| Fetch single order | `GET /papi/v1/um/order` | `GET` |
| Resolve order by clientOrderId (idempotency) | `GET /papi/v1/um/order` (`origClientOrderId`) | `GET` |
| Cancel order | `DELETE /papi/v1/um/order` | `DELETE` |
| Fetch open conditional (stoploss) order | `GET /papi/v1/um/algo/algoOrder` | `GET` |
| Fetch ALL open conditional (stoploss) orders (startup consistency and entry preflight) | `GET /papi/v1/um/algo/openAlgoOrders` | `GET` |
| Fetch conditional (stoploss) order history | `GET /papi/v1/um/algo/allAlgoOrders` | `GET` |
| Cancel conditional (stoploss) order | `DELETE /papi/v1/um/algo/order` | `DELETE` |
| Fetch open orders | `GET /papi/v1/um/openOrders` | `GET` |
| Fetch order history | `GET /papi/v1/um/allOrders` | `GET` |
| Fetch executed trades (fills/fees) | `GET /papi/v1/um/userTrades` | `GET` |
| Fetch funding fees | `GET /papi/v1/um/income` (`incomeType=FUNDING_FEE`) | `GET` |
| Verify dynamic whitelist symbols against this PM account | `GET /papi/v1/um/symbolConfig` | `GET` |
| Fetch positions | `GET /papi/v1/um/positionRisk` | `GET` |
| Fetch account risk / equity / uniMMR | `GET /papi/v1/account` | `GET` |
| Fetch balances | `GET /papi/v1/balance` | `GET` |
| Set leverage | `POST /papi/v1/um/leverage` | `POST` |
| Sign TradFi Perps agreement | `POST /papi/v1/um/stock/contract` | **Never called automatically** |
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

The conditional (stoploss) lifecycle is implemented end-to-end; `algoStatus`
alone is never trusted to close a trade:

1. **Pre-entry gate**: when `stoploss_on_exchange=true`, every non-reduce-only
   entry/DCA verifies `GET /um/algo/openAlgoOrders?algoType=CONDITIONAL`. A 404,
   permission failure, transport failure, or invalid response blocks new exposure.
   Successful checks are cached for the configured short interval.
2. **Create**: `POST /um/algo/order` with `algoType=CONDITIONAL`,
   `type=STOP|STOP_MARKET`, `triggerPrice`, `reduceOnly=true` and a
   client-generated `clientAlgoId`. The intent AND its outbox row are
   durably committed to the main database (`pm_order_intents` +
   `pm_outbox`, state `PREPARED`/`PENDING`) **before** the POST so a
   timeout/restart can never submit a duplicate stoploss for the same position.
   The gate is scoped to the SAME PAIR: an unrelated pair's pending intent never
   blocks a protective stop. If the database cannot be written, the POST is
   refused (fail-closed, see Durable Order Intents above).
3. **State model**: `NEW` -> `open`. `TRIGGERED` -> **still open**: the exchange
   reports the real order id in `actualOrderId` (only present after trigger);
   the real order is fetched through `GET /um/order` and its authoritative
   `filled`/`average`/`cost`/`fee`/`trades` are merged. Only a real `FILLED`
   closes the strategy order. `CANCELLED`/`EXPIRED` never fabricate fills.
4. **User stream**: order events are matched by local order id first, then by the
   actual-order map (real order id -> strategy id). An unmatched event with our
   `ft*`/`st*` client id triggers fail-closed blocking (`unmatched_stream_order`)
   + Telegram/API alert + full reconciliation; foreign orders are ignored.
5. **Recovery**: `_pm_reconcile_open_orders` branches per order side - stoploss
   orders go through `fetch_stoploss_order` (conditional lifecycle), normal
   orders through `fetch_order`. `/pm_recover` runs the same path and clears the
   fail-closed block only when it completes without errors.
6. **Startup**: the outbox relay re-dispatches undispatched rows (idempotent);
   pending order intents are resolved against the exchange first (unresolvable
   => block `pending_intent_unresolved`; an existing order without a local
   record stays ACKED with full evidence and blocks). The consistency check
   then compares positions, normal open orders AND conditional open orders
   (`GET /um/algo/openAlgoOrders`) in BOTH directions; unknown conditional
   orders pause + block by default, and `cancel` mode deletes them through the
   Algo DELETE endpoint - never through `/um/order`.

## Go-Live Scenario Gate

Before any live upgrade is accepted, verify each of these scenarios (unit /
integration tests where marked, supervised live checks otherwise):

| Scenario | How to verify |
|----------|---------------|
| listenKey keepalive/delete | `tests/exchange/test_binance_pm.py` keepalive/delete tests assert the official no-parameter contract; live: watch the 20-minute keepalive succeed |
| Crash before POST | `test_crash_before_post_is_redispatched_exactly_once` (real PostgreSQL, `FREQTRADE_TEST_PG_URL`) |
| Crash after ACK, before local commit | `test_crash_after_ack_before_local_commit_keeps_block` (real PostgreSQL) - evidence preserved, no duplicate POST, entries blocked |
| LINK + local commit atomicity | `test_link_and_local_commit_are_atomic` (real PostgreSQL) - rollback leaves the intent ACKED |
| Concurrent bot processes | `test_advisory_lock_is_exclusive_across_threads` (real PostgreSQL) + `pm_single_instance` lock |
| 429 backoff / 418 circuit breaker | `tests/exchange/test_pm_governor.py` |
| listenKey expiry / user-stream reconnect | `tests/freqtradebot/test_pm_user_stream_health.py` + live restart during an open trade |
| Out-of-order / duplicate events | Live user-stream recording against the PM testnet, then unit fixtures (to be captured on first canary run) |
| Queue overflow (dropped events) | user-stream dropped counter -> DEGRADED + block (`test_pm_user_stream_health.py`) |
| Manual order / external position | `_pm_emergency_close_all` closes foreign positions (reduce-only) and verifies FLAT; live: place a manual order, run `/pm_close all CONFIRM` |
| Algo stoploss trigger | "Conditional Stoploss Closed Loop" steps 4-6 above |
| Unhealthy market data | unified fail-closed: entry blocked + `blocked_data` ledger row; `data_failure_exit` force-closes open positions (strategy synthetic test in `巡检/test_vwap_fixes.py` + `tests/persistence/test_pm_signal_ledger.py`) |

## Minimal Live Acceptance Steps (real PM account, small size)

Before `CANARY_READY` can be considered, run this supervised sequence on the real
standard Portfolio Margin account with minimal BTCUSDT size and low leverage.
Stop after each step if anything deviates; reconcile via `/pm_recover`.

1. Read-only probe: `python scripts/binance_pm_probe.py --algo-stoploss --pretty`
   must report `UM Algo CONDITIONAL`; `--all` additionally checks account, balance,
   positions, API trading status, account config, symbol config and listenKey.
2. Manual entry: `POST /papi/v1/um/order` LIMIT BUY 0.001 BTCUSDT
   (`newClientOrderId` = `ft` + 30 hex). Verify via `GET /um/order`.
3. Filled entry appears in freqtrade (`/status` shows the trade and the entry order).
4. Place a `STOP_MARKET` stoploss: `POST /um/algo/order` with
   `algoType=CONDITIONAL`, `clientAlgoId` = `st` + 30 hex and `triggerPrice`
   slightly below entry.
   Verify `/pm_status` shows the stoploss order open.
5. Let the market hit the stop price (or move the stop price to trigger
   immediately). Verify: the Algo history returns `TRIGGERED` with a real
   `actualOrderId`; freqtrade closes the trade with non-zero `filled`/`average`; the
   position is flat on the exchange.
6. Repeat with a `STOP` (limit) stoploss whose real order rests in the book
   (NOT filled). Verify the strategy stays open in freqtrade and cancel it via
   `/um/algo/order` DELETE; verify freqtrade reflects `canceled`.
7. User-stream: with a trade open, restart the bot process; verify pending
   intents are resolved, the startup consistency check matches exchange state,
   and open stoploss orders are re-associated after reconnect.
8. Kill the network mid-POST (or simulate with `--pm-force-timeout` probe),
   restart, and verify no duplicate order is placed (same client id resolved).
9. Emergency: `/pm_close all CONFIRM` closes everything; `/pm_recover` shows a
   clean reconcile and clears any fail-closed block.

Until steps 1-9 complete successfully on the real account, this branch stays
`READ_ONLY_READY` (never `CANARY_READY` or `LIVE_READY`).
