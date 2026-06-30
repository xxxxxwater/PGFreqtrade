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
| `max_daily_loss` | Maximum realized loss per UTC day before emergency stop. | _optional_ |

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

### `/pm_close [all|USDT|USDC]`
Creates market exit orders for open futures trades:
- `all` — close all trades regardless of settlement currency
- `USDT` — close only USDT-settled contracts
- `USDC` — close only USDC-settled contracts

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
