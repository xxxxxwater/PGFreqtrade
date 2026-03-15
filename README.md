# ![PG Research](https://pgresearch.org/assets/pgresearch-logo.svg)

# PGFreqtrade

PGFreqtrade is a PG Research–maintained trading bot distribution based on the Freqtrade engine, with custom exchange integrations and container-ready deployment for production environments. It is intended for research, simulation, and live execution workflows managed by PG Research.

For more information about PG Research, visit [pgresearch.org](https://pgresearch.org).

## Overview

PGFreqtrade provides:

- **PG Research branding and packaging** aligned with the PG Research ecosystem.
- **CCXT-based exchange integrations**, including Coinbase Advanced Trade via the `coinbase` exchange id.
- **Docker-first deployment** for servers and lab environments.
- **Backtesting, live trading, and strategy experimentation** under a single workflow.

## Disclaimer

PG Research provides this software for research and educational use. Trading carries significant risk. You are solely responsible for evaluating and managing any financial risk. PG Research assumes no responsibility for trading outcomes.

Always start in dry-run mode and validate strategies before moving to live execution.

## Supported Exchanges (Spot)

PGFreqtrade tracks CCXT exchange coverage while prioritizing tested integrations. Current spot exchanges include:

- Binance
- BingX
- Bitget
- Bitmart
- Bybit
- Coinbase Advanced Trade (`coinbase`)
- Gate.io
- HTX
- Hyperliquid (DEX)
- Kraken
- OKX / MyOKX

Community-tested exchanges:

- Bitvavo
- Kucoin

If an exchange is supported by CCXT but not listed above, it may still work. Validate in dry-run before production use.

## Supported Exchanges (Futures)

- Binance
- Bitget
- Gate.io
- Hyperliquid
- OKX
- Bybit
- Coinbase Advanced Trade (`coinbase`) — isolated futures mode with explicit portfolio configuration

Refer to `docs/exchanges.md` and `docs/leverage.md` for futures-specific guidance.

## Quick Start (Docker)

The repository ships with a Dockerfile and a Docker Compose example preconfigured for PGFreqtrade.

```bash
docker compose build
```

```bash
docker compose up -d
```

Configuration lives under `./user_data`, including `config.json`, strategies, and logs.

## Exchange Configuration (Coinbase Advanced Trade)

Use the `coinbase` exchange id in your configuration:

```json
"exchange": {
    "name": "coinbase",
    "key": "your_exchange_key",
    "secret": "your_exchange_secret"
}
```

## Features

- **Python 3.11+ runtime**
- **SQLite persistence**
- **Dry-run and live trading modes**
- **Backtesting and data conversion**
- **Strategy optimization (including ML workflows)**
- **Web UI for monitoring and control**
- **Telegram integration**

## Documentation

PGFreqtrade documentation is maintained by PG Research. Visit [pgresearch.org](https://pgresearch.org) for the latest updates and deployment notes.

## Support

For support and collaboration, please contact the PG Research team via [pgresearch.org](https://pgresearch.org).
