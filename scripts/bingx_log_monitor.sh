#!/usr/bin/env bash

set -euo pipefail

LOG_FILE="${1:-user_data/logs/freqtrade.log}"

if [ ! -f "$LOG_FILE" ]; then
  echo "log file not found: $LOG_FILE" >&2
  exit 1
fi

echo "Monitoring $LOG_FILE"
tail -F "$LOG_FILE" | grep --line-buffered -iE "stoploss|cancel|error|ddos|rate|timeout|too many requests|order not exist"
