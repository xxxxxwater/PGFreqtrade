#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${PM_WATCHDOG_CONTAINER:-pgfreqtrade-native-freqtrade-pm-1}"
LOG_FILE="${PM_WATCHDOG_LOG:-/root/PMBinanceJP-native/PGFreqtrade/user_data/logs/freqtrade.log}"
MAX_AGE="${PM_WATCHDOG_MAX_AGE:-180}"
STARTUP_GRACE="${PM_WATCHDOG_STARTUP_GRACE:-300}"
CONFIRM_SECONDS="${PM_WATCHDOG_CONFIRM_SECONDS:-60}"
STRIKE_FILE="${PM_WATCHDOG_STRIKE_FILE:-/run/pgfreqtrade-heartbeat-watchdog.strike}"
CHECK_ONLY=0
[[ "${1:-}" == "--check-only" ]] && CHECK_ONLY=1

log() {
    logger -t pgfreqtrade-watchdog -- "$*" || true
    printf '%s\n' "$*"
}

# Do not countermand an explicit `docker stop`. Docker's unless-stopped policy
# handles crashes; this watchdog is specifically for a RUNNING-but-stuck process.
running="$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null || printf 'false')"
if [[ "$running" != "true" ]]; then
    rm -f "$STRIKE_FILE"
    log "container=$CONTAINER not running; no watchdog restart (respecting operator/docker policy)."
    exit 0
fi

now="$(date +%s)"
started_raw="$(docker inspect -f '{{.State.StartedAt}}' "$CONTAINER")"
started="$(date -d "$started_raw" +%s 2>/dev/null || printf '0')"
uptime=$(( now - started ))
if (( started <= 0 || uptime < STARTUP_GRACE )); then
    rm -f "$STRIKE_FILE"
    log "container=$CONTAINER startup_grace active uptime=${uptime}s."
    exit 0
fi

if [[ ! -r "$LOG_FILE" ]]; then
    heartbeat_age=$(( MAX_AGE + 1 ))
    last_ts="unreadable"
else
    line="$(grep 'Bot heartbeat\.' "$LOG_FILE" | tail -n 1 || true)"
    if [[ -z "$line" ]]; then
        heartbeat_age=$(( MAX_AGE + 1 ))
        last_ts="missing"
    else
        last_ts="${line:0:19}"
        last_epoch="$(date -d "$last_ts" +%s 2>/dev/null || printf '0')"
        if (( last_epoch <= 0 )); then
            heartbeat_age=$(( MAX_AGE + 1 ))
        else
            heartbeat_age=$(( now - last_epoch ))
            (( heartbeat_age < 0 )) && heartbeat_age=0
        fi
    fi
fi

if (( heartbeat_age <= MAX_AGE )); then
    rm -f "$STRIKE_FILE"
    log "healthy container=$CONTAINER heartbeat_age=${heartbeat_age}s last='$last_ts'."
    exit 0
fi

if (( CHECK_ONLY )); then
    log "STALE(check-only) container=$CONTAINER heartbeat_age=${heartbeat_age}s last='$last_ts'."
    exit 0
fi

if [[ ! -f "$STRIKE_FILE" ]]; then
    printf '%s\n' "$now" > "$STRIKE_FILE"
    log "STALE first strike container=$CONTAINER heartbeat_age=${heartbeat_age}s; waiting for confirmation."
    exit 0
fi

first="$(cat "$STRIKE_FILE" 2>/dev/null || printf '%s' "$now")"
[[ "$first" =~ ^[0-9]+$ ]] || first="$now"
confirm_age=$(( now - first ))
if (( confirm_age < CONFIRM_SECONDS )); then
    log "STALE awaiting confirmation container=$CONTAINER heartbeat_age=${heartbeat_age}s confirm_age=${confirm_age}s."
    exit 0
fi

log "CRITICAL stale heartbeat confirmed container=$CONTAINER age=${heartbeat_age}s; restarting bot container only."
if docker restart -t 30 "$CONTAINER" >/dev/null; then
    rm -f "$STRIKE_FILE"
    log "restart succeeded container=$CONTAINER; persistent bot state/cold reconcile will govern re-entry."
else
    log "restart FAILED container=$CONTAINER."
    exit 1
fi
