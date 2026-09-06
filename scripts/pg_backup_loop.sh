#!/bin/sh
# PostgreSQL backup loop for the PM production stack.
#
# Guarantees:
#   - pg_dump writes to a TEMP file first; only a complete dump is atomically
#     renamed to the final name (a truncated/failed dump can never appear as a
#     valid backup).
#   - Every final backup gets a SHA-256 checksum; restore verifies it.
#   - A periodic restore smoke test restores the newest backup into a scratch
#     database and verifies all seven trading/PM pipeline tables exist.
#   - Backup health (last OK / last failure) is exposed in BACKUP_HEALTH.
#
# Env vars: PG_HOST, PG_USER, PG_DB, BACKUP_DIR, KEEP, BACKUP_INTERVAL_SECS,
#           RESTORE_SMOKE_EVERY_SECS. PGPASSWORD must be set for auth.

set -u

BACKUP_DIR="${BACKUP_DIR:-/backups}"
# Keep 48 hourly backups (~2 days).  The database is a few MB per dump, so
# this is cheap; combined with the host-level Alibaba hbrclient volume backup
# it gives a practical recovery window.  Set KEEP explicitly via compose.
KEEP="${KEEP:-48}"
SLEEP="${BACKUP_INTERVAL_SECS:-3600}"
RESTORE_SMOKE_EVERY_SECS="${RESTORE_SMOKE_EVERY_SECS:-86400}"
LAST_SMOKE_FILE="${BACKUP_DIR}/.last_restore_smoke"
HEALTH_FILE="${BACKUP_DIR}/BACKUP_HEALTH"
PG_HOST="${PG_HOST:-db}"
PG_USER="${PG_USER:-freqtrade}"
PG_DB="${PG_DB:-freqtrade}"

mkdir -p "$BACKUP_DIR"

# Build ~/.pgpass from the Docker secret (PGPASSWORD_FILE) so the password
# never appears in `docker inspect` or the process environment listing.
if [ -z "${PGPASSWORD:-}" ] && [ -n "${PGPASSWORD_FILE:-}" ] && [ -f "$PGPASSWORD_FILE" ]; then
    PASS=$(cat "$PGPASSWORD_FILE" | tr -d '\r\n')
    umask 077
    # The database field MUST be '*': dropdb/createdb connect to the
    # 'postgres' maintenance database, not to $PG_DB, so a pgpass line that
    # only matches $PG_DB silently fails and the non-interactive client then
    # hangs forever on the password prompt (observed: dropdb stuck 31h).
    echo "${PG_HOST}:5432:*:${PG_USER}:${PASS}" > "${HOME}/.pgpass"
    chmod 600 "${HOME}/.pgpass"
    unset PASS
fi

# Never prompt for a password and never wait forever on a dead connection:
# fail fast so the health file records a real error instead of a stuck loop.
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-10}"

fail() {
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) FAILED: $1" > "$HEALTH_FILE"
    echo "backup failed: $1" >&2
}

backup_once() {
    STAMP=$(date -u +%Y%m%dT%H%M%SZ)
    TMP="${BACKUP_DIR}/.pg_${STAMP}.sql.tmp"
    FINAL="${BACKUP_DIR}/pg_${STAMP}.sql"

    if ! pg_dump -w -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" > "$TMP" 2> "${BACKUP_DIR}/.last_error"; then
        fail "pg_dump error: $(cat "${BACKUP_DIR}/.last_error" 2>/dev/null)"
        rm -f "$TMP"
        return 1
    fi

    # Durability: flush the dump to disk BEFORE it is published.
    sync

    sha256sum "$TMP" | awk '{print $1}' > "${TMP}.sha256"

    # Atomic publish: mv within the same filesystem is atomic; a reader can
    # never observe a partially written backup under the final name.
    if ! mv "$TMP" "$FINAL"; then
        fail "mv to final name failed"
        rm -f "$TMP" "${TMP}.sha256"
        return 1
    fi
    mv "${TMP}.sha256" "${FINAL}.sha256"
    sync

    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) OK $FINAL" > "$HEALTH_FILE"
    echo "backup OK: $FINAL"

    # Rotation
    ls -1t "${BACKUP_DIR}"/pg_*.sql 2>/dev/null | tail -n +$((KEEP + 1)) | while read -r old; do
        rm -f "$old" "${old}.sha256"
        echo "rotated away: $old"
    done
    return 0
}

restore_smoke() {
    NOW=$(date +%s)
    LAST=$(cat "$LAST_SMOKE_FILE" 2>/dev/null || echo 0)
    if [ $((NOW - LAST)) -lt "$RESTORE_SMOKE_EVERY_SECS" ]; then
        return 0
    fi
    NEWEST=$(ls -1t "${BACKUP_DIR}"/pg_*.sql 2>/dev/null | head -n 1)
    if [ -z "$NEWEST" ]; then
        return 0
    fi

    # Verify checksum BEFORE restoring.
    EXPECTED=$(cat "${NEWEST}.sha256" 2>/dev/null || true)
    ACTUAL=$(sha256sum "$NEWEST" | awk '{print $1}')
    if [ -z "$EXPECTED" ] || [ "$EXPECTED" != "$ACTUAL" ]; then
        fail "checksum mismatch for $NEWEST"
        return 1
    fi

    START_NS=$(date +%s%N)
    SCRATCH="${PG_DB}_restore_smoke"
    dropdb -w -h "$PG_HOST" -U "$PG_USER" --if-exists "$SCRATCH" 2>/dev/null || true
    if ! createdb -w -h "$PG_HOST" -U "$PG_USER" "$SCRATCH" 2>/dev/null; then
        fail "createdb failed for restore smoke test"
        return 1
    fi
    # ON_ERROR_STOP: ANY SQL error (DDL, COPY, constraint) fails the restore
    # immediately - a partially restored database can never pass as success.
    if ! psql -w -v ON_ERROR_STOP=1 -h "$PG_HOST" -U "$PG_USER" -d "$SCRATCH" -f "$NEWEST" >/dev/null 2>&1; then
        fail "restore smoke test failed for $NEWEST"
        dropdb -w -h "$PG_HOST" -U "$PG_USER" --if-exists "$SCRATCH" 2>/dev/null || true
        return 1
    fi

    # Every table the trading pipeline depends on must exist after restore.
    # This release adds pm_stream_journal. Pre-upgrade backups must first be
    # restored and migrated in isolation before they can pass this release's
    # seven-table readiness check; do not silently waive the journal check.
    for TABLE in trades orders pm_order_intents pm_outbox pm_signal_ledger pm_candle_watermarks pm_stream_journal; do
        TABLE_CHECK=$(psql -w -h "$PG_HOST" -U "$PG_USER" -d "$SCRATCH" -tAc \
            "SELECT to_regclass('public.$TABLE') IS NOT NULL")
        if [ "$TABLE_CHECK" != "t" ]; then
            fail "restore smoke: table $TABLE missing after restore"
            dropdb -w -h "$PG_HOST" -U "$PG_USER" --if-exists "$SCRATCH" 2>/dev/null || true
            return 1
        fi
    done

    # Key referential sanity: order rows must reference an existing trade.
    # The query exit code is checked AND only an explicit numeric "0" is
    # accepted - an empty/failed query must fail the smoke test, never pass.
    ORPHANS=""
    if ! ORPHANS=$(psql -w -h "$PG_HOST" -U "$PG_USER" -d "$SCRATCH" -tAc \
        "SELECT count(*) FROM orders WHERE ft_trade_id IS NOT NULL AND ft_trade_id NOT IN (SELECT id FROM trades)" 2>/dev/null); then
        dropdb -w -h "$PG_HOST" -U "$PG_USER" --if-exists "$SCRATCH" 2>/dev/null || true
        fail "restore smoke: orphan-check query failed"
        return 1
    fi
    dropdb -w -h "$PG_HOST" -U "$PG_USER" "$SCRATCH" 2>/dev/null || true
    if [ "$ORPHANS" != "0" ]; then
        fail "restore smoke: $ORPHANS orphaned order rows (ft_trade_id without trade)"
        return 1
    fi

    ELAPSED_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))
    echo "$NOW" > "$LAST_SMOKE_FILE"
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) OK $NEWEST elapsed_ms=$ELAPSED_MS" >> "${BACKUP_DIR}/RESTORE_HEALTH"
    echo "restore smoke test OK: $NEWEST elapsed_ms=$ELAPSED_MS"
    return 0
}

while true; do
    backup_once
    restore_smoke
    sleep "$SLEEP"
done
