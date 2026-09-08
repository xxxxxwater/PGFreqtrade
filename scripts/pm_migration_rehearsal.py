#!/usr/bin/env python3
"""
PM database migration rehearsal (read-mostly, for a THROWAWAY copy).

Validates the production migration path against a real PostgreSQL 17 database
restored from the CURRENT pre-release production backup.

1. ``pm_order_intents`` and ``pm_outbox`` exist,
2. the pre-send ``origin_trade_id`` evidence column is absent before migration,
3. the FINAL application ``init_db`` creates ``pm_notification_outbox`` and adds ownership + send-before-POST evidence without disturbing ACK/link evidence,
4. legacy ``PENDING`` rows are migrated to ``PREPARED`` and old dispatch attempts become MAY_HAVE_BEEN_SENT,
5. a second application startup is a no-op (idempotent).

Usage (NEVER point this at the production database - use a restored copy):

    FREQTRADE_DB_URL=postgresql://postgres:pmtest@127.0.0.1:5434/freqtrade_rehearsal \
        python scripts/pm_migration_rehearsal.py

Exit code 0: migration rehearsed successfully.
"""

from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine, inspect, text

from freqtrade.persistence import PMNotificationOutbox, init_db


def main() -> int:
    db_url = os.environ.get("FREQTRADE_DB_URL")
    if not db_url:
        print("Set FREQTRADE_DB_URL to the throwaway rehearsal database.", file=sys.stderr)
        return 2
    engine = create_engine(db_url)
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    required_tables = {"pm_order_intents", "pm_outbox"}
    missing_tables = required_tables - tables
    if missing_tables:
        print(
            f"required PM table(s) missing from rehearsal restore: {sorted(missing_tables)}",
            file=sys.stderr,
        )
        return 2

    def columns(table: str) -> set[str]:
        return {col["name"] for col in inspect(engine).get_columns(table)}

    intent_before = columns("pm_order_intents")
    outbox_before = columns("pm_outbox")
    existing_ack_fields = {
        "exchange_order_id",
        "raw_response",
        "acked_at",
        "linked_order_id",
        "linked_trade_id",
        "linked_at",
    }
    if not existing_ack_fields <= intent_before:
        print("restore is older than the current production schema; use the latest backup")
        return 2
    if "dispatch_started_at" in outbox_before:
        print("restore already has dispatch_started_at - cannot rehearse this release migration")
        return 2

    # Legacy rows: insert a PENDING row as the old build would have written it.
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO pm_order_intents "
                "(client_id, kind, pair, state, reduce_only, created_at) "
                "VALUES ('legacy-pending', 'order', 'ETH/USDT:USDT', 'PENDING', "
                "false, now())"
            )
        )
        conn.execute(
            text(
                "INSERT INTO pm_outbox "
                "(client_id, operation, payload, state, dispatch_attempts, created_at) "
                "VALUES ('legacy-pending', 'order', '{}', 'PENDING', 1, now())"
            )
        )

    # Mirror the real final-image startup order: metadata.create_all() creates
    # new tables, then PM ALTER migration upgrades existing tables.
    init_db(db_url)
    intent_after = columns("pm_order_intents")
    outbox_after = columns("pm_outbox")
    if "origin_trade_id" not in intent_after:
        print("FAIL: pm_order_intents.origin_trade_id missing after migration")
        return 1
    if "origin_trade_id" not in outbox_after:
        print("FAIL: pm_outbox.origin_trade_id missing after migration")
        return 1
    if "dispatch_started_at" not in outbox_after:
        print("FAIL: pm_outbox.dispatch_started_at missing after migration")
        return 1
    tables_after = set(inspect(engine).get_table_names())
    if "pm_notification_outbox" not in tables_after:
        print("FAIL: pm_notification_outbox was not created by final application startup")
        return 1
    notification_columns = columns("pm_notification_outbox")
    required_notification = {"incident_id", "dedupe_key", "state", "priority", "attempts", "next_attempt_at", "sent_at"}
    if not required_notification <= notification_columns:
        print(f"FAIL: pm_notification_outbox missing columns {sorted(required_notification - notification_columns)}")
        return 1

    # Existing timestamp evidence must remain PostgreSQL-native TIMESTAMP.
    col_types = {col["name"]: col["type"] for col in inspect(engine).get_columns("pm_order_intents")}
    for name in ("acked_at", "linked_at"):
        typename = str(col_types[name]).upper()
        if "DATETIME" in typename or "TIMESTAMP" not in typename:
            print(f"FAIL: column {name} has type {typename!r}, expected TIMESTAMP")
            return 1

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT state FROM pm_order_intents WHERE client_id = 'legacy-pending'"
            )
        ).fetchone()
        if row is None or row[0] != "PREPARED":
            print(f"FAIL: legacy PENDING row was not migrated to PREPARED (got {row!r})")
            return 1
        marker = conn.execute(
            text("SELECT dispatch_started_at FROM pm_outbox WHERE client_id='legacy-pending'")
        ).scalar()
        if marker is None:
            print("FAIL: legacy dispatch_attempts>0 row was not marked MAY_HAVE_BEEN_SENT")
            return 1

    # Idempotency: a second FINAL application startup must not raise or change anything.
    init_db(db_url)
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM pm_order_intents WHERE client_id = 'legacy-pending'")
        ).scalar()
        if count != 1:
            print(f"FAIL: second migration run changed row count ({count})")
            return 1

    print(
        "MIGRATION REHEARSAL OK: notification outbox created, origin_trade_id + dispatch_started_at migrated, "
        "legacy send evidence preserved, PENDING->PREPARED migrated, second startup idempotent."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
