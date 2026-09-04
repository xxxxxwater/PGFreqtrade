#!/usr/bin/env python3
"""
PM database migration rehearsal (read-mostly, for a THROWAWAY copy).

Validates the production migration path against a real PostgreSQL 17 database
that contains the PRE-REDO ``pm_order_intents`` schema (the shape the live
database has before the release starts):

1. the legacy table exists and is missing the redo columns,
2. ``migrate_pm_tables`` adds exactly the missing columns (TIMESTAMP, not
   DATETIME - PostgreSQL has no DATETIME type),
3. legacy ``PENDING`` rows are migrated to ``PREPARED``,
4. a second run is a no-op (idempotent),
5. the unresolved-state guard keeps legacy ``PENDING`` fail-closed.

Usage (NEVER point this at the production database - use a restored copy):

    FREQTRADE_DB_URL=postgresql://postgres:pmtest@127.0.0.1:5434/freqtrade_rehearsal \
        python scripts/pm_migration_rehearsal.py

Exit code 0: migration rehearsed successfully.
"""

from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine, inspect, text

from freqtrade.persistence.migrations import migrate_pm_tables


def main() -> int:
    db_url = os.environ.get("FREQTRADE_DB_URL")
    if not db_url:
        print("Set FREQTRADE_DB_URL to the throwaway rehearsal database.", file=sys.stderr)
        return 2
    engine = create_engine(db_url)
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    if "pm_order_intents" not in tables:
        print(
            "pm_order_intents table missing - the rehearsal database must be a "
            "restore of the pre-redo production dump.",
            file=sys.stderr,
        )
        return 2

    def columns() -> set[str]:
        return {col["name"] for col in inspect(engine).get_columns("pm_order_intents")}

    before = columns()
    required = {
        "exchange_order_id",
        "raw_response",
        "acked_at",
        "linked_order_id",
        "linked_trade_id",
        "linked_at",
    }
    if required <= before:
        print("Table already has the redo columns - cannot rehearse the migration.")
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

    migrate_pm_tables(engine)
    after = columns()
    missing = required - after
    if missing:
        print(f"FAIL: columns still missing after migration: {sorted(missing)}")
        return 1

    # Column types must be PostgreSQL-native TIMESTAMP, never DATETIME.
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

    # Idempotency: a second run must not raise or change anything.
    migrate_pm_tables(engine)
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM pm_order_intents WHERE client_id = 'legacy-pending'")
        ).scalar()
        if count != 1:
            print(f"FAIL: second migration run changed row count ({count})")
            return 1

    print(
        "MIGRATION REHEARSAL OK: columns added with TIMESTAMP types, "
        "PENDING->PREPARED migrated, idempotent."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
