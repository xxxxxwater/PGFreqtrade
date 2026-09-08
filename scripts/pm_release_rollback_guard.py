#!/usr/bin/env python3
"""Refuse rollback to a pre-dispatch-marker PM image while durable work is active.

Older PM images do not understand ``pm_outbox.dispatch_started_at`` or the
critical notification outbox. A rollback is safe only when the order pipeline
is quiescent and there are no pending durable critical notifications.
"""
from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine, inspect, text

ACTIVE_INTENT_STATES = ("PENDING", "PREPARED", "ACKED", "UNKNOWN")
ACTIVE_OUTBOX_STATES = ("PENDING", "ACKED", "LINKED")


def check_rollback_safety(engine) -> dict[str, int | bool]:
    tables = set(inspect(engine).get_table_names())
    result: dict[str, int | bool] = {
        "unresolved_intents": 0,
        "active_outbox": 0,
        "dispatch_marked_active_outbox": 0,
        "pending_notifications": 0,
        "safe": True,
    }
    with engine.connect() as conn:
        if "pm_order_intents" in tables:
            result["unresolved_intents"] = int(
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM pm_order_intents "
                        "WHERE state IN ('PENDING','PREPARED','ACKED','UNKNOWN')"
                    )
                ).scalar()
                or 0
            )
        if "pm_outbox" in tables:
            result["active_outbox"] = int(
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM pm_outbox "
                        "WHERE state IN ('PENDING','ACKED','LINKED')"
                    )
                ).scalar()
                or 0
            )
            cols = {c["name"] for c in inspect(engine).get_columns("pm_outbox")}
            if "dispatch_started_at" in cols:
                result["dispatch_marked_active_outbox"] = int(
                    conn.execute(
                        text(
                            "SELECT COUNT(*) FROM pm_outbox "
                            "WHERE state IN ('PENDING','ACKED','LINKED') "
                            "AND dispatch_started_at IS NOT NULL"
                        )
                    ).scalar()
                    or 0
                )
        if "pm_notification_outbox" in tables:
            result["pending_notifications"] = int(
                conn.execute(
                    text("SELECT COUNT(*) FROM pm_notification_outbox WHERE state='PENDING'")
                ).scalar()
                or 0
            )
    result["safe"] = not any(
        int(result[k])
        for k in (
            "unresolved_intents",
            "active_outbox",
            "dispatch_marked_active_outbox",
            "pending_notifications",
        )
    )
    return result


def main() -> int:
    db_url = os.environ.get("FREQTRADE_DB_URL")
    if not db_url:
        print("Set FREQTRADE_DB_URL to the database being considered for rollback.", file=sys.stderr)
        return 2
    result = check_rollback_safety(create_engine(db_url, future=True))
    if not result["safe"]:
        print(
            "ROLLBACK BLOCKED: order/notification pipeline is not quiescent: "
            f"unresolved_intents={result['unresolved_intents']} "
            f"active_outbox={result['active_outbox']} "
            f"dispatch_marked_active_outbox={result['dispatch_marked_active_outbox']} "
            f"pending_notifications={result['pending_notifications']}. "
            "Resolve/reconcile these rows on the current image before rollback."
        )
        return 1
    print("ROLLBACK GUARD OK: PM order pipeline and critical notification outbox are quiescent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
