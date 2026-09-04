#!/usr/bin/env python3
"""
Database backup helper for freqtrade (SQLite-first, with optional PostgreSQL note).

The default freqtrade database is SQLite (tradesv3.sqlite). SQLite is fine for a
single-node dry-run, but for a live PM deployment you should either:

A) Take regular SQLite backups with this script, or
B) Move to PostgreSQL (recommended for live):
       freqtrade convert-db --db-url sqlite:///user_data/tradesv3.sqlite \
                            --db-url-out postgresql://user:pass@localhost:5432/freqtrade
   then start the bot with --db-url postgresql://...

Usage:
    python scripts/backup_db.py --db-url sqlite:///user_data/tradesv3.sqlite \
        --backup-dir user_data/backups --keep 7
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path


def backup_sqlite(db_path: Path, backup_dir: Path, keep: int) -> Path:
    db_path = db_path.resolve()
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target = backup_dir / f"{db_path.stem}-{stamp}.sqlite"
    # Use SQLite online backup API so the live DB can be copied safely while the bot runs.
    src = sqlite3.connect(str(db_path))
    dst = sqlite3.connect(str(target))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    print(f"Backup written to {target}")
    _prune(backup_dir, db_path.stem, keep)
    return target


def _prune(backup_dir: Path, stem: str, keep: int) -> None:
    backups = sorted(backup_dir.glob(f"{stem}-*.sqlite"))
    for old in backups[:-keep] if keep > 0 else []:
        old.unlink()
        print(f"Pruned old backup {old}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backup a freqtrade SQLite database.")
    parser.add_argument(
        "--db-url", required=True, help="Database URL, e.g. sqlite:///path/tradesv3.sqlite"
    )
    parser.add_argument(
        "--backup-dir", default="user_data/backups", help="Directory to store backups."
    )
    parser.add_argument(
        "--keep", type=int, default=7, help="Number of backups to keep (0 = keep all)."
    )
    args = parser.parse_args()

    db_url = args.db_url
    if db_url.startswith("sqlite:///"):
        db_path = Path(db_url.replace("sqlite:///", "", 1))
        backup_sqlite(db_path, Path(args.backup_dir), args.keep)
    elif db_url.startswith("postgresql"):
        print(
            "PostgreSQL detected - use pg_dump for consistent backups, e.g.:\n"
            "  pg_dump 'postgresql://user:pass@localhost:5432/freqtrade' > backup.sql\n"
            "or use the freqtrade convert-db workflow for SQLite migrations.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    else:
        raise SystemExit(f"Unsupported db-url: {db_url}")


if __name__ == "__main__":
    main()
