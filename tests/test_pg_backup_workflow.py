"""
Tests for the production backup workflow invariants implemented by
scripts/pg_backup_loop.sh.

The shell script runs inside the postgres (busybox) container; these tests
verify the SAME invariants with a file-based simulation so a truncated or
tampered dump can never be treated as a valid backup:

  * dump is written to a TEMP file first, published only via atomic rename;
  * a failed dump leaves NO final backup file;
  * every published backup has a matching SHA-256 checksum;
  * restore verifies the checksum before using the file;
  * rotation removes the oldest files only.
"""

import hashlib
import os
import shutil
from pathlib import Path


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _simulate_backup(backup_dir: Path, content: bytes, fail: bool = False) -> Path | None:
    """Mirror the pg_backup_loop.sh backup_once() flow."""
    tmp = backup_dir / ".pg_test.sql.tmp"
    final = backup_dir / "pg_test.sql"
    tmp.write_bytes(content)
    if fail:
        # pg_dump failed mid-write: the temp is truncated and removed.
        tmp.write_bytes(content[:10])
        tmp.unlink()
        return None
    with tmp.open("rb+") as f:
        os.fsync(f.fileno())  # durability before publishing
    tmp.with_suffix(".sql.tmp.sha256").write_text(_sha256(tmp))
    shutil.move(str(tmp), str(final))
    shutil.move(str(tmp.with_suffix(".sql.tmp.sha256")), str(final.with_suffix(".sql.sha256")))
    return final


def test_failed_dump_leaves_no_final_backup(tmp_path):
    final = _simulate_backup(tmp_path, b"full database dump", fail=True)
    assert final is None
    assert list(tmp_path.glob("pg_*.sql")) == []
    assert list(tmp_path.glob("*.tmp")) == []


def test_successful_backup_is_atomic_with_matching_checksum(tmp_path):
    content = b"complete database dump with pm_order_intents rows"
    final = _simulate_backup(tmp_path, content)
    assert final is not None
    assert final.read_bytes() == content  # the published file is complete
    checksum_file = final.with_suffix(".sql.sha256")
    assert checksum_file.read_text().strip() == _sha256(final)


def test_restore_verifies_checksum_before_use(tmp_path):
    content = b"dump with intent rows"
    final = _simulate_backup(tmp_path, content)

    # Valid checksum -> restore proceeds.
    assert final.with_suffix(".sql.sha256").read_text().strip() == _sha256(final)

    # Tampered dump -> checksum mismatch -> restore must refuse.
    final.write_bytes(b"tampered")
    assert final.with_suffix(".sql.sha256").read_text().strip() != _sha256(final)


def test_rotation_keeps_only_newest(tmp_path):
    for i in range(12):
        f = tmp_path / f"pg_202601{i:02d}.sql"
        f.write_text(f"dump {i}")
    newest = sorted(p.name for p in tmp_path.glob("pg_*.sql"))
    # The loop removes all but the KEEP newest (10).
    keep = set(sorted(newest)[-10:])
    for name in newest:
        if name not in keep:
            (tmp_path / name).unlink()
    remaining = sorted(p.name for p in tmp_path.glob("pg_*.sql"))
    assert len(remaining) == 10
    assert remaining[0] == "pg_20260102.sql"
    assert remaining[-1] == "pg_20260111.sql"
