"""
Tests for the cross-platform single-instance lock used in production.
"""

import os

import pytest

from scripts.pm_single_instance import acquire


def test_lock_acquires_and_second_instance_fails(tmp_path):
    lock_file = tmp_path / ".pm_instance.lock"

    fd = acquire(lock_file)  # first instance
    assert lock_file.exists()
    assert os.fstat(fd).st_size > 0

    with pytest.raises(SystemExit, match="already running"):
        acquire(lock_file)  # second instance must be refused

    os.close(fd)


def test_lock_file_is_created_in_missing_dir(tmp_path):
    lock_file = tmp_path / "nested" / ".pm_instance.lock"
    fd = acquire(lock_file)
    assert lock_file.exists()
    os.close(fd)


def test_lock_released_after_close(tmp_path):
    lock_file = tmp_path / ".pm_instance.lock"
    fd = acquire(lock_file)
    os.close(fd)

    # After the fd is closed the lock is released - a new instance can acquire.
    fd2 = acquire(lock_file)
    os.close(fd2)
