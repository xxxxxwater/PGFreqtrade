"""
Real process-level tests for the PM single-instance lock wrapper.

Every test spawns REAL separate Python processes (the wrapper script), so the
behavior verified here is exactly what production containers use - including
the wrapper child mode, lock release after child exit, SIGTERM/SIGKILL
recovery and env-from-file secret injection.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts.pm_single_instance import LOCK_BUSY_EXIT_CODE, acquire, read_env_file


SCRIPT = str(Path(__file__).resolve().parents[1] / "scripts" / "pm_single_instance.py")
PY = sys.executable


def _spawn_wrapper(lock_file: Path, extra_args: list[str]) -> subprocess.Popen:
    return subprocess.Popen(
        [PY, SCRIPT, "--lock-file", str(lock_file), *extra_args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_exit(proc: subprocess.Popen, timeout: float = 10.0) -> int:
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise AssertionError("wrapper did not exit in time")


def _wait_ready(lock_file: Path, wrapper: subprocess.Popen, timeout: float = 10.0) -> None:
    """Wait until the wrapper holds the lock (child spawned if any)."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if lock_file.exists() and wrapper.poll() is None:
            time.sleep(0.2)
            if wrapper.poll() is None:
                return
        time.sleep(0.05)
    out, err = wrapper.communicate()
    raise AssertionError(f"wrapper did not become ready; out={out!r} err={err!r}")


def test_read_env_file_preserves_special_characters(tmp_path):
    """Secret env files keep special characters intact (no shell mangling)."""
    secret = tmp_path / "secret.env"
    secret.write_text(
        "FREQTRADE__DB_URL=postgresql://u:p%40%24%23%21@h/db\n"
        "FREQTRADE__EXCHANGE__KEY=key-with-slash/and+plus\n"
        "EMPTYISH=\n"
        "# comment line\n"
    )
    env = read_env_file(str(secret))
    assert env["FREQTRADE__DB_URL"] == "postgresql://u:p%40%24%23%21@h/db"
    assert env["FREQTRADE__EXCHANGE__KEY"] == "key-with-slash/and+plus"
    assert "EMPTYISH" not in env


def test_db_url_with_special_char_password_round_trips():
    """A URL-encoded special-character password parses back to the original."""
    import urllib.parse

    from sqlalchemy.engine import make_url

    password = "p@$$w0rd:#%!'"
    encoded = urllib.parse.quote(password, safe="")
    url = f"postgresql://freqtrade:{encoded}@db:5432/freqtrade"
    parsed = make_url(url)
    assert parsed.password is not None
    assert urllib.parse.unquote(parsed.password) == password
    assert parsed.host == "db"
    assert parsed.database == "freqtrade"


SLEEP_CHILD = [PY, "-c", "import time; time.sleep(30)"]


def test_second_wrapper_process_is_rejected(tmp_path):
    """First wrapper (with a sleep child) holds the lock; a second standalone
    wrapper process must exit with a non-zero code."""
    lock_file = tmp_path / ".pm_instance.lock"
    first = _spawn_wrapper(lock_file, ["--exec", *SLEEP_CHILD])
    try:
        _wait_ready(lock_file, first)

        second = _spawn_wrapper(lock_file, [])
        rc = _wait_exit(second)
        assert rc == LOCK_BUSY_EXIT_CODE
        _, err = second.communicate()
        assert "already running" in (err or "")
    finally:
        first.terminate()
        first.wait(timeout=10)


def test_lock_released_after_child_exits(tmp_path):
    """After the first wrapper's child exits, a third process can re-acquire."""
    lock_file = tmp_path / ".pm_instance.lock"
    short_child = [PY, "-c", "import time; time.sleep(0.5)"]
    first = _spawn_wrapper(lock_file, ["--exec", *short_child])
    _wait_ready(lock_file, first)
    assert first.wait(timeout=10) == 0  # wrapper exits with the child's code

    fd = acquire(lock_file)  # re-acquirable in this process
    os.close(fd)


def test_wrapper_sigterm_releases_lock(tmp_path):
    """SIGTERM (terminate) on the wrapper releases the lock (no stale lock)."""
    lock_file = tmp_path / ".pm_instance.lock"
    wrapper = _spawn_wrapper(lock_file, [])
    _wait_ready(lock_file, wrapper)
    wrapper.terminate()
    _wait_exit(wrapper)

    fd = acquire(lock_file)  # lock must be re-acquirable
    os.close(fd)


def test_wrapper_sigkill_releases_lock(tmp_path):
    """SIGKILL (kill) on the wrapper also releases the lock via the OS."""
    lock_file = tmp_path / ".pm_instance.lock"
    wrapper = _spawn_wrapper(lock_file, [])
    _wait_ready(lock_file, wrapper)
    wrapper.kill()
    _wait_exit(wrapper)

    # Give the OS a moment to release the byte-range/flock.
    for _ in range(50):
        try:
            fd = acquire(lock_file)
            os.close(fd)
            return
        except SystemExit:
            time.sleep(0.1)
    raise AssertionError("lock was not released after SIGKILL")


def test_direct_acquire_mode_blocks_others(tmp_path):
    """Direct acquire mode (no --exec) holds the lock while the wrapper runs."""
    lock_file = tmp_path / ".pm_instance.lock"
    wrapper = _spawn_wrapper(lock_file, [])
    _wait_ready(lock_file, wrapper)

    with pytest.raises(SystemExit, match="already running"):
        acquire(lock_file)  # same-machine second holder must be refused

    wrapper.terminate()
    _wait_exit(wrapper)


def test_env_from_file_injected_into_child(tmp_path):
    """--env-from-file injects env into the child without command-line exposure."""
    lock_file = tmp_path / ".pm_instance.lock"
    secret_file = tmp_path / "secret.env"
    secret_file.write_text("FREQTRADE__DB_URL=postgresql://u:p%40ss@host/db\n")

    child = [PY, "-c", "import os,time; print(os.environ['FREQTRADE__DB_URL']); time.sleep(0.5)"]
    wrapper = subprocess.Popen(
        [
            PY,
            SCRIPT,
            "--lock-file",
            str(lock_file),
            "--env-from-file",
            str(secret_file),
            "--exec",
            *child,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    _wait_ready(lock_file, wrapper)
    out, _ = wrapper.communicate(timeout=15)
    assert wrapper.returncode == 0
    assert "postgresql://u:p%40ss@host/db" in out


def test_wrapper_forwards_exit_code(tmp_path):
    """The wrapper exits with the child's exit code."""
    lock_file = tmp_path / ".pm_instance.lock"
    failing_child = [PY, "-c", "import sys,time; time.sleep(0.4); sys.exit(7)"]
    wrapper = _spawn_wrapper(lock_file, ["--exec", *failing_child])
    assert _wait_exit(wrapper) == 7


# --- account-level lock (host-wide, keyed by exchange API key) ---


def _spawn_wrapper_with_env(
    lock_file: Path, account_dir: Path, env_file: Path, extra_args: list[str]
) -> subprocess.Popen:
    return subprocess.Popen(
        [
            PY,
            SCRIPT,
            "--lock-file",
            str(lock_file),
            "--account-lock-dir",
            str(account_dir),
            "--env-from-file",
            str(env_file),
            *extra_args,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_account_lock_path_derives_from_api_key(tmp_path):
    """Same API key -> same host-wide lock path, regardless of directory."""
    from scripts.pm_single_instance import account_lock_path

    env1 = {"FREQTRADE__EXCHANGE__KEY": "key-one"}
    p1 = account_lock_path(env1, tmp_path)
    assert p1 is not None
    assert p1.name.startswith("account-")
    assert account_lock_path({"FREQTRADE__EXCHANGE__KEY": "key-one"}, tmp_path) == p1
    assert account_lock_path({"FREQTRADE__EXCHANGE__KEY": "key-two"}, tmp_path) != p1
    assert account_lock_path({}, tmp_path) is None
    assert account_lock_path({"BINANCE_PM_API_KEY": "key-one"}, tmp_path) == p1


def test_same_key_second_wrapper_rejected_even_with_different_lock_file(tmp_path):
    """A second deployment directory with the SAME API key must be refused:
    the account-level lock is independent of the per-directory lock file."""
    env_file = tmp_path / "pm_env.txt"
    env_file.write_text("FREQTRADE__EXCHANGE__KEY=shared-account-key\n")
    lock1 = tmp_path / "dir1" / ".pm_instance.lock"
    lock2 = tmp_path / "dir2" / ".pm_instance.lock"
    lock1.parent.mkdir()
    lock2.parent.mkdir()
    account_dir = tmp_path / "account_locks"

    first = _spawn_wrapper_with_env(lock1, account_dir, env_file, ["--exec", *SLEEP_CHILD])
    try:
        _wait_ready(lock1, first)
        second = _spawn_wrapper_with_env(lock2, account_dir, env_file, [])
        assert _wait_exit(second) == LOCK_BUSY_EXIT_CODE
    finally:
        first.kill()
        first.wait()


def test_different_key_wrapper_is_allowed(tmp_path):
    """Two different accounts may run concurrently on the same host."""
    env1 = tmp_path / "env1.txt"
    env1.write_text("FREQTRADE__EXCHANGE__KEY=account-a\n")
    env2 = tmp_path / "env2.txt"
    env2.write_text("FREQTRADE__EXCHANGE__KEY=account-b\n")
    lock1 = tmp_path / "dir1" / ".pm_instance.lock"
    lock2 = tmp_path / "dir2" / ".pm_instance.lock"
    lock1.parent.mkdir()
    lock2.parent.mkdir()
    account_dir = tmp_path / "account_locks"

    first = _spawn_wrapper_with_env(lock1, account_dir, env1, ["--exec", *SLEEP_CHILD])
    try:
        _wait_ready(lock1, first)
        second = _spawn_wrapper_with_env(lock2, account_dir, env2, ["--exec", *SLEEP_CHILD])
        _wait_ready(lock2, second)
        assert second.poll() is None  # holds its own account lock happily
    finally:
        second.terminate()
        _wait_exit(second)
        first.kill()
        first.wait()
