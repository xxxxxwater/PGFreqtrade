"""Regression tests for lock ownership across pool and engine boundaries."""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from freqtrade.exchange.pm_locks import pm_pipeline_lock


def lock_fixture():
    conn = MagicMock()
    conn.invalidated = False
    conn.invalidate.side_effect = lambda: setattr(conn, "invalidated", True)
    conn.execute.return_value.scalar.side_effect = [True, True]
    engine = MagicMock()
    engine.dialect = SimpleNamespace(name="postgresql")
    engine.connect.return_value = conn
    session = MagicMock()
    session.get_bind.return_value = engine
    return session, engine, conn


def test_unlock_failure_invalidates_before_return_to_pool():
    session, _, conn = lock_fixture()
    conn.execute.side_effect = [MagicMock(scalar=MagicMock(return_value=True)),
                                RuntimeError("unlock failed")]
    with pytest.raises(RuntimeError, match="unlock failed"):
        with pm_pipeline_lock(session):
            session.commit()
            session.rollback()
    conn.invalidate.assert_called_once()
    conn.close.assert_called_once()


def test_nested_same_engine_reuses_connection():
    session, engine, conn = lock_fixture()
    with pm_pipeline_lock(session):
        with pm_pipeline_lock(session):
            session.commit()
    engine.connect.assert_called_once()
    assert conn.execute.call_count == 2
    conn.invalidate.assert_not_called()


def test_nested_other_engine_acquires_its_own_lock():
    s1, e1, c1 = lock_fixture()
    s2, e2, c2 = lock_fixture()
    with pm_pipeline_lock(s1):
        with pm_pipeline_lock(s2):
            pass
    e1.connect.assert_called_once()
    e2.connect.assert_called_once()
    assert c1.execute.call_count == c2.execute.call_count == 2
