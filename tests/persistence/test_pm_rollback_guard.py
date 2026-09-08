from sqlalchemy import create_engine, text

from scripts.pm_release_rollback_guard import check_rollback_safety


def _engine():
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE pm_order_intents (id INTEGER PRIMARY KEY, state TEXT)"))
        c.execute(text("CREATE TABLE pm_outbox (id INTEGER PRIMARY KEY, state TEXT, dispatch_started_at TIMESTAMP)"))
        c.execute(text("CREATE TABLE pm_notification_outbox (id INTEGER PRIMARY KEY, state TEXT)"))
    return engine


def test_rollback_guard_allows_quiescent_pipeline():
    assert check_rollback_safety(_engine())["safe"] is True


def test_rollback_guard_blocks_dispatch_marker_even_without_attempt_counter():
    engine = _engine()
    with engine.begin() as c:
        c.execute(text("INSERT INTO pm_outbox (id,state,dispatch_started_at) VALUES (1,'PENDING',CURRENT_TIMESTAMP)"))
    result = check_rollback_safety(engine)
    assert result["safe"] is False
    assert result["dispatch_marked_active_outbox"] == 1


def test_rollback_guard_blocks_unresolved_intent_and_pending_notification():
    engine = _engine()
    with engine.begin() as c:
        c.execute(text("INSERT INTO pm_order_intents (id,state) VALUES (1,'UNKNOWN')"))
        c.execute(text("INSERT INTO pm_notification_outbox (id,state) VALUES (1,'PENDING')"))
    result = check_rollback_safety(engine)
    assert result["safe"] is False
    assert result["unresolved_intents"] == 1
    assert result["pending_notifications"] == 1
