"""PostgreSQL fault boundaries for the critical notification outbox."""

import os
import uuid

import pytest

from freqtrade.persistence import PMNotificationOutbox, Trade, init_db

pytest.importorskip("psycopg2")
PG_URL = os.environ.get("FREQTRADE_TEST_PG_URL")
if not PG_URL:
    pytest.skip("Set FREQTRADE_TEST_PG_URL for notification PG tests", allow_module_level=True)


def test_notification_pool_still_writes_when_business_pool_is_fully_checked_out():
    init_db(PG_URL)
    business_engine = Trade.session.get_bind()
    notification_engine = PMNotificationOutbox.session.get_bind()
    assert business_engine is not notification_engine

    pool = business_engine.pool
    max_overflow = int(getattr(pool, "_max_overflow", 0))
    capacity = int(pool.size()) + max(0, max_overflow)
    held = []
    try:
        for _ in range(capacity):
            held.append(business_engine.connect())
        assert pool.checkedout() == capacity

        key = f"pg-pool-{uuid.uuid4().hex}"
        row = PMNotificationOutbox(
            incident_id=key[:32],
            dedupe_key=key,
            channel="telegram",
            message='{"text":"critical"}',
            state="PENDING",
            priority=100,
        )
        PMNotificationOutbox.session.merge(row)
        PMNotificationOutbox.session.commit()
        assert PMNotificationOutbox.get_by_dedupe_key(key) is not None
    finally:
        PMNotificationOutbox.session.rollback()
        PMNotificationOutbox.session.remove()
        for conn in held:
            conn.close()
        Trade.session.remove()


def test_notification_pool_is_bounded_to_two_connections():
    init_db(PG_URL)
    notification_engine = PMNotificationOutbox.session.get_bind()
    pool = notification_engine.pool
    assert pool.size() == 2
    assert int(getattr(pool, "_max_overflow", -1)) == 0
