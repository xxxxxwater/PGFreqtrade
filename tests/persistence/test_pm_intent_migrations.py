"""Regression tests for PM order-intent schema upgrades."""

from __future__ import annotations

from sqlalchemy import create_engine, inspect, text

from freqtrade.persistence import migrations
from freqtrade.persistence.migrations import migrate_pm_tables
from freqtrade.persistence.pm_order_intent import UNRESOLVED_STATES


def _create_legacy_intent_table(engine) -> None:
    """Create the small pre-redo schema which exists on an upgraded PM database."""
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE pm_order_intents ("
                "id INTEGER PRIMARY KEY, "
                "client_id VARCHAR(40) NOT NULL, "
                "state VARCHAR(16) NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO pm_order_intents (id, client_id, state) VALUES "
                "(1, 'legacy-pending', 'PENDING'), "
                "(2, 'legacy-lowercase', 'pending'), "
                "(3, 'legacy-unknown', 'UNKNOWN')"
            )
        )


def test_pm_intent_migration_sqlite_adds_timestamp_columns_and_maps_pending() -> None:
    """A legacy SQLite database upgrades safely and the migration is idempotent."""
    engine = create_engine("sqlite://")
    _create_legacy_intent_table(engine)

    migrate_pm_tables(engine)

    columns = {column["name"]: str(column["type"]).upper() for column in inspect(engine).get_columns(
        "pm_order_intents"
    )}
    assert columns["acked_at"] == "TIMESTAMP"
    assert columns["linked_at"] == "TIMESTAMP"
    assert {"exchange_order_id", "raw_response", "linked_order_id", "linked_trade_id"} <= set(
        columns
    )
    with engine.connect() as connection:
        states = dict(
            connection.execute(
                text("SELECT client_id, state FROM pm_order_intents ORDER BY id")
            ).all()
        )
    assert states == {
        "legacy-pending": "PREPARED",
        "legacy-lowercase": "PREPARED",
        "legacy-unknown": "UNKNOWN",
    }

    # No columns are missing on a second startup, but a PENDING row introduced
    # by a partially completed/manual upgrade still must be made safe.
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE pm_order_intents SET state = 'PENDING' WHERE id = 1")
        )
    migrate_pm_tables(engine)
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT state FROM pm_order_intents WHERE id = 1")
        ).scalar_one() == "PREPARED"


def test_pm_intent_migration_emits_postgresql_compatible_sql(mocker) -> None:
    """The generated ALTER statements use PostgreSQL's TIMESTAMP spelling."""

    class FakeResult:
        rowcount = 1

    class FakeConnection:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, statement):
            self.statements.append(str(statement))
            return FakeResult()

    class FakeBegin:
        def __init__(self, connection: FakeConnection) -> None:
            self.connection = connection

        def __enter__(self) -> FakeConnection:
            return self.connection

        def __exit__(self, exc_type, exc_value, traceback) -> None:
            return None

    class FakePostgresEngine:
        name = "postgresql"

        def __init__(self) -> None:
            self.connection = FakeConnection()

        def begin(self) -> FakeBegin:
            return FakeBegin(self.connection)

    class FakeInspector:
        @staticmethod
        def get_table_names() -> list[str]:
            return ["pm_order_intents"]

        @staticmethod
        def get_columns(table_name: str) -> list[dict[str, str]]:
            assert table_name == "pm_order_intents"
            return [{"name": "id"}, {"name": "client_id"}, {"name": "state"}]

    engine = FakePostgresEngine()
    mocker.patch.object(migrations, "inspect", return_value=FakeInspector())

    migrate_pm_tables(engine)  # type: ignore[arg-type]

    statements = "\n".join(engine.connection.statements).upper()
    assert 'ADD COLUMN "ACKED_AT" TIMESTAMP' in statements
    assert 'ADD COLUMN "LINKED_AT" TIMESTAMP' in statements
    assert "DATETIME" not in statements
    assert "SET STATE = 'PREPARED'" in statements
    assert "UPPER(STATE) = 'PENDING'" in statements


def test_legacy_pending_is_fail_closed_until_startup_migration_runs() -> None:
    """A failed/interrupted migration must never make an old intent invisible."""
    assert "PENDING" in UNRESOLVED_STATES

def test_migration_marks_historical_acked_unknown_as_may_have_been_sent() -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE pm_order_intents ("
            "id INTEGER PRIMARY KEY, client_id VARCHAR(40) NOT NULL, state VARCHAR(16) NOT NULL, "
            "exchange_order_id VARCHAR(64), raw_response TEXT, acked_at TIMESTAMP)"
        ))
        connection.execute(text(
            "CREATE TABLE pm_outbox ("
            "id INTEGER PRIMARY KEY, client_id VARCHAR(40) NOT NULL, state VARCHAR(16) NOT NULL, "
            "exchange_order_id VARCHAR(64), raw_response TEXT, dispatch_attempts INTEGER NOT NULL, "
            "created_at TIMESTAMP NOT NULL, processed_at TIMESTAMP)"
        ))
        connection.execute(text(
            "INSERT INTO pm_order_intents "
            "(id, client_id, state, exchange_order_id, raw_response, acked_at) VALUES "
            "(1, 'acked-zero', 'ACKED', '123', '{}', CURRENT_TIMESTAMP), "
            "(2, 'unknown-zero', 'UNKNOWN', NULL, NULL, NULL), "
            "(3, 'pristine', 'PREPARED', NULL, NULL, NULL)"
        ))
        connection.execute(text(
            "INSERT INTO pm_outbox "
            "(id, client_id, state, exchange_order_id, raw_response, dispatch_attempts, created_at) VALUES "
            "(1, 'acked-zero', 'PENDING', NULL, NULL, 0, CURRENT_TIMESTAMP), "
            "(2, 'unknown-zero', 'PENDING', NULL, NULL, 0, CURRENT_TIMESTAMP), "
            "(3, 'pristine', 'PENDING', NULL, NULL, 0, CURRENT_TIMESTAMP)"
        ))

    migrate_pm_tables(engine)

    with engine.connect() as connection:
        rows = dict(connection.execute(text(
            "SELECT client_id, dispatch_started_at FROM pm_outbox ORDER BY id"
        )).all())
    assert rows["acked-zero"] is not None
    assert rows["unknown-zero"] is not None
    assert rows["pristine"] is None
