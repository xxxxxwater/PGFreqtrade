"""
Tests for the durable PM order intent store (pm_order_intents table).

Covers automatic schema creation on old databases, data preservation, the
unique client_id constraint, backup/restore round-trips and the fail-closed
behavior when the store is unavailable.
"""

from unittest.mock import MagicMock

import pytest
from sqlalchemy import inspect, text

from freqtrade.exceptions import OperationalException, TemporaryError
from freqtrade.exchange.binance import Binance
from freqtrade.persistence import Order, PMOrderIntent, Trade, init_db


def intent_count() -> int:
    return len(PMOrderIntent.session.query(PMOrderIntent).all())


def test_init_db_creates_pm_order_intents_table(default_conf):
    init_db(default_conf["db_url"])
    assert (
        PMOrderIntent.__tablename__
        in inspect(PMOrderIntent.session.get_bind()).get_table_names()
    )


@pytest.mark.usefixtures("init_persistence")
def test_pm_order_intent_crud_and_state_lifecycle():
    row = PMOrderIntent(
        client_id="ft123",
        kind="order",
        pair="BTC/USDT:USDT",
        side="buy",
        order_type="limit",
        amount=0.001,
        reduce_only=False,
        state="PENDING",
    )
    PMOrderIntent.session.add(row)
    PMOrderIntent.session.commit()

    assert intent_count() == 1
    assert PMOrderIntent.has_unresolved() is True
    loaded = PMOrderIntent.get_by_client_id("ft123")
    assert loaded is not None and loaded.is_unresolved is True
    assert loaded.to_dict()["state"] == "PENDING"

    # UNKNOWN also counts as unresolved.
    loaded.state = "UNKNOWN"
    loaded.last_error = "timeout"
    PMOrderIntent.session.commit()
    assert PMOrderIntent.has_unresolved() is True
    assert PMOrderIntent.get_unresolved()[0].last_error == "timeout"

    # Clearing removes the row.
    PMOrderIntent.session.delete(loaded)
    PMOrderIntent.session.commit()
    assert PMOrderIntent.has_unresolved() is False


@pytest.mark.usefixtures("init_persistence")
def test_pm_order_intent_unique_client_id():
    PMOrderIntent.session.add(
        PMOrderIntent(
            client_id="ftuniq",
            kind="order",
            pair="BTC/USDT:USDT",
            reduce_only=False,
            state="PENDING",
        )
    )
    PMOrderIntent.session.commit()

    from sqlalchemy.exc import IntegrityError

    PMOrderIntent.session.add(
        PMOrderIntent(
            client_id="ftuniq",
            kind="order",
            pair="ETH/USDT:USDT",
            reduce_only=False,
            state="PENDING",
        )
    )
    with pytest.raises(IntegrityError):
        PMOrderIntent.session.commit()
    PMOrderIntent.session.rollback()


def test_old_sqlite_database_upgrade_creates_table_and_preserves_trades(
    default_conf, fee, tmp_path
):
    """
    Simulate an OLD database (no pm_order_intents table): restarting the bot on it
    must create the new table automatically while existing trades/orders survive.
    """
    db_file = tmp_path / "old_freqtrade.sqlite"
    db_url = f"sqlite:///{db_file}"

    # 1. Create the database WITHOUT the pm_order_intents table.
    init_db(db_url)
    engine = PMOrderIntent.session.get_bind()
    tables_before = inspect(engine).get_table_names()
    assert "pm_order_intents" in tables_before
    # Drop it to simulate the pre-upgrade schema.
    PMOrderIntent.session.execute(text("DROP TABLE pm_order_intents"))
    PMOrderIntent.session.commit()
    assert "pm_order_intents" not in inspect(engine).get_table_names()

    # 2. Insert legacy data (trade + order).
    trade = Trade(
        pair="ADA/USDT",
        stake_amount=60.0,
        open_rate=2.0,
        amount=30.0,
        is_open=True,
        exchange="binance",
        fee_open=fee.return_value,
        fee_close=fee.return_value,
    )
    Trade.session.add(trade)
    Trade.session.flush()
    order = Order(
        ft_order_side="buy",
        ft_pair=trade.pair,
        ft_is_open=True,
        ft_amount=trade.amount,
        ft_price=trade.open_rate,
        order_id="legacy1",
        status="open",
        symbol=trade.pair,
        order_type="market",
        side="buy",
        price=trade.open_rate,
        average=trade.open_rate,
        filled=0,
        remaining=trade.amount,
        cost=0,
        order_date=trade.open_date,
        ft_trade_id=trade.id,
    )
    Order.session.add(order)
    Order.session.commit()
    Trade.session.remove()

    # 3. "Restart" on the old database.
    init_db(db_url)
    new_tables = inspect(PMOrderIntent.session.get_bind()).get_table_names()
    assert "pm_order_intents" in new_tables, "upgrade must auto-create the intent table"

    trades = Trade.session.query(Trade).all()
    orders = Order.session.query(Order).all()
    assert len(trades) == 1 and len(orders) == 1
    assert trades[0].pair == "ADA/USDT"
    assert orders[0].order_id == "legacy1"


def test_backup_includes_pm_order_intents_and_restores(default_conf, tmp_path):
    """
    The SQLite online-backup script copies the whole database, so pm_order_intents
    is included; after restore the intent state must be readable.
    """
    from scripts.backup_db import backup_sqlite

    db_file = tmp_path / "live.sqlite"
    init_db(f"sqlite:///{db_file}")

    PMOrderIntent.session.add(
        PMOrderIntent(
            client_id="st-backup",
            kind="conditional",
            pair="BTC/USDT:USDT",
            stop_price=60000,
            reduce_only=True,
            state="UNKNOWN",
            last_error="timeout",
        )
    )
    PMOrderIntent.session.commit()

    backup_dir = tmp_path / "backups"
    target = backup_sqlite(db_file, backup_dir, keep=3)
    assert target.exists()

    # Restore: point the session at the backup file and verify the intent is there.
    init_db(f"sqlite:///{target}")
    restored = PMOrderIntent.get_by_client_id("st-backup")
    assert restored is not None
    assert restored.state == "UNKNOWN"
    assert restored.last_error == "timeout"
    assert PMOrderIntent.has_unresolved() is True


def test_intent_put_failure_blocks_post():
    """
    A failing durable intent write must refuse the POST (fail-closed): the caller
    raises before `_papi_request` is ever invoked.
    """
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    from threading import RLock

    exchange._pm_user_stream_lock = RLock()
    exchange._exchange_ws = None
    exchange._api_async = None
    exchange._ws_async = None
    exchange.loop = None
    exchange._config = {"dry_run": False, "exchange": {}}
    exchange._pm_new_client_order_id = MagicMock(return_value="ft123")
    exchange._pm_order_params = MagicMock(return_value={"symbol": "BTCUSDT"})
    exchange._papi_request = MagicMock()
    exchange._pm_intent_put = MagicMock(
        side_effect=OperationalException("could not write intent store")
    )

    with pytest.raises(OperationalException, match="could not write intent store"):
        exchange._pm_place_order(
            "BTC/USDT:USDT",
            "limit",
            "buy",
            0.001,
            60000,
            {},
            log_tag="test",
        )

    exchange._papi_request.assert_not_called()


def test_unreadable_intent_store_blocks_new_orders():
    """
    When the intent store cannot be read, exposure-increasing orders must be
    refused (TemporaryError) and the PAPI POST must never happen.
    """
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    from threading import RLock

    exchange._pm_user_stream_lock = RLock()
    exchange._exchange_ws = None
    exchange._api_async = None
    exchange._ws_async = None
    exchange.loop = None
    exchange._config = {"dry_run": False, "exchange": {"portfolio_margin_risk": {}}}
    exchange._get_params = MagicMock(return_value={})
    exchange.pm_has_unresolved_intents = MagicMock(return_value=True)
    exchange._papi_request = MagicMock()

    with pytest.raises(TemporaryError, match="unresolved order intents"):
        exchange.create_order(
            pair="BTC/USDT:USDT",
            ordertype="market",
            side="buy",
            amount=0.001,
            rate=60000,
            leverage=5,
        )
    exchange._papi_request.assert_not_called()


def _make_live_validate_exchange(mocker):
    from unittest.mock import PropertyMock

    mocker.patch("freqtrade.exchange.exchange.Exchange.validate_config", MagicMock())
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = MagicMock()
    exchange._exchange_ws = None
    exchange._api_async = None
    exchange._ws_async = None
    exchange.loop = None
    exchange._validate_pm_risk_config = MagicMock()
    exchange.trading_mode = "futures"
    exchange.margin_mode = "cross"
    mocker.patch.object(
        type(exchange), "markets", PropertyMock(return_value={}), create=True
    )
    exchange._api = MagicMock(apiKey="k", secret="s")
    return exchange


def test_live_pm_without_db_url_refuses_to_start(mocker):
    """Live PM without a persistent database must fail startup (fail-closed)."""
    exchange = _make_live_validate_exchange(mocker)
    config = {
        "dry_run": False,
        "stake_currency": "USDT",
        "exchange": {
            "key": "k",
            "secret": "s",
            "portfolio_margin_risk": {"min_uni_mmr": 1.5},
            "pair_whitelist": [],
        },
    }

    with pytest.raises(OperationalException, match="persistent database"):
        exchange.validate_config(config)


def test_live_pm_with_db_url_passes_credential_gate(mocker):
    """With db_url present the credential gate must not reject (other checks may run)."""
    exchange = _make_live_validate_exchange(mocker)
    config = {
        "dry_run": False,
        "stake_currency": "USDT",
        "db_url": "sqlite:///trades.sqlite",
        "exchange": {
            "key": "k",
            "secret": "s",
            "portfolio_margin_risk": {"min_uni_mmr": 1.5},
            "pair_whitelist": [],
        },
    }

    # Must not raise the persistent-database error.
    try:
        exchange.validate_config(config)
    except OperationalException as e:
        assert "persistent database" not in str(e)


# ---------------------------------------------------------------------------
# Readonly / corrupted database fail-closed behavior
# ---------------------------------------------------------------------------


def test_readonly_db_blocks_intent_write(tmp_path):
    """
    A read-only SQLite database must make intent writes fail, so the order POST
    is refused (fail-closed). Verified with a real read-only SQLite connection.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    db_file = tmp_path / "readonly.sqlite"
    engine = create_engine(f"sqlite:///{db_file}")
    PMOrderIntent.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        session.add(
            PMOrderIntent(
                client_id="ft-ro",
                kind="order",
                pair="BTC/USDT:USDT",
                reduce_only=False,
                state="PENDING",
            )
        )
        session.commit()

    # Open the SAME database read-only.
    ro_engine = create_engine(f"sqlite:///file:{db_file}?mode=ro", future=True)
    ro_session = sessionmaker(bind=ro_engine)()
    try:
        ro_session.add(
            PMOrderIntent(
                client_id="ft-ro-2",
                kind="order",
                pair="BTC/USDT:USDT",
                reduce_only=False,
                state="PENDING",
            )
        )
        from sqlalchemy.exc import DatabaseError

        with pytest.raises(DatabaseError):
            ro_session.commit()
    finally:
        ro_session.close()


def test_corrupted_db_read_fails_closed(tmp_path):
    """
    A corrupted (non-SQLite) database file must surface read errors, which the
    intent gate treats as unresolved (block) - never as "no intents".
    """
    from sqlalchemy import create_engine
    from sqlalchemy.exc import DatabaseError
    from sqlalchemy.orm import sessionmaker

    db_file = tmp_path / "corrupted.sqlite"
    db_file.write_text("this is not a sqlite database at all" * 10)

    engine = create_engine(f"sqlite:///{db_file}")
    PMOrderIntent.session = sessionmaker(bind=engine)()

    with pytest.raises(DatabaseError):
        PMOrderIntent.has_unresolved()

    # The exchange-level gate must translate store errors into "unresolved" (block).
    from threading import RLock

    exchange = Binance.__new__(Binance)
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    exchange._exchange_ws = None
    exchange._api_async = None
    exchange._ws_async = None
    exchange.loop = None
    assert exchange.pm_has_unresolved_intents() is True


# ---------------------------------------------------------------------------
# PostgreSQL DDL / optional integration
# ---------------------------------------------------------------------------


def test_postgresql_ddl_has_unique_client_id():
    """
    The PostgreSQL DDL for pm_order_intents must enforce a UNIQUE client_id.

    SQLAlchemy renders ``unique=True + index=True`` as a standalone
    ``CREATE UNIQUE INDEX`` on client_id - verified without a server.
    """
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateIndex, CreateTable

    ddl = str(
        CreateTable(PMOrderIntent.__table__).compile(dialect=postgresql.dialect())
    ).upper()
    assert "PM_ORDER_INTENTS" in ddl
    assert "CLIENT_ID" in ddl
    assert "REDUCE_ONLY" in ddl

    index_ddls = [
        str(CreateIndex(idx).compile(dialect=postgresql.dialect())).upper()
        for idx in PMOrderIntent.__table__.indexes
    ]
    assert any(
        "UNIQUE" in ddl_stmt and "CLIENT_ID" in ddl_stmt for ddl_stmt in index_ddls
    ), "PostgreSQL must enforce the unique client_id constraint via a UNIQUE index"


@pytest.mark.skipif(
    not __import__("os").environ.get("FREQTRADE_TEST_PG_URL"),
    reason="Set FREQTRADE_TEST_PG_URL to a scratch PostgreSQL URL to run this integration test",
)
def test_postgresql_init_and_unique_constraint(tmp_path):
    """
    Real PostgreSQL integration (opt-in): init_db creates pm_order_intents, the
    unique client_id constraint is enforced by the server, and data survives a
    second init_db on the same URL.
    """
    import os

    pg_url = os.environ["FREQTRADE_TEST_PG_URL"]
    pytest.importorskip("psycopg2")

    init_db(pg_url)
    names = inspect(PMOrderIntent.session.get_bind()).get_table_names()
    assert "pm_order_intents" in names

    PMOrderIntent.session.add(
        PMOrderIntent(
            client_id=f"ft-pg-{__import__('uuid').uuid4().hex[:8]}",
            kind="order",
            pair="BTC/USDT:USDT",
            reduce_only=False,
            state="PENDING",
        )
    )
    PMOrderIntent.session.commit()

    from sqlalchemy.exc import IntegrityError

    row = PMOrderIntent.get_unresolved()[0]
    PMOrderIntent.session.add(
        PMOrderIntent(
            client_id=row.client_id,  # duplicate
            kind="order",
            pair="ETH/USDT:USDT",
            reduce_only=False,
            state="PENDING",
        )
    )
    with pytest.raises(IntegrityError):
        PMOrderIntent.session.commit()
    PMOrderIntent.session.rollback()

    # Second init on the same URL keeps data intact.
    init_db(pg_url)
    assert len(PMOrderIntent.get_unresolved()) == 1
