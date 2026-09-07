"""STOPPED recovery must never relay exposure-increasing PENDING outbox rows."""

from unittest.mock import MagicMock

import pytest

from freqtrade.persistence import PMOrderIntent, PMOutbox, init_db
from tests.exchange.test_pm_outbox_flow import make_exchange


@pytest.fixture(autouse=True)
def pm_db(default_conf_usdt):
    init_db(default_conf_usdt["db_url"])


def test_stopped_outbox_relay_suppresses_pending_entry_without_mutating_it(
    mocker, default_conf_usdt
):
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._pm_enqueue(
        "ft-stopped-entry",
        {
            "kind": "order",
            "pair": "ETH/USDT:USDT",
            "side": "buy",
            "type": "market",
            "amount": 1.0,
            "reduce_only": False,
        },
        payload={"symbol": "ETHUSDT", "side": "BUY", "quantity": "1"},
    )
    exchange._pm_dispatch_order = MagicMock()

    report = exchange.pm_drain_outbox(allow_exposure_increasing=False)

    assert report["suppressed_exposure_increasing"] == 1
    assert report["drained"] == 0
    exchange._pm_dispatch_order.assert_not_called()
    intent = PMOrderIntent.get_by_client_id("ft-stopped-entry")
    outbox = PMOutbox.get_by_client_id("ft-stopped-entry")
    assert intent is not None and intent.state == "PREPARED"
    assert outbox is not None and outbox.state == "PENDING"
    assert outbox.dispatch_attempts == 0


def test_stopped_outbox_scope_still_allows_reduce_only_recovery(mocker, default_conf_usdt):
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._pm_enqueue(
        "ft-stopped-exit",
        {
            "kind": "order",
            "pair": "ETH/USDT:USDT",
            "side": "sell",
            "type": "market",
            "amount": 1.0,
            "reduce_only": True,
            "origin_trade_id": 7,
        },
        payload={"symbol": "ETHUSDT", "side": "SELL", "quantity": "1", "reduceOnly": True},
    )
    exchange._pm_dispatch_order = MagicMock(return_value={"id": "exit-1"})

    report = exchange.pm_drain_outbox(allow_exposure_increasing=False)

    assert report["suppressed_exposure_increasing"] == 0
    assert report["drained"] == 1
    assert report["acked"] == 1
    exchange._pm_dispatch_order.assert_called_once()


def test_origin_trade_id_is_durable_before_post(mocker, default_conf_usdt):
    exchange = make_exchange(mocker, default_conf_usdt)
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda order: order)
    from tests.exchange.test_pm_outbox_flow import PM_ORDER
    exchange._papi_request = MagicMock(return_value=PM_ORDER)

    exchange._pm_place_order(
        "ETH/USDT:USDT",
        "limit",
        "sell",
        1,
        100,
        {"reduceOnly": True},
        log_tag="papi_create_order",
        origin_trade_id=77,
    )

    intent = PMOrderIntent.get_unresolved()[0]
    outbox = PMOutbox.get_by_client_id(intent.client_id)
    assert intent.origin_trade_id == 77
    assert outbox is not None and outbox.origin_trade_id == 77
