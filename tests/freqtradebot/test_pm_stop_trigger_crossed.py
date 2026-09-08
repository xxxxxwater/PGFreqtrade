"""Explicit immediate-trigger rejection never masquerades as a fill or a retry."""
from unittest.mock import MagicMock

import ccxt
import pytest

from freqtrade.exceptions import InvalidOrderException, StopWouldImmediatelyTrigger, TemporaryError
from tests.exchange.test_binance_pm import get_patched_pm_exchange
from tests.freqtradebot.test_pm_protection_correctness import switch_bot
from tests.freqtradebot.test_pm_recovery import pm_conf as _pm_conf

pm_conf = _pm_conf


@pytest.fixture(autouse=True)
def intent_database(default_conf_usdt):
    from freqtrade.persistence import init_db

    init_db(default_conf_usdt["db_url"])


@pytest.mark.parametrize("side", ["buy", "sell"])
@pytest.mark.parametrize("price_type,working", [("last", "CONTRACT_PRICE"), ("mark", "MARK_PRICE")])
def test_explicit_trigger_code_preserves_request_evidence(
    mocker, default_conf_usdt, side, price_type, working
):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._lev_prep = MagicMock()
    exchange._papi_request = MagicMock(
        side_effect=ccxt.InvalidOrder('binance {"code":-2021,"msg":"Order would immediately trigger."}')
    )
    exchange.amount_to_precision = MagicMock(return_value=1)
    exchange.price_to_precision = MagicMock(return_value=95)
    with pytest.raises(StopWouldImmediatelyTrigger, match=f"working_type={working}"):
        exchange.create_stoploss(
            pair="ETH/USDT:USDT", amount=1, stop_price=95, side=side, leverage=2,
            order_types={"stoploss": "market", "stoploss_price_type": price_type},
        )
    exchange._papi_request.assert_called_once()
    assert exchange._papi_request.call_args.args[2]["triggerPrice"] == 95
    assert exchange._papi_request.call_args.args[2]["reduceOnly"] == "true"
    from freqtrade.persistence import PMOutbox

    client_id = exchange._papi_request.call_args.args[2]["clientAlgoId"]
    row = PMOutbox.get_by_client_id(client_id)
    assert row.state == "REJECTED"
    assert row.dispatch_started_at is not None


@pytest.mark.parametrize("message", [
    'binance {"code":-2010,"msg":"rejected"}',
    'binance {"code":-20210,"msg":"other error"}',
    "Order would immediately trigger.",
])
def test_other_invalid_orders_not_reclassified(mocker, default_conf_usdt, message):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._lev_prep = MagicMock()
    exchange._papi_request = MagicMock(side_effect=ccxt.InvalidOrder(message))
    with pytest.raises(InvalidOrderException) as exc:
        exchange.create_stoploss(
            pair="ETH/USDT:USDT", amount=1, stop_price=95, side="sell", leverage=2,
            order_types={"stoploss": "market"},
        )
    assert not isinstance(exc.value, StopWouldImmediatelyTrigger)


def test_crossed_stop_keeps_protection_and_uses_existing_exit(mocker, pm_conf, caplog):
    bot, trade = switch_bot(mocker, pm_conf)
    bot.exchange.create_stoploss.side_effect = StopWouldImmediatelyTrigger("explicit -2021")
    bot.emergency_exit = MagicMock()
    assert bot.create_stoploss_order(trade, 0.008237) is False
    bot.emergency_exit.assert_called_once_with(trade, 0.008237)
    bot.exchange.create_stoploss.assert_called_once()
    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()
    assert {o.order_id for o in trade.open_sl_orders} == {"stold"}
    assert trade.is_open
    assert "PM stop trigger crossed" in caplog.text
    assert "Unable to place a stoploss order on exchange" not in caplog.text


def test_uncertain_stop_is_not_reclassified_as_crossed(mocker, pm_conf):
    bot, trade = switch_bot(mocker, pm_conf)
    bot.exchange.create_stoploss.side_effect = TemporaryError("lookup timeout -2021")
    bot.emergency_exit = MagicMock()
    assert bot.create_stoploss_order(trade, 0.008237) is False
    bot.emergency_exit.assert_not_called()
    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()
