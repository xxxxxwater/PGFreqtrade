"""Read failures yield to protection, never to duplicate order submission."""
from unittest.mock import MagicMock

import ccxt
import pytest

from freqtrade.enums import State
from freqtrade.exceptions import InvalidOrderException, OperationalException, TemporaryError
from freqtrade.persistence import Trade
from tests.exchange.test_binance_pm import PM_ALGO_ORDER, get_patched_pm_exchange
from tests.exchange.test_binance_pm import pm_intent_db as _pm_intent_db
from tests.freqtradebot.test_pm_loop_continuity import market_bot
from tests.freqtradebot.test_pm_protection_correctness import conditional
from tests.freqtradebot.test_pm_recovery import make_pm_bot, make_stoploss_trade
from tests.freqtradebot.test_pm_unattended_gap_closure import pm_conf as _pm_conf
from tests.freqtradebot.test_pm_unattended_gap_closure import pm_db as _pm_db


pm_intent_db = _pm_intent_db
pm_conf = _pm_conf
pm_db = _pm_db


@pytest.mark.parametrize("error", [ccxt.NetworkError, ccxt.RequestTimeout,
                                  ccxt.ExchangeNotAvailable])
def test_symbol_config_disconnect_is_read_retry_not_fatal(mocker, default_conf_usdt, error):
    ex = get_patched_pm_exchange(mocker, default_conf_usdt)
    ex._api.request = MagicMock(side_effect=error("signed-url?signature=SECRET"))
    with pytest.raises(TemporaryError) as caught:
        ex.get_pm_tradable_pairs()
    assert "SECRET" not in str(caught.value)
    assert ex._pm_um_symbol_config_cache.get("pairs") is None
    ex._api.request.assert_called_once()
    assert ex._api.request.call_args.args[2] == "GET"
    ex._api.request.side_effect = None
    ex._api.request.return_value = [
        {"symbol": "ETHUSDT", "marginType": "CROSSED", "maxNotionalValue": "100000"}
    ]
    assert ex.get_pm_tradable_pairs() == {"ETH/USDT:USDT"}


@pytest.mark.parametrize("error", [TemporaryError, ccxt.NetworkError])
def test_market_retry_is_bounded_and_clears_only_after_complete_batch(mocker, pm_conf, error):
    bot = market_bot(mocker, pm_conf)
    bot.state = State.PAUSED
    bot._pm_block_orders("stop_retire_unresolved")
    clock = mocker.patch("freqtrade.freqtradebot._time.monotonic", return_value=100)
    bot._refresh_active_whitelist.side_effect = error("disconnected")
    bot._pm_refresh_market_with_budget(12)
    assert bot._pm_cycle_exposure_block_reason == "market_refresh_failed"
    assert bot._pm_market_retry_after == 103
    bot._pm_refresh_market_with_budget(12)
    assert bot._pm_cycle_exposure_block_reason == "market_refresh_retry_pending"
    assert bot._refresh_active_whitelist.call_count == 1
    clock.return_value = 104
    bot._refresh_active_whitelist.side_effect = None
    bot._pm_refresh_market_with_budget(12)
    assert bot._pm_cycle_exposure_block_reason is None
    assert bot._pm_market_read_failures == 0
    assert "stop_retire_unresolved" in bot._pm_orders_blocked_reasons
    assert bot.state == State.PAUSED


@pytest.mark.parametrize("error", [OperationalException, ValueError])
def test_market_does_not_hide_permanent_or_programming_errors(mocker, pm_conf, error):
    bot = market_bot(mocker, pm_conf)
    bot._refresh_active_whitelist.side_effect = error("bad configuration")
    with pytest.raises(error):
        bot._pm_refresh_market_with_budget(12)
    assert bot._pm_cycle_exposure_block_reason == "market_refresh_failed"


@pytest.mark.parametrize("budget", [0, 12])
def test_disconnect_process_still_services_orders_exits_and_holds_increases(
    mocker, pm_conf, budget
):
    bot = market_bot(mocker, pm_conf)
    bot.config["exchange"]["portfolio_margin_risk"]["market_analysis_budget_seconds"] = budget
    bot._refresh_active_whitelist.side_effect = ccxt.NetworkError("disconnected")
    events = []
    bot._pm_consume_user_stream_events = MagicMock()
    bot._pm_maintenance_schedule.run_pending = MagicMock()
    bot._schedule.run_pending = MagicMock()
    bot._pm_service_protection_before_market = MagicMock(
        side_effect=lambda: events.append("protect")
    )
    bot.manage_open_orders = MagicMock(side_effect=lambda: events.append("orders"))
    bot.exit_positions = MagicMock(side_effect=lambda _: events.append("exit"))
    bot.process_open_trade_positions = MagicMock()
    bot.get_free_open_trades = MagicMock(return_value=False)
    bot._pm_record_entry_block_for_pairs = MagicMock()
    bot.rpc.process_msg_queue = MagicMock()
    bot.process()
    assert events == ["protect", "protect", "orders", "exit"]
    assert bot._pm_cycle_exposure_block_reason == "market_refresh_failed"
    bot.strategy.analyze.assert_not_called()


@pytest.mark.parametrize("lookup", ["55", "stabc"])
def test_algo_history_matches_both_exchange_and_client_ids(mocker, default_conf_usdt, lookup):
    ex = get_patched_pm_exchange(mocker, default_conf_usdt)
    ex._papi_request = MagicMock(side_effect=[
        ccxt.OrderNotFound("-2013 Order does not exist"),
        [dict(PM_ALGO_ORDER, algoStatus="CANCELED")],
    ])
    row = ex.fetch_stoploss_order(lookup, "ETH/USDT:USDT")
    assert row["status"] == "canceled"
    assert row["id"] == lookup
    assert ex._papi_request.call_count == 2


@pytest.mark.parametrize("history", [None, {}, {"data": None}, {"orders": {}}, [None]])
def test_malformed_algo_history_is_not_absence(mocker, default_conf_usdt, history):
    ex = get_patched_pm_exchange(mocker, default_conf_usdt)
    ex._papi_request = MagicMock(side_effect=[
        ccxt.OrderNotFound("-2013 Order does not exist"), history,
    ])
    with pytest.raises(TemporaryError, match="shape"):
        ex.fetch_stoploss_order("stabc", "ETH/USDT:USDT")


@pytest.mark.parametrize("exc", [InvalidOrderException("absent"), TemporaryError("offline")])
def test_unverified_durable_candidate_prevents_another_post_after_reload(mocker, pm_conf, exc):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="st-candidate", side="sell")
    Trade.commit()
    tid = trade.id
    Trade.session.expunge_all()
    trade = Trade.session.get(Trade, tid)
    bot.exchange.fetch_stoploss_order = MagicMock(side_effect=exc)
    bot.create_stoploss_order = MagicMock()
    bot._pm_retire_stop_ids = MagicMock()
    for _ in range(3):
        assert bot._pm_replace_stop_protection(trade, ["st-candidate"]) == "kept_old"
    bot.create_stoploss_order.assert_not_called()
    bot._pm_retire_stop_ids.assert_not_called()
    assert trade.open_sl_orders[0].status == "open"
    assert "stop_protection_missing" in bot._pm_blocked_order_reasons()


def test_known_undersized_old_stop_does_not_prevent_dca_resize(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stold", side="sell")
    bot.exchange.fetch_stoploss_order = MagicMock(
        return_value=conditional("stold", amount=1, info={"reduceOnly": True})
    )
    assert bot._pm_existing_stop_lifecycles_known(trade) is True


@pytest.mark.parametrize("history", [False, True])
def test_triggered_without_visible_child_remains_pending(mocker, default_conf_usdt, history):
    ex = get_patched_pm_exchange(mocker, default_conf_usdt)
    raw = dict(PM_ALGO_ORDER, algoStatus="TRIGGERED")
    if history:
        ex._papi_request = MagicMock(side_effect=[
            ccxt.OrderNotFound("-2013 Order does not exist"), [raw],
        ])
    else:
        ex._papi_request = MagicMock(return_value=raw)
    row = ex.fetch_stoploss_order("stabc", "ETH/USDT:USDT")
    assert row["status_stop"] == "triggered"
    assert row["filled"] == 0
    assert row["id"] == "stabc"
