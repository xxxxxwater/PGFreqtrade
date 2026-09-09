"""Targeted regressions for PM unattended-safety gap closure."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

from freqtrade.exceptions import InvalidOrderException
from freqtrade.persistence import Order, PMOrderIntent, Trade, init_db
from tests.freqtradebot.test_pm_protection_correctness import make_stoploss_trade
from tests.freqtradebot.test_pm_recovery import make_open_trade, make_pm_bot


@pytest.fixture(autouse=True)
def pm_db(pm_conf):
    init_db(pm_conf["db_url"])


@pytest.fixture
def pm_conf(default_conf_usdt):
    conf = default_conf_usdt.copy()
    conf["dry_run"] = False
    conf["trading_mode"] = "futures"
    conf["margin_mode"] = "cross"
    conf["stake_currency"] = "USDT"
    conf["exchange"] = conf["exchange"].copy()
    conf["exchange"]["name"] = "binance"
    conf["exchange"]["key"] = "dummy_key"
    conf["exchange"]["secret"] = "dummy_secret"
    conf["exchange"]["pair_whitelist"] = ["ETH/USDT:USDT"]
    conf["exchange"]["portfolio_margin"] = True
    conf["exchange"]["portfolio_margin_risk"] = {
        "min_uni_mmr": 1.5,
        "user_stream_enabled": False,
    }
    return conf


def test_pm_legacy_cancel_not_found_never_fabricates_canceled(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="st-old", side="sell")
    old = trade.open_sl_orders[0]
    bot.exchange.cancel_stoploss_order_with_result = MagicMock(
        side_effect=InvalidOrderException("not found")
    )
    bot.exchange.fetch_stoploss_order = MagicMock(
        side_effect=InvalidOrderException("no lifecycle proof")
    )

    bot.cancel_stoploss_on_exchange(trade)

    assert old.ft_is_open is True
    assert old.status not in {"canceled", "cancelled"}
    assert "stop_retire_unresolved" in bot._pm_blocked_order_reasons()


def test_dca_data_stale_blocks_create_order(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="entry", side="buy", amount=11.0)
    # Mark the synthetic entry terminal so execute_entry is the only open-order path.
    trade.orders[-1].ft_is_open = False
    trade.orders[-1].status = "closed"
    Trade.commit()
    bot._pm_blocked_order_reasons = MagicMock(return_value=[])
    bot._pm_pair_entry_block_reasons = MagicMock(return_value=[])
    bot.get_valid_enter_price_and_stake = MagicMock(return_value=(0.01, 10.0, 1.0))
    bot._pm_exposure_data_block_reason = MagicMock(
        return_value="market_data_stale: candle_age=900s"
    )
    bot._pm_record_signal_decision = MagicMock(return_value=True)
    bot.exchange.create_order = MagicMock()

    assert bot.execute_entry(
        trade.pair,
        10.0,
        price=0.01,
        trade=trade,
        mode="pos_adjust",
        is_short=False,
    ) is False

    bot.exchange.create_order.assert_not_called()
    assert bot._pm_record_signal_decision.call_args.kwargs["decision_scope"] == "dca"
    assert bot._pm_record_signal_decision.call_args.kwargs["decision"] == "blocked_data"


def test_reduction_path_is_not_blocked_by_exposure_data_gate(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="entry", side="buy", amount=11.0)
    trade.orders[-1].ft_is_open = False
    trade.orders[-1].status = "closed"
    Trade.commit()
    bot.exchange.get_rates = MagicMock(return_value=(0.01, 0.01))
    bot.exchange.get_min_pair_stake_amount = MagicMock(return_value=0.001)
    bot.exchange.get_max_pair_stake_amount = MagicMock(return_value=1000.0)
    bot.wallets.get_available_stake_amount = MagicMock(return_value=1000.0)
    bot.strategy._adjust_trade_position_internal = MagicMock(return_value=(-0.02, "risk_reduce"))
    bot._pm_exposure_data_block_reason = MagicMock(return_value="market_data_stale")
    bot.execute_trade_exit = MagicMock(return_value=True)

    bot.check_and_call_adjust_trade_position(trade)

    bot.execute_trade_exit.assert_called_once()
    bot._pm_exposure_data_block_reason.assert_not_called()


def test_open_partial_dca_fill_recalculates_and_resizes_stop(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="st-old", side="sell", amount=11.0)
    partial = Order(
        ft_order_side=trade.entry_side,
        ft_pair=trade.pair,
        ft_is_open=True,
        ft_amount=5.0,
        ft_price=0.01,
        order_id="dca-partial",
        status="open",
        symbol=trade.pair,
        order_type="limit",
        side=trade.entry_side,
        price=0.01,
        average=0.01,
        filled=5.0,
        remaining=0.0,
        cost=0.05,
        order_date=trade.open_date,
    )
    trade.orders.append(partial)
    Trade.commit()
    bot._pm_resize_stop_protection = MagicMock(return_value="active")

    bot._pm_apply_partial_entry_fill(trade, partial)

    assert trade.amount == pytest.approx(16.0)
    assert partial.ft_is_open is True
    bot._pm_resize_stop_protection.assert_called_once_with(trade)


def test_partial_fill_restart_repairs_undersized_stop_even_without_new_fill_delta(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="st-old", side="sell", amount=11.0)
    partial = Order(
        ft_order_side=trade.entry_side,
        ft_pair=trade.pair,
        ft_is_open=True,
        ft_amount=5.0,
        ft_price=0.01,
        order_id="dca-partial",
        status="open",
        symbol=trade.pair,
        order_type="limit",
        side=trade.entry_side,
        price=0.01,
        average=0.01,
        filled=5.0,
        remaining=0.0,
        cost=0.05,
        order_date=trade.open_date,
    )
    trade.orders.append(partial)
    # Model a crash after local exposure was persisted but before stop resize.
    trade.amount = 16.0
    Trade.commit()
    bot._pm_resize_stop_protection = MagicMock(return_value="active")

    bot._pm_apply_partial_entry_fill(trade, partial)

    assert trade.amount == pytest.approx(16.0)
    bot._pm_resize_stop_protection.assert_called_once_with(trade)


def test_pending_stop_store_failure_is_explicit_unknown(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="entry", side="buy", amount=11.0)
    mocker.patch.object(
        PMOrderIntent, "get_unresolved_for_pair", side_effect=RuntimeError("db down")
    )

    state, intent, outbox, owned = bot._pm_pending_conditional_stop_intent(trade)

    assert state == "unknown"
    assert intent is None and outbox is None and owned is False
    assert "intent_store_unavailable" in bot._pm_blocked_order_reasons()


def test_wall_clock_stale_snapshot_blocks_exposure(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.config["exchange"]["portfolio_margin_risk"]["market_data_max_candle_age_seconds"] = 660
    bot.strategy.pm_signal_snapshot = MagicMock(
        return_value={
            "candle_open_time": datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=900),
            "factor_hash": "h" * 64,
            "data_fresh": True,
            "freshness_detail": None,
            "signal_tag": None,
        }
    )
    bot.dataprovider.get_analyzed_dataframe = MagicMock(
        return_value=(
            pd.DataFrame(
                {
                    "date": [
                        datetime.now(UTC) - timedelta(minutes=20),
                        datetime.now(UTC) - timedelta(minutes=15),
                    ]
                }
            ),
            None,
        )
    )

    _snapshot, reason = bot._pm_ledger_snapshot(trade_pair := "ETH/USDT:USDT")

    assert trade_pair
    assert reason is not None and reason.startswith("market_data_stale")


def test_pm_maintenance_runs_before_market_reload(mocker, default_conf_usdt):
    from tests.conftest import get_patched_freqtradebot

    bot = get_patched_freqtradebot(mocker, default_conf_usdt)
    order = []
    bot._pm_consume_user_stream_events = MagicMock(side_effect=lambda: order.append("stream"))
    bot._pm_maintenance_schedule.run_pending = MagicMock(
        side_effect=lambda: order.append("maintenance")
    )
    bot._schedule.run_pending = MagicMock(side_effect=lambda: order.append("risk"))
    bot.exchange.reload_markets = MagicMock(side_effect=lambda: order.append("reload"))
    bot.update_trades_without_assigned_fees = MagicMock()
    mocker.patch.object(Trade, "get_open_trades", return_value=[])
    bot._refresh_active_whitelist = MagicMock(return_value=[])
    bot.dataprovider.refresh = MagicMock()
    bot.pairlists.create_pair_list = MagicMock(return_value=[])
    bot.strategy.gather_informative_pairs = MagicMock(return_value=[])
    bot.strategy.bot_loop_start = MagicMock()
    bot.strategy.analyze = MagicMock()
    bot.manage_open_orders = MagicMock()
    bot.exit_positions = MagicMock()
    bot.get_free_open_trades = MagicMock(return_value=False)
    bot._pm_record_entry_block_for_pairs = MagicMock()
    bot.rpc.process_msg_queue = MagicMock()

    bot.process()

    assert order.index("maintenance") < order.index("reload")
    assert order.index("risk") < order.index("reload")


def test_sync_closed_dca_never_calls_legacy_stop_cancel(mocker, pm_conf):
    """The exact execute_entry(pos_adjust)+closed branch uses safe resize only."""
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="st-old", side="sell", amount=11.0)
    bot._pm_blocked_order_reasons = MagicMock(return_value=[])
    bot._pm_pair_entry_block_reasons = MagicMock(return_value=[])
    bot._pm_exposure_data_block_reason = MagicMock(return_value=None)
    bot._pm_record_signal_decision = MagicMock(return_value=True)
    bot.get_valid_enter_price_and_stake = MagicMock(return_value=(0.01, 0.05, 1.0))
    bot.handle_similar_open_order = MagicMock(return_value=False)
    bot.exchange.create_order = MagicMock(
        return_value={
            "id": "dca-closed",
            "clientOrderId": "ft-dca-closed",
            "timestamp": None,
            "datetime": None,
            "lastTradeTimestamp": None,
            "symbol": trade.pair,
            "type": "limit",
            "timeInForce": "GTC",
            "side": trade.entry_side,
            "price": 0.01,
            "average": 0.01,
            "amount": 5.0,
            "filled": 5.0,
            "remaining": 0.0,
            "cost": 0.05,
            "status": "closed",
            "fee": None,
            "trades": [],
            "info": {},
        }
    )
    bot.exchange.get_fee = MagicMock(return_value=0.001)
    bot.exchange.get_pair_base_currency = MagicMock(return_value="ETH")
    bot.exchange.get_funding_fees = MagicMock(return_value=0.0)
    bot._pm_link_intent_for_order = MagicMock()
    bot._notify_enter = MagicMock()
    bot._pm_resize_stop_protection = MagicMock(return_value="active")
    bot.cancel_stoploss_on_exchange = MagicMock()
    mocker.patch("freqtrade.freqtradebot.update_liquidation_prices")

    assert bot.execute_entry(
        trade.pair,
        0.05,
        price=0.01,
        trade=trade,
        mode="pos_adjust",
        is_short=False,
        ordertype="limit",
    )

    bot._pm_resize_stop_protection.assert_called_once_with(trade)
    bot.cancel_stoploss_on_exchange.assert_not_called()
