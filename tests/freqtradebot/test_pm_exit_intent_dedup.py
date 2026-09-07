"""Regression tests for PM timeout-uncertain reduce-only exit de-duplication."""

from unittest.mock import MagicMock

import pytest

from freqtrade.enums import ExitCheckTuple, ExitType
from freqtrade.persistence import Trade
from freqtrade.persistence.pm_order_intent import PMOrderIntent
from tests.freqtradebot.test_pm_recovery import make_open_trade, make_pm_bot


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
    conf["exchange"]["portfolio_margin_risk"] = {"user_stream_enabled": False}
    return conf


def _intent(trade, *, kind="order", reduce_only=True, side=None, client_id="ft-pending-exit"):
    row = PMOrderIntent(
        client_id=client_id,
        kind=kind,
        pair=trade.pair,
        side=side or trade.exit_side,
        order_type="market" if kind == "order" else "stop_market",
        amount=trade.amount,
        reduce_only=reduce_only,
        state="UNKNOWN",
    )
    PMOrderIntent.session.add(row)
    Trade.commit()
    return row


def test_repeated_exit_is_suppressed_while_same_reduce_only_intent_unresolved(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="entry-dedup", side="buy", amount=11.0)
    pending = _intent(trade)
    bot._pm_auto_recovery_due = MagicMock(return_value=False)
    bot.exchange.create_order = MagicMock()

    for _ in range(8):
        result = bot.execute_trade_exit(
            trade,
            limit=trade.open_rate,
            exit_check=ExitCheckTuple(exit_type=ExitType.ROI),
        )
        assert result is False

    bot.exchange.create_order.assert_not_called()
    assert PMOrderIntent.get_by_client_id(pending.client_id) is not None
    assert len(PMOrderIntent.get_unresolved_for_pair(trade.pair)) == 1


def test_pending_exit_triggers_same_id_recovery_before_suppression(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="entry-recover", side="buy", amount=11.0)
    _intent(trade, client_id="ft-recover-me")
    bot._pm_auto_recovery_due = MagicMock(return_value=True)
    bot._pm_recover_pending_intents = MagicMock(return_value={"unresolved": 1})
    bot.exchange.create_order = MagicMock()

    result = bot.execute_trade_exit(
        trade,
        limit=trade.open_rate,
        exit_check=ExitCheckTuple(exit_type=ExitType.ROI),
    )

    assert result is False
    bot._pm_recover_pending_intents.assert_called_once_with()
    bot.exchange.create_order.assert_not_called()


def test_conditional_stop_intent_does_not_block_normal_reduce_only_exit(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="entry-stop", side="buy", amount=11.0)
    _intent(trade, kind="conditional", client_id="st-pending")

    assert bot._pm_pending_reduce_only_exit_intent(trade) is None


def test_exposure_increasing_intent_does_not_block_risk_reducing_exit(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_open_trade(pm_conf, order_id="entry-other", side="buy", amount=11.0)
    _intent(trade, reduce_only=False, side=trade.entry_side, client_id="ft-entry-pending")

    assert bot._pm_pending_reduce_only_exit_intent(trade) is None
