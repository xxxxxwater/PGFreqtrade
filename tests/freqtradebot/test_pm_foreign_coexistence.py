"""Ownership-aware PM coexistence: MANUAL/FOREIGN + VWAP bot in one account."""

import json
from copy import deepcopy
from unittest.mock import MagicMock

from freqtrade.persistence import Trade
from freqtrade.persistence.pm_outbox import PMOutbox
from tests.freqtradebot.test_pm_recovery import make_open_trade, make_pm_bot
from tests.freqtradebot.test_pm_recovery import pm_conf as _pm_conf


pm_conf = _pm_conf


def _enable_coexistence(conf):
    risk = conf["exchange"].setdefault("portfolio_margin_risk", {})
    risk["allow_foreign_positions"] = True
    risk["emergency_close_foreign_positions"] = False


def _add_btc_market(bot):
    market = deepcopy(bot.exchange.markets["ETH/USDT:USDT"])
    market["id"] = "BTCUSDT"
    market["symbol"] = "BTC/USDT:USDT"
    market["base"] = "BTC"
    bot.exchange.markets["BTC/USDT:USDT"] = market


def _position(pair, contracts, side="long"):
    return {"symbol": pair, "side": side, "contracts": contracts}


def test_foreign_position_does_not_trigger_global_quantity_gate(mocker, pm_conf):
    _enable_coexistence(pm_conf)
    bot = make_pm_bot(mocker, pm_conf)
    _add_btc_market(bot)
    bot.exchange.fetch_positions = MagicMock(
        return_value=[_position("BTC/USDT:USDT", 8.647, "short")]
    )

    assert bot._pm_account_position_reconcile() == []
    assert bot._pm_foreign_position_pairs == {"BTC/USDT:USDT"}


def test_foreign_position_blocks_only_same_pair_not_other_whitelist_pair(mocker, pm_conf):
    _enable_coexistence(pm_conf)
    bot = make_pm_bot(mocker, pm_conf)
    _add_btc_market(bot)
    bot.exchange.fetch_positions = MagicMock(
        return_value=[_position("BTC/USDT:USDT", 8.647, "short")]
    )
    bot.exchange.fetch_open_orders = MagicMock(return_value=[])
    bot.exchange.fetch_open_conditional_orders = MagicMock(return_value=[])

    assert "foreign_position_conflict" in bot._pm_pair_entry_block_reasons("BTC/USDT:USDT")
    assert bot._pm_pair_entry_block_reasons("ETH/USDT:USDT") == []
    assert "position_quantity_mismatch" not in bot._pm_blocked_order_reasons()


def test_bot_owned_mismatch_still_global_fail_closed_with_foreign_coexistence(mocker, pm_conf):
    _enable_coexistence(pm_conf)
    bot = make_pm_bot(mocker, pm_conf)
    _add_btc_market(bot)
    trade = make_open_trade(pm_conf, amount=11.0)
    bot.exchange.fetch_positions = MagicMock(
        return_value=[
            _position(trade.pair, 3.0, "long"),
            _position("BTC/USDT:USDT", 8.647, "short"),
        ]
    )
    bot.exchange.fetch_open_orders = MagicMock(return_value=[])
    bot.exchange.fetch_open_conditional_orders = MagicMock(return_value=[])
    bot.rpc.send_msg = MagicMock()

    report = bot._pm_reconcile_open_orders()

    assert any(trade.pair in item for item in report["position_mismatches"])
    assert all("BTC/USDT:USDT" not in item for item in report["position_mismatches"])
    assert "position_quantity_mismatch" in bot._pm_blocked_order_reasons()
    assert "BTC/USDT:USDT" in report["foreign_position_pairs"]


def test_manual_open_orders_are_pair_local_foreign_conflicts(mocker, pm_conf):
    _enable_coexistence(pm_conf)
    bot = make_pm_bot(mocker, pm_conf)
    _add_btc_market(bot)
    bot.exchange.fetch_positions = MagicMock(return_value=[])
    bot.exchange.fetch_open_orders = MagicMock(
        side_effect=lambda pair=None, **_: (
            [{"id": "9001", "symbol": "BTC/USDT:USDT", "clientOrderId": "ios_manual_1"}]
            if pair in {None, "BTC/USDT:USDT"}
            else []
        )
    )
    bot.exchange.fetch_open_conditional_orders = MagicMock(
        side_effect=lambda pair=None: (
            [{"id": "ios_algo_1", "symbol": "BTC/USDT:USDT", "clientAlgoId": "ios_algo_1"}]
            if pair in {None, "BTC/USDT:USDT"}
            else []
        )
    )

    assert "foreign_order_conflict" in bot._pm_pair_entry_block_reasons("BTC/USDT:USDT")
    assert bot._pm_pair_entry_block_reasons("ETH/USDT:USDT") == []
    assert "reconciliation_incomplete" not in bot._pm_blocked_order_reasons()


def test_untracked_framework_order_is_never_reclassified_foreign(mocker, pm_conf):
    _enable_coexistence(pm_conf)
    bot = make_pm_bot(mocker, pm_conf)
    bot.exchange.fetch_positions = MagicMock(return_value=[])
    bot.exchange.fetch_open_orders = MagicMock(
        return_value=[
            {
                "id": "12345",
                "symbol": "ETH/USDT:USDT",
                "clientOrderId": "ft_missing_but_ours",
            }
        ]
    )
    bot.exchange.fetch_open_conditional_orders = MagicMock(return_value=[])

    reasons = bot._pm_pair_entry_block_reasons("ETH/USDT:USDT")

    assert "bot_order_ownership_conflict" in reasons
    assert "reconciliation_incomplete" in bot._pm_blocked_order_reasons()
    assert "ETH/USDT:USDT" not in bot._pm_foreign_order_pairs


def test_startup_consistency_treats_positive_foreign_evidence_as_nonfatal(mocker, pm_conf):
    _enable_coexistence(pm_conf)
    bot = make_pm_bot(mocker, pm_conf)
    _add_btc_market(bot)
    bot.exchange.fetch_positions = MagicMock(
        return_value=[_position("BTC/USDT:USDT", 8.647, "short")]
    )
    bot.exchange.fetch_open_orders = MagicMock(
        return_value=[{"id": "9001", "symbol": "BTC/USDT:USDT", "clientOrderId": "ios_manual"}]
    )
    bot.exchange.fetch_open_conditional_orders = MagicMock(
        return_value=[{"id": "ios_algo", "symbol": "BTC/USDT:USDT", "clientAlgoId": "ios_algo"}]
    )

    result = bot._pm_startup_consistency_check()

    assert result["status"] == "consistent"
    assert result["unknown_positions"] == []
    assert result["unknown_orders"] == []
    assert result["unknown_conditional_orders"] == []
    assert len(result["foreign_positions"]) == 1
    assert len(result["foreign_orders"]) == 1
    assert len(result["foreign_conditional_orders"]) == 1


def test_startup_framework_order_still_mismatch_in_coexistence_mode(mocker, pm_conf):
    _enable_coexistence(pm_conf)
    bot = make_pm_bot(mocker, pm_conf)
    bot.exchange.fetch_positions = MagicMock(return_value=[])
    bot.exchange.fetch_open_orders = MagicMock(
        return_value=[
            {
                "id": "12345",
                "symbol": "ETH/USDT:USDT",
                "clientOrderId": "ft_missing_but_ours",
            }
        ]
    )
    bot.exchange.fetch_open_conditional_orders = MagicMock(return_value=[])

    result = bot._pm_startup_consistency_check()

    assert result["status"] == "mismatch"
    assert len(result["unknown_orders"]) == 1
    assert result["foreign_orders"] == []


def test_emergency_policy_preserves_foreign_manual_position(mocker, pm_conf):
    _enable_coexistence(pm_conf)
    bot = make_pm_bot(mocker, pm_conf)
    _add_btc_market(bot)
    bot.rpc.send_msg = MagicMock()
    bot.exchange.fetch_positions = MagicMock(
        return_value=[_position("BTC/USDT:USDT", 8.647, "short")]
    )
    bot._pm_force_close_foreign_position = MagicMock(return_value=True)

    bot._pm_emergency_close_all()

    bot._pm_force_close_foreign_position.assert_not_called()
    assert any("preserved" in str(call).lower() for call in bot.rpc.send_msg.call_args_list)



def test_active_outbox_evidence_prevents_foreign_downgrade(mocker, pm_conf):
    _enable_coexistence(pm_conf)
    bot = make_pm_bot(mocker, pm_conf)
    _add_btc_market(bot)
    row = PMOutbox(
        client_id="ft_active_outbox_owner",
        operation="order",
        payload=json.dumps({"symbol": "BTCUSDT", "side": "BUY"}),
        state="ACKED",
        exchange_order_id="99112233",
    )
    PMOutbox.session.add(row)
    Trade.commit()
    bot.exchange.fetch_positions = MagicMock(
        return_value=[_position("BTC/USDT:USDT", 1.0, "long")]
    )

    mismatches = bot._pm_account_position_reconcile()

    assert mismatches
    assert "BTC/USDT:USDT" in mismatches[0]
    assert "BTC/USDT:USDT" not in bot._pm_foreign_position_pairs

def test_legacy_default_still_treats_unknown_position_as_mismatch(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    _add_btc_market(bot)
    bot.exchange.fetch_positions = MagicMock(
        return_value=[_position("BTC/USDT:USDT", 1.0, "long")]
    )

    mismatches = bot._pm_account_position_reconcile()

    assert mismatches
    assert "BTC/USDT:USDT" in mismatches[0]
    assert bot._pm_foreign_position_pairs == set()
