"""End-to-end PM loop boundary regressions from 2026-09-10 production evidence."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import ccxt

from freqtrade.exceptions import InvalidOrderException, TemporaryError
from freqtrade.exchange.binance import Binance
from freqtrade.freqtradebot import FreqtradeBot
from freqtrade.persistence import Order, PMOutbox, Trade
from freqtrade.rpc.rpc import RPC
from tests.freqtradebot.test_pm_protection_correctness import switch_bot
from tests.freqtradebot.test_pm_recovery import make_pm_bot, make_stoploss_trade
from tests.freqtradebot.test_pm_unattended_gap_closure import pm_conf, pm_db


PAIR = "BTC/USDT:USDT"


def _symbol_exchange() -> Binance:
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._config = {"dry_run": False, "exchange": {"portfolio_margin": True}}
    exchange._markets = {
        PAIR: {
            "id": "BTCUSDT",
            "symbol": PAIR,
            "quote": "USDT",
            "settle": "USDT",
            "swap": True,
            "linear": True,
            "inverse": False,
            "contract": True,
            "future": True,
            "type": "swap",
            "active": True,
        }
    }
    exchange._pm_um_symbol_config_cache = {}
    exchange._pm_um_symbol_config_last_good = None
    exchange._pm_um_symbol_config_last_good_at = None
    exchange._pm_um_symbol_config_last_error = None
    exchange._exchange_ws = None
    exchange._api_async = None
    exchange._ws_async = None
    exchange.loop = None
    return exchange


def test_symbol_config_transient_failure_uses_lkg_but_marks_exposure_degraded(mocker, pm_conf):
    exchange = _symbol_exchange()
    exchange._papi_request = MagicMock(
        return_value=[
            {"symbol": "BTCUSDT", "marginType": "CROSSED", "maxNotionalValue": "1000000"}
        ]
    )
    assert exchange.get_pm_tradable_pairs() == {PAIR}
    assert exchange.get_pm_symbol_config_health()["degraded"] is False

    # Expire only the TTL cache; the separate durable-in-process LKG survives.
    exchange._pm_um_symbol_config_cache.clear()
    exchange._papi_request.side_effect = ccxt.NetworkError("remote closed connection")
    assert exchange.get_pm_tradable_pairs() == {PAIR}
    health = exchange.get_pm_symbol_config_health()
    assert health["degraded"] is True
    assert health["has_last_good"] is True
    assert "NetworkError" in health["last_error"]

    # A minimal bot consumes the degraded health as an exposure-only data gate;
    # no Freqtrade fixture/class monkeypatch is needed for this assertion.
    bot = FreqtradeBot.__new__(FreqtradeBot)
    bot.exchange = MagicMock()
    bot.exchange.get_pm_symbol_config_health.return_value = health
    bot._pm_cycle_exposure_block_reason = None
    assert bot._pm_exposure_data_block_reason(PAIR).startswith("pm_symbol_config_degraded:")

    # A later fresh account snapshot clears the gate automatically.
    exchange._pm_um_symbol_config_cache.clear()
    exchange._papi_request.side_effect = None
    exchange._papi_request.return_value = [
        {"symbol": "BTCUSDT", "marginType": "CROSSED", "maxNotionalValue": "1000000"}
    ]
    assert exchange.get_pm_tradable_pairs() == {PAIR}
    assert exchange.get_pm_symbol_config_health()["degraded"] is False


def test_symbol_config_network_failure_without_lkg_is_retryable_not_public_fallback():
    exchange = _symbol_exchange()
    exchange._papi_request = MagicMock(side_effect=ccxt.NetworkError("remote closed connection"))
    try:
        exchange.get_pm_tradable_pairs()
    except TemporaryError as exc:
        assert "no last-known-good" in str(exc)
    else:
        raise AssertionError("missing PM account whitelist must not be inferred from public markets")


def _add_local_stop(trade, stop_id: str = "stnew") -> Order:
    order = Order(
        ft_order_side="stoploss",
        ft_pair=trade.pair,
        ft_is_open=True,
        ft_amount=float(trade.amount),
        ft_price=trade.stoploss_or_liquidation,
        order_id=stop_id,
        status="open",
        symbol=trade.pair,
        order_type="stoploss",
        side=trade.exit_side,
        price=trade.stoploss_or_liquidation,
        filled=0.0,
        remaining=float(trade.amount),
        cost=0.0,
        order_date=datetime.now(UTC),
    )
    trade.orders.append(order)
    Trade.commit()
    return order


def _add_recent_stop_ack(trade, *, age_seconds: int = 1) -> PMOutbox:
    row = PMOutbox(
        client_id="stnew",
        operation="conditional",
        payload=json.dumps({
            "clientAlgoId": "stnew", "symbol": trade.pair,
            "side": trade.exit_side, "quantity": trade.amount, "reduceOnly": True,
        }),
        state="LINKED",
        exchange_order_id="stnew",
        raw_response='{"clientAlgoId":"stnew"}',
        origin_trade_id=trade.id,
        processed_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=age_seconds),
    )
    PMOutbox.session.add(row)
    Trade.commit()
    return row


def test_fresh_stop_ack_not_yet_visible_is_pending_not_missing(mocker, pm_conf):
    bot, trade = switch_bot(mocker, pm_conf)
    _add_recent_stop_ack(trade)
    bot.exchange.fetch_stoploss_order = MagicMock(side_effect=InvalidOrderException("not visible yet"))

    verdict = bot._pm_replace_stop_protection(trade, ["stold"], new_stop_price=0.009)

    assert verdict == "verification_pending"
    assert "stop_verification_pending" in bot._pm_blocked_order_reasons()
    assert "stop_protection_missing" not in bot._pm_blocked_order_reasons()
    bot.exchange.cancel_stoploss_order_with_result.assert_not_called()
    # Pending consistency is logged, not escalated to a critical missing-stop alert.
    bot.rpc.send_msg.assert_not_called()


def test_stop_ack_absence_escalates_only_after_verification_grace(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stnew", side="sell")
    row = _add_recent_stop_ack(trade, age_seconds=1)
    bot.strategy.order_types["stoploss_on_exchange"] = True
    bot.exchange.fetch_open_conditional_orders.return_value = []
    bot.exchange.fetch_stoploss_order = MagicMock(side_effect=InvalidOrderException("not visible"))

    assert bot._pm_verify_stop_protection() == []
    assert "stop_verification_pending" in bot._pm_blocked_order_reasons()

    row.processed_at = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=31)
    Trade.commit()
    offenders = bot._pm_verify_stop_protection()
    assert offenders and offenders[0]["trade_id"] == trade.id
    assert "stop_verification_pending" not in bot._pm_blocked_order_reasons()


def test_verified_old_stop_plus_fresh_replacement_pending_is_not_missing(mocker, pm_conf):
    bot, trade = switch_bot(mocker, pm_conf)
    _add_recent_stop_ack(trade)
    _add_local_stop(trade, "stnew")
    bot.strategy.order_types["stoploss_on_exchange"] = True
    old = {
        "id": "stold", "clientAlgoId": "stold", "symbol": trade.pair,
        "type": "stoploss", "side": "sell", "amount": float(trade.amount),
        "filled": 0.0, "remaining": float(trade.amount), "status": "open",
        "info": {"reduceOnly": True},
    }
    bot.exchange.fetch_open_conditional_orders.return_value = [old]
    bot.exchange.fetch_stoploss_order = MagicMock(side_effect=InvalidOrderException("new not visible"))

    assert bot._pm_verify_stop_protection() == []
    assert "stop_verification_pending" in bot._pm_blocked_order_reasons()
    assert "stop_retire_unresolved" not in bot._pm_blocked_order_reasons()


def test_confirm_trade_entry_log_is_strategy_not_user_attribution():
    source = (Path(__file__).parents[2] / "freqtrade" / "freqtradebot.py").read_text()
    assert "Strategy confirm_trade_entry rejected initial entry" in source
    assert "User denied entry for {pair}" not in source


def test_pm_status_exposes_whitelist_health_and_stop_verification_pending(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    bot.active_pair_whitelist = ["ETH/USDT:USDT"]
    bot._pm_blocked_order_reasons = MagicMock(return_value=[])
    bot._pm_last_reconcile_result = {
        "unprotected_positions": [],
        "stop_verification_pending": [
            {"trade_id": 7, "pair": "XAG/USDT:USDT", "ids": ["stpending"]}
        ],
    }
    bot.exchange.get_pm_risk_summary = MagicMock(
        return_value={
            "enabled": True,
            "account_status": "NORMAL",
            "uni_mmr": 10.0,
            "account_equity": 1000.0,
            "initial_margin": 10.0,
            "maintenance_margin": 1.0,
        }
    )
    bot.exchange.fetch_positions = MagicMock(return_value=[])
    bot.exchange.get_balances = MagicMock(return_value={})
    bot.exchange.get_pm_user_stream_stats = MagicMock(return_value={"connected": True})
    bot.exchange.get_market_data_health = MagicMock(return_value={})
    bot.exchange.get_pm_read_freshness_stats = MagicMock(return_value={})
    bot.exchange.get_pm_symbol_config_health = MagicMock(
        return_value={
            "enabled": True,
            "has_last_good": True,
            "last_good_age_s": 12.0,
            "degraded": True,
            "last_error": "NetworkError: remote closed connection",
        }
    )

    status = RPC(bot)._rpc_pm_status()

    assert status["pm_symbol_config"]["degraded"] is True
    assert status["entry_permission"]["global_allowed"] is False
    assert "pm_symbol_config_degraded" in status["entry_permission"]["exposure_data_reasons"]
    assert status["protection"]["verification_pending"][0]["trade_id"] == 7


def test_grace_requires_owned_covering_identity_and_does_not_restart_clock(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stnew", side="sell")
    row = _add_recent_stop_ack(trade)
    assert bot._pm_stop_ack_within_verification_grace(trade, "stnew")
    row.origin_trade_id = None
    assert not bot._pm_stop_ack_within_verification_grace(trade, "stnew")
    row.origin_trade_id = trade.id
    original = row.payload
    for field, bad in [
        ("symbol", "BTC/USDT:USDT"), ("side", "buy"),
        ("quantity", trade.amount / 2), ("reduceOnly", False),
        ("clientAlgoId", "different-candidate"),
    ]:
        request = json.loads(original)
        request[field] = bad
        row.payload = json.dumps(request)
        assert not bot._pm_stop_ack_within_verification_grace(trade, "stnew")
    row.payload = original
    row.created_at = datetime.now(UTC) - timedelta(seconds=60)
    row.processed_at = datetime.now(UTC)
    row.linked_at = datetime.now(UTC)
    assert not bot._pm_stop_ack_within_verification_grace(trade, "stnew")


def test_pending_candidate_loop_never_posts_another_stop(mocker, pm_conf):
    bot = make_pm_bot(mocker, pm_conf)
    trade = make_stoploss_trade(pm_conf, order_id="stnew", side="sell")
    _add_recent_stop_ack(trade)
    bot.exchange.fetch_stoploss_order = MagicMock(side_effect=InvalidOrderException("not visible"))
    bot.exchange.create_stoploss = MagicMock()
    for _ in range(4):
        assert bot._pm_replace_stop_protection(trade, ["stnew"]) == "kept_old"
    assert "stop_verification_pending" in bot._pm_blocked_order_reasons()
    assert trade.open_sl_orders[0].ft_is_open
    bot.exchange.create_stoploss.assert_not_called()
