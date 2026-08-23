"""
RPC-level tests for the PM commands (/pm_close settlement filtering, failure
alerting) and the shared /pm_recover path.
"""

from unittest.mock import MagicMock, PropertyMock

import pytest

from freqtrade.enums import RPCMessageType, State, TradingMode
from freqtrade.persistence import Order, Trade
from freqtrade.rpc import RPC, RPCException
from freqtrade.util.datetime_helpers import dt_now
from tests.conftest import get_patched_freqtradebot


@pytest.fixture
def pm_ftbot(mocker, default_conf_usdt):
    conf = default_conf_usdt.copy()
    conf["dry_run"] = False
    conf["trading_mode"] = "futures"
    conf["margin_mode"] = "cross"
    conf["stake_currency"] = "USDT"
    conf["exchange"] = conf["exchange"].copy()
    conf["exchange"]["name"] = "binance"
    conf["exchange"]["key"] = "dummy_key"
    conf["exchange"]["secret"] = "dummy_secret"
    conf["exchange"]["portfolio_margin"] = True
    conf["exchange"]["portfolio_margin_risk"] = {
        "min_uni_mmr": 1.5,
        "user_stream_enabled": False,
    }
    conf["exchange"]["pair_whitelist"] = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
    conf["exchange"]["pair_blacklist"] = []
    # validate_config checks settle/inverse on whitelisted pairs - provide PM markets.
    pm_markets = {
        "BTC/USDT:USDT": {
            "id": "BTCUSDT",
            "settle": "USDT",
            "inverse": False,
            "swap": True,
            "linear": True,
            "active": True,
        },
        "ETH/USDT:USDT": {
            "id": "ETHUSDT",
            "settle": "USDT",
            "inverse": False,
            "swap": True,
            "linear": True,
            "active": True,
        },
        "BTC/USDT:USDC": {
            "id": "BTCUSDC",
            "settle": "USDC",
            "inverse": False,
            "swap": True,
            "linear": True,
            "active": True,
        },
    }
    # patch_exchange() (inside get_patched_freqtradebot) resolves its mock markets
    # through tests.conftest.get_markets - patch it so PM pairs are available.
    mocker.patch("tests.conftest.get_markets", return_value=pm_markets)
    ftbot = get_patched_freqtradebot(mocker, conf)
    mocker.patch.object(
        type(ftbot.exchange),
        "markets",
        PropertyMock(return_value=pm_markets),
        create=True,
    )
    ftbot.rpc.send_msg = MagicMock()
    return ftbot


def _open_pm_trade(pair, exchange="binance", amount=1.0):
    trade = Trade(
        pair=pair,
        open_rate=2.0,
        exchange=exchange,
        amount=amount,
        fee_open=0.0,
        fee_close=0.0,
        stake_amount=2.0,
        open_date=dt_now(),
        is_open=True,
        trading_mode=TradingMode.FUTURES,
    )
    Trade.session.add(trade)
    Trade.commit()
    return trade


def test_pm_close_failure_sends_explicit_warning(mocker, pm_ftbot, init_persistence):
    """Failed pm_close exits must send an explicit WARNING (never silent)."""
    rpc = RPC(pm_ftbot)
    _open_pm_trade("BTC/USDT:USDT")
    pm_ftbot.state = State.RUNNING
    pm_ftbot._exit_lock = MagicMock()  # type: ignore[assignment]

    rpc._freqtrade._exit_lock = pm_ftbot._exit_lock
    exec_mock = MagicMock(return_value=False)
    mocker.patch.object(rpc, "_RPC__exec_force_exit", exec_mock)
    pm_ftbot.wallets.update = MagicMock()

    result = rpc._rpc_pm_close(target_currency="USDT", ordertype="market")

    assert "Failed ids" in result["result"]
    warning_calls = [
        c for c in pm_ftbot.rpc.send_msg.call_args_list if c[0][0]["type"] == RPCMessageType.WARNING
    ]
    assert warning_calls, "failed pm_close must send an RPC WARNING"
    assert "remain OPEN" in warning_calls[0][0][0]["status"]


def test_pm_close_filters_settlement_currency(mocker, pm_ftbot, init_persistence):
    """CONFIRM'd pm_close only closes the requested settlement currency."""
    rpc = RPC(pm_ftbot)
    _open_pm_trade("BTC/USDT:USDT")
    _open_pm_trade("BTC/USDT:USDC")
    pm_ftbot.state = State.RUNNING
    pm_ftbot._exit_lock = MagicMock()  # type: ignore[assignment]
    rpc._freqtrade._exit_lock = pm_ftbot._exit_lock
    pm_ftbot.wallets.update = MagicMock()

    closed: list = []
    exec_mock = MagicMock(
        side_effect=lambda trade, ot: closed.append(trade.pair) or True
    )
    mocker.patch.object(rpc, "_RPC__exec_force_exit", exec_mock)

    result = rpc._rpc_pm_close(target_currency="USDT", ordertype="market")

    assert closed == ["BTC/USDT:USDT"]
    assert "Skipped other settlements" in result["result"]


def test_pm_close_requires_running_bot(mocker, pm_ftbot, init_persistence):
    rpc = RPC(pm_ftbot)
    pm_ftbot.state = State.STOPPED
    _open_pm_trade("BTC/USDT:USDT")

    with pytest.raises(RPCException, match="trader is not running"):
        rpc._rpc_pm_close(target_currency="all", ordertype="market")


def test_pm_recover_rpc_uses_shared_reconcile(mocker, pm_ftbot, init_persistence):
    """The RPC /pm_recover must delegate to the shared, full-lifecycle reconcile."""
    rpc = RPC(pm_ftbot)
    mocker.patch.object(
        pm_ftbot.exchange,
        "_is_portfolio_margin",
        MagicMock(return_value=True),
    )
    reconcile_mock = mocker.patch.object(
        pm_ftbot,
        "_pm_reconcile_open_orders",
        return_value={
            "open_trades": 1,
            "checked": 1,
            "reconciled": 1,
            "mismatches": ["order1(ETH/USDT:USDT): DB=open Exchange=closed"],
            "errors": [],
        },
    )

    result = rpc._rpc_pm_recover()

    reconcile_mock.assert_called_once()
    assert result["orders_checked"] == 1
    assert result["mismatches"][0].startswith("order1")
