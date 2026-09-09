import asyncio
from datetime import UTC, datetime
from threading import RLock
from unittest.mock import MagicMock

import ccxt
import pytest

from freqtrade.enums import PriceType
from freqtrade.exceptions import OperationalException, TemporaryError
from freqtrade.exchange.binance import Binance


PAIR = "BTC/USDT:USDT"


def set_minimal_exchange_cleanup_attrs(exchange):
    exchange._exchange_ws = None
    exchange._api_async = None
    exchange._ws_async = None
    exchange.loop = None
    exchange._markets = {
        PAIR: {"id": "BTCUSDT", "settle": "USDT", "inverse": False}
    }


class _EmptyIntentModel:
    """Stands in for PMOrderIntent in unit tests without a database."""

    @classmethod
    def get_unresolved_exposure_increasing(cls):
        return []


def make_pm_exchange(risk_config):
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {
        "dry_run": False,
        "exchange": {"portfolio_margin_risk": risk_config},
    }
    exchange.fetch_pm_account_information = MagicMock(
        return_value={
            "accountStatus": "NORMAL",
            "uniMMR": "10",
            "accountEquity": "1000",
            "totalCollateralValue": "1000",
            "accountInitialMargin": "50",
            "accountMaintMargin": "5",
        }
    )
    exchange.fetch_positions = MagicMock(
        return_value=[{"symbol": PAIR, "initialMargin": 5.0, "leverage": 10.0}]
    )
    exchange.get_rate = MagicMock(return_value=60000)
    # Risk reservation inputs (open orders + unresolved intents).
    exchange.fetch_open_orders = MagicMock(return_value=[])
    exchange._pm_intent_model = MagicMock(return_value=_EmptyIntentModel)
    return exchange


def test_pm_replace_entry_counts_new_notional():
    """A replace entry IS a new exposure event and must be counted against caps."""
    exchange = make_pm_exchange({"max_total_notional": 100})

    # existing position 50 + replace 60 = 110 > 100 -> refused
    with pytest.raises(OperationalException, match="entry_mode=replace"):
        exchange.assert_pm_risk_allows_order(
            pair=PAIR, amount=0.001, leverage=5, entry_mode="replace"
        )

    exchange.fetch_positions.assert_called_once()


def test_pm_position_adjustment_counts_incremental_notional():
    exchange = make_pm_exchange({"max_total_notional": 100})

    with pytest.raises(OperationalException, match="entry_mode=pos_adjust"):
        exchange.assert_pm_risk_allows_order(
            pair=PAIR, amount=0.001, leverage=5, entry_mode="pos_adjust"
        )


def test_pm_dynamic_pairlist_uses_wildcard_position_notional_cap():
    exchange = make_pm_exchange({"max_position_notional": {"*": 100}})

    with pytest.raises(OperationalException, match=r"per-pair max 100"):
        exchange.assert_pm_risk_allows_order(pair=PAIR, amount=0.001, leverage=1)

    exchange.fetch_positions.assert_called_once()


def test_pm_exact_position_cap_overrides_dynamic_pairlist_wildcard():
    exchange = make_pm_exchange({"max_position_notional": {"*": 1000, PAIR: 100}})

    with pytest.raises(OperationalException, match=r"per-pair max 100"):
        exchange.assert_pm_risk_allows_order(pair=PAIR, amount=0.001, leverage=1)


@pytest.mark.parametrize(("side", "is_short"), [("buy", False), ("sell", True)])
def test_pm_create_order_passes_entry_mode_and_direction_to_risk_check(side, is_short):
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {"dry_run": False, "exchange": {"portfolio_margin_risk": {}}}
    exchange._get_params = MagicMock(return_value={})
    exchange.amount_to_precision = MagicMock(side_effect=lambda pair, amount: amount)
    exchange._amount_to_contracts = MagicMock(side_effect=lambda pair, amount: amount)
    exchange._order_needs_price = MagicMock(return_value=False)
    exchange.assert_pm_risk_allows_order = MagicMock()
    exchange._lev_prep = MagicMock()
    exchange.pm_has_unresolved_intents = MagicMock(return_value=False)
    exchange._pm_place_order = MagicMock(return_value={"id": "1", "status": "open"})

    exchange.create_order(
        pair=PAIR,
        ordertype="market",
        side=side,
        amount=0.001,
        rate=60000,
        leverage=5,
        entry_mode="pos_adjust",
    )

    exchange.assert_pm_risk_allows_order.assert_called_once_with(
        pair=PAIR,
        amount=0.001,
        leverage=5,
        entry_mode="pos_adjust",
        is_short=is_short,
    )


def test_pm_notional_risk_uses_directional_entry_price():
    exchange = make_pm_exchange({"max_total_notional": 1000})

    exchange.assert_pm_risk_allows_order(
        pair=PAIR, amount=0.001, leverage=1, is_short=True
    )

    exchange.get_rate.assert_called_once_with(
        PAIR, side="entry", is_short=True, refresh=True
    )


def test_pm_create_order_blocked_on_unresolved_intents():
    """Exposure-increasing orders must be refused when intents are unresolved."""
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {"dry_run": False, "exchange": {"portfolio_margin_risk": {}}}
    exchange._get_params = MagicMock(return_value={})
    exchange.pm_has_unresolved_intents = MagicMock(return_value=True)

    with pytest.raises(TemporaryError, match="exposure-increasing order intents"):
        exchange.create_order(
            pair=PAIR,
            ordertype="market",
            side="buy",
            amount=0.001,
            rate=60000,
            leverage=5,
            entry_mode="initial",
        )


def test_pm_risk_config_rejects_unknown_keys():
    exchange = Binance.__new__(Binance)
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)

    with pytest.raises(OperationalException, match="Unknown Binance PM risk config key"):
        exchange._validate_pm_risk_config(
            {"exchange": {"portfolio_margin_risk": {"min_unimmr": 1.5}}}
        )


def test_pm_risk_config_accepts_market_data_budget_keys():
    exchange = Binance.__new__(Binance)
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)

    exchange._validate_pm_risk_config(
        {
            "exchange": {
                "portfolio_margin_risk": {
                    "market_analysis_budget_seconds": 12,
                    "market_data_max_candle_age_seconds": 660,
                }
            }
        }
    )


class _MarketsApi:
    def __init__(self) -> None:
        self.has = {"fetchCurrencies": True}
        self.session = None
        self.fetch_currencies_values: list[bool] = []

    async def load_markets(self, reload=False, params=None):
        self.fetch_currencies_values.append(self.has["fetchCurrencies"])
        return {}

    async def close(self):
        return None


def test_pm_reload_markets_skips_ccxt_fetch_currencies():
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._api_async = _MarketsApi()

    asyncio.run(exchange._api_reload_markets(reload=True))

    assert exchange._api_async.fetch_currencies_values == [False]
    assert exchange._api_async.has["fetchCurrencies"] is True


def test_non_pm_reload_markets_keeps_ccxt_fetch_currencies():
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = False
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._api_async = _MarketsApi()

    asyncio.run(exchange._api_reload_markets(reload=True))

    assert exchange._api_async.fetch_currencies_values == [True]
    assert exchange._api_async.has["fetchCurrencies"] is True


def test_pm_papi_request_uses_ccxt_papi_namespace():
    exchange = Binance.__new__(Binance)
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {"exchange": {}}
    exchange._api = MagicMock()
    exchange._api.request.return_value = {"accountStatus": "NORMAL"}

    assert exchange._papi_request("account", "GET") == {"accountStatus": "NORMAL"}

    exchange._api.request.assert_called_once_with("account", "papi", "GET", {})


def test_pm_papi_request_normalizes_full_path():
    exchange = Binance.__new__(Binance)
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {"exchange": {}}
    exchange._api = MagicMock()
    exchange._api.request.return_value = []

    exchange._papi_request("/papi/v1/um/positionRisk", "GET", {"symbol": "BTCUSDT"})

    exchange._api.request.assert_called_once_with(
        "um/positionRisk", "papi", "GET", {"symbol": "BTCUSDT"}
    )


def test_pm_papi_auth_error_includes_request_ip_hint():
    exchange = Binance.__new__(Binance)
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {"exchange": {}}
    exchange._api = MagicMock()
    exchange._api.request.side_effect = ccxt.AuthenticationError(
        'binance {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action, '
        'request ip: 43.212.29.197"}'
    )

    with pytest.raises(OperationalException, match=r"43\.212\.29\.197"):
        exchange._papi_request("account", "GET")


# ---------------------------------------------------------------------------
# Fail-closed risk checks
# ---------------------------------------------------------------------------


def test_pm_risk_fail_closed_missing_account_status():
    exchange = make_pm_exchange({})
    exchange.fetch_pm_account_information = MagicMock(return_value={"uniMMR": "10"})

    with pytest.raises(OperationalException, match="accountStatus is missing"):
        exchange.assert_pm_risk_allows_order(pair=PAIR, amount=0.001, leverage=5)


def test_pm_risk_fail_closed_missing_uni_mmr_when_configured():
    exchange = make_pm_exchange({"min_uni_mmr": 1.5})
    exchange.fetch_pm_account_information = MagicMock(return_value={"accountStatus": "NORMAL"})

    with pytest.raises(OperationalException, match="uniMMR is missing"):
        exchange.assert_pm_risk_allows_order(pair=PAIR, amount=0.001, leverage=5)


def test_pm_risk_fail_closed_api_exception():
    exchange = make_pm_exchange({})
    exchange.fetch_pm_account_information = MagicMock(side_effect=ccxt.ExchangeError("boom"))

    with pytest.raises(OperationalException, match="fail-closed"):
        exchange.assert_pm_risk_allows_order(pair=PAIR, amount=0.001, leverage=5)


def test_pm_risk_fail_closed_price_unavailable():
    exchange = make_pm_exchange({"max_total_notional": 100})
    exchange.get_rate = MagicMock(side_effect=Exception("no price"))

    with pytest.raises(OperationalException, match="Could not fetch entry price"):
        exchange.assert_pm_risk_allows_order(pair=PAIR, amount=0.001, leverage=5)


def test_pm_risk_fail_closed_zero_price():
    exchange = make_pm_exchange({"max_total_notional": 100})
    exchange.get_rate = MagicMock(return_value=0)

    with pytest.raises(OperationalException, match="Invalid entry price"):
        exchange.assert_pm_risk_allows_order(pair=PAIR, amount=0.001, leverage=5)


# ---------------------------------------------------------------------------
# PM order endpoints
# ---------------------------------------------------------------------------


def test_pm_fetch_orders_uses_papi_all_orders():
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {"dry_run": False, "exchange": {}}
    exchange._pm_symbol_for_pair = MagicMock(return_value="BTCUSDT")
    exchange._pm_namespace_for_pair = MagicMock(return_value="um")
    exchange._papi_request = MagicMock(return_value=[{"orderId": 1, "status": "FILLED"}])
    exchange._log_exchange_response = MagicMock()
    exchange._parse_pm_order = MagicMock(return_value={"id": "1", "status": "closed"})
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)

    orders = exchange._fetch_orders(PAIR, datetime(2024, 1, 1, tzinfo=UTC))

    assert len(orders) == 1
    assert exchange._papi_request.call_args[0][0] == "um/allOrders"
    request = exchange._papi_request.call_args[0][2]
    assert request["symbol"] == "BTCUSDT"
    assert "startTime" in request


def test_pm_fetch_order_strips_stop_param():
    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {"dry_run": False, "exchange": {}}
    exchange._pm_symbol_for_pair = MagicMock(return_value="BTCUSDT")
    exchange._pm_namespace_for_pair = MagicMock(return_value="um")
    exchange._papi_request = MagicMock(return_value={"orderId": 1, "status": "NEW"})
    exchange._log_exchange_response = MagicMock()
    exchange._parse_pm_order = MagicMock(return_value={"id": "1", "status": "open"})
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)

    exchange.fetch_order("1", PAIR, params={"stop": True})

    request = exchange._papi_request.call_args[0][2]
    assert "stop" not in request
    assert request["orderId"] == "1"


def test_pm_create_stoploss_uses_papi_algo_reduce_only(default_conf_usdt, tmp_path):
    """PM stoploss goes through the PAPI algo endpoint and persists ACK evidence."""
    from freqtrade.persistence import init_db
    from freqtrade.persistence.pm_order_intent import PMOrderIntent

    init_db(default_conf_usdt["db_url"])

    exchange = Binance.__new__(Binance)
    exchange._portfolio_margin = True
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {"dry_run": False, "exchange": {}, "user_data_dir": str(tmp_path)}
    exchange.pm_has_unresolved_intents_for_pair = MagicMock(return_value=False)
    exchange._get_stop_order_type = MagicMock(return_value=("stop_market", "market"))
    exchange._pm_new_client_strategy_id = MagicMock(return_value="st1234567890abcdef")
    exchange._pm_namespace_for_pair = MagicMock(return_value="um")
    exchange._pm_symbol_for_pair = MagicMock(return_value="BTCUSDT")
    exchange.price_to_precision = MagicMock(side_effect=lambda pair, price, **kw: price)
    exchange.amount_to_precision = MagicMock(side_effect=lambda pair, amount: amount)
    exchange._amount_to_contracts = MagicMock(side_effect=lambda pair, amount: amount)
    exchange._lev_prep = MagicMock()
    exchange._papi_request = MagicMock(
        return_value={"algoId": 9, "algoStatus": "NEW", "orderType": "STOP_MARKET"}
    )
    exchange._log_exchange_response = MagicMock()
    exchange._parse_pm_conditional_order = MagicMock(return_value={"id": "9", "status": "open"})
    exchange._order_contracts_to_amount = MagicMock(side_effect=lambda o: o)
    exchange._pm_log_order_path_metrics = MagicMock()

    exchange.create_stoploss(
        pair=PAIR,
        amount=0.001,
        stop_price=60000,
        order_types={"stoploss": "market", "stoploss_price_type": PriceType.LAST},
        side="sell",
        leverage=5,
    )

    assert exchange._papi_request.call_args[0][0] == "um/algo/order"
    request = exchange._papi_request.call_args[0][2]
    assert request["algoType"] == "CONDITIONAL"
    assert request["type"] == "STOP_MARKET"
    assert request["reduceOnly"] == "true"
    assert request["triggerPrice"] == 60000
    assert request["workingType"] == "CONTRACT_PRICE"
    assert request["clientAlgoId"] == "st1234567890abcdef"
    # New contract: intent ACKED with evidence, not deleted on ACK.
    intents = PMOrderIntent.get_unresolved()
    assert len(intents) == 1
    assert intents[0].state == "ACKED"
    assert intents[0].kind == "conditional"
    assert intents[0].reduce_only is True
    assert intents[0].exchange_order_id == "9"


def test_pm_papi_operation_rejected_auth_error_is_not_retried():
    exchange = Binance.__new__(Binance)
    exchange._pm_user_stream = None
    exchange._pm_user_stream_lock = RLock()
    set_minimal_exchange_cleanup_attrs(exchange)
    exchange._config = {"exchange": {}}
    exchange._api = MagicMock()
    exchange._api.request.side_effect = ccxt.OperationRejected(
        'binance {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action, '
        'request ip: 43.212.29.197"}'
    )

    with pytest.raises(OperationalException, match="Binance PM authentication failed"):
        exchange._papi_request("account", "GET")
