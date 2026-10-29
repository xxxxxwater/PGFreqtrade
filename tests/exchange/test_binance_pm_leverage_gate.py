"""Coverage for the live PM 2x leverage / aggregate-exposure gate."""

from unittest.mock import MagicMock

import pytest

from freqtrade.exceptions import PMRiskLimitExceeded
from tests.exchange.test_binance_pm import get_patched_pm_exchange


PAIR = "ETH/USDT:USDT"
SYMBOL = "ETHUSDT"
# 0-10k notional allows up to 5x; above that only 1x.
TIERS = [
    {"minNotional": 0.0, "maxNotional": 10_000.0, "maxLeverage": 5.0},
    {"minNotional": 10_000.0, "maxNotional": 1e9, "maxLeverage": 1.0},
]


def make_gate(mocker, default_conf_usdt, positions=None, orders=None):
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._verified_pm_tiers = MagicMock(return_value=TIERS)
    exchange._contracts_to_amount = MagicMock(side_effect=lambda pair, qty: qty)
    responses = {"um/positionRisk": positions or [], "um/openOrders": orders or []}
    exchange._papi_request = MagicMock(side_effect=lambda path, method, params: responses[path])
    return exchange


def test_leverage_gate_allows_2x_within_bracket(mocker, default_conf_usdt):
    exchange = make_gate(mocker, default_conf_usdt)

    exchange.assert_pm_leverage_allows_order(PAIR, 10, 100, 2)

    exchange._verified_pm_tiers.assert_called_once_with(PAIR, force=True)
    exchange._papi_request.assert_any_call("um/positionRisk", "GET", {"symbol": SYMBOL})
    exchange._papi_request.assert_any_call("um/openOrders", "GET", {"symbol": SYMBOL})


@pytest.mark.parametrize("leverage", [1, 3, 5])
def test_leverage_gate_rejects_anything_but_2x(mocker, default_conf_usdt, leverage):
    exchange = make_gate(mocker, default_conf_usdt)

    with pytest.raises(PMRiskLimitExceeded, match="2x/aggregate exposure not verified") as exc:
        exchange.assert_pm_leverage_allows_order(PAIR, 10, 100, leverage)
    assert "requires exactly 2x" in str(exc.value.__cause__)
    exchange._papi_request.assert_not_called()


def test_leverage_gate_counts_position_and_open_entry_orders(mocker, default_conf_usdt):
    # new 1000 + position 5000 (at mark) + open entry 4500 = 10500 -> 1x bracket
    exchange = make_gate(
        mocker,
        default_conf_usdt,
        positions=[{"symbol": SYMBOL, "positionAmt": "50", "leverage": "2", "markPrice": "100"}],
        orders=[
            {
                "symbol": SYMBOL,
                "reduceOnly": False,
                "origQty": "50",
                "executedQty": "5",
                "price": "100",
            },
        ],
    )

    with pytest.raises(PMRiskLimitExceeded) as exc:
        exchange.assert_pm_leverage_allows_order(PAIR, 10, 100, 2)
    assert "projected notional 10500.00 permits only 1.0x" in str(exc.value.__cause__)


def test_leverage_gate_ignores_reduce_only_orders(mocker, default_conf_usdt):
    exchange = make_gate(
        mocker,
        default_conf_usdt,
        orders=[
            {
                "symbol": SYMBOL,
                "reduceOnly": True,
                "origQty": "500",
                "executedQty": "0",
                "price": "100",
            },
        ],
    )

    exchange.assert_pm_leverage_allows_order(PAIR, 10, 100, 2)


def test_leverage_gate_refuses_to_add_to_non_2x_position(mocker, default_conf_usdt):
    exchange = make_gate(
        mocker,
        default_conf_usdt,
        positions=[{"symbol": SYMBOL, "positionAmt": "-1", "leverage": "1", "markPrice": "100"}],
    )

    with pytest.raises(PMRiskLimitExceeded) as exc:
        exchange.assert_pm_leverage_allows_order(PAIR, 10, 100, 2)
    assert "differs from 2x" in str(exc.value.__cause__)


@pytest.mark.parametrize("bad", [{"code": -1}, None])
def test_leverage_gate_fails_closed_on_unverified_responses(mocker, default_conf_usdt, bad):
    exchange = make_gate(mocker, default_conf_usdt)
    exchange._papi_request = MagicMock(return_value=bad)

    with pytest.raises(PMRiskLimitExceeded) as exc:
        exchange.assert_pm_leverage_allows_order(PAIR, 10, 100, 2)
    assert "unverified positions/open orders response" in str(exc.value.__cause__)


def test_leverage_gate_rejects_mismatched_symbol(mocker, default_conf_usdt):
    exchange = make_gate(
        mocker,
        default_conf_usdt,
        positions=[{"symbol": "BTCUSDT", "positionAmt": "0", "leverage": "2", "markPrice": "1"}],
    )

    with pytest.raises(PMRiskLimitExceeded) as exc:
        exchange.assert_pm_leverage_allows_order(PAIR, 10, 100, 2)
    assert "symbol mismatch" in str(exc.value.__cause__)
