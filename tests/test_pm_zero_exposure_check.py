"""
Tests for scripts/pm_zero_exposure_check.py: an unverifiable channel must
produce exit code 2 (unknown), never a false "zero exposure".
"""

import json
import urllib.error
from unittest.mock import MagicMock, patch

import scripts.pm_zero_exposure_check as zcheck


def _run(positions, open_orders, algo_orders, algo_error=None):
    fake_responses = {
        "/papi/v1/um/positionRisk": positions,
        "/papi/v1/um/openOrders": open_orders,
    }

    def signed_get(key, secret, path):
        if path == "/papi/v1/um/algo/openAlgoOrders" and algo_error is not None:
            raise algo_error
        if path == "/papi/v1/um/algo/openAlgoOrders":
            return algo_orders
        return fake_responses[path]

    with patch.object(zcheck, "load_credentials", return_value=("k", "s")), patch.object(
        zcheck, "signed_get", side_effect=signed_get
    ):
        return zcheck.main()


def test_all_zero_returns_0():
    assert _run([], [], []) == 0


def test_nonzero_position_returns_3():
    positions = [{"symbol": "BTCUSDT", "positionAmt": "1"}]
    assert _run(positions, [], []) == 3


def test_algo_endpoint_failure_returns_2_not_0():
    """Algo channel unknown must NEVER count as zero exposure."""
    algo_error = urllib.error.HTTPError("url", 429, "rate limited", None, None)
    assert _run([], [], [], algo_error=algo_error) == 2


def test_algo_non_list_returns_2():
    """A non-list algo response is unknown, not zero."""
    assert _run([], [], {"error": "not a list"}) == 2


def test_open_orders_non_list_returns_2():
    assert _run([], {"error": "not a list"}, []) == 2
