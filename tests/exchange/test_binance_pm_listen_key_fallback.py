"""Regression coverage for unsigned Binance PM listenKey fallback requests."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import ccxt
import pytest

from freqtrade.exceptions import OperationalException
from freqtrade.exchange.binance import Binance
from freqtrade.persistence import init_db
from tests.conftest import get_patched_exchange


@pytest.fixture(autouse=True)
def pm_intent_db(default_conf_usdt):
    """Bind the main DB because constructing the PM exchange uses its models."""
    init_db(default_conf_usdt["db_url"])


def get_patched_pm_exchange(mocker, default_conf_usdt):
    conf = default_conf_usdt.copy()
    conf["dry_run"] = False
    conf["trading_mode"] = "futures"
    conf["margin_mode"] = "cross"
    conf["stake_currency"] = "USDT"
    conf["exchange"] = conf["exchange"].copy()
    conf["exchange"].update(
        {
            "name": "binance",
            "key": "dummy_key",
            "secret": "dummy_secret",
            "pair_whitelist": ["ETH/USDT:USDT"],
            "portfolio_margin": True,
            "portfolio_margin_risk": {"user_stream_enabled": False},
        }
    )
    return get_patched_exchange(
        mocker,
        conf,
        api_mock=MagicMock(),
        exchange="binance",
        mock_markets=True,
    )


class _RawResponse:
    def __init__(self, payload: dict | None) -> None:
        self.headers = {"X-MBX-USED-WEIGHT-1M": "7"}
        self._payload = b"" if payload is None else json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._payload


class _ResponseContext:
    def __init__(self, response: _RawResponse) -> None:
        self.response = response

    def __enter__(self) -> _RawResponse:
        return self.response

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None


@pytest.mark.parametrize(
    ("method", "payload", "expected"),
    [
        ("POST", {"listenKey": "created-listen-key"}, {"listenKey": "created-listen-key"}),
        ("PUT", None, {}),
        ("DELETE", None, {}),
    ],
)
def test_pm_listen_key_not_supported_uses_unsigned_header_only_raw_fallback(
    default_conf_usdt, mocker, method, payload, expected
):
    """CCXT NotSupported must use the documented header-only PM stream route."""
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._api.apiKey = "header-only-key"
    # This endpoint must work without a secret: its contract is API-key-only.
    exchange._api.secret = ""
    exchange._api.request.side_effect = ccxt.NotSupported("PAPI listenKey unavailable")
    urlopen = mocker.patch(
        "freqtrade.exchange.binance.urllib.request.urlopen",
        return_value=_ResponseContext(_RawResponse(payload)),
    )

    assert exchange._papi_request("/papi/v1/listenKey", method) == expected

    exchange._api.request.assert_called_once_with("listenKey", "papi", method, {})
    request = urlopen.call_args.args[0]
    assert request.method == method
    assert request.full_url == "https://papi.binance.com/papi/v1/listenKey"
    assert request.data is None
    assert "?" not in request.full_url
    headers = {name.lower(): value for name, value in request.header_items()}
    assert headers == {"x-mbx-apikey": "header-only-key"}


def test_pm_listen_key_rejects_parameters_before_ccxt_or_raw_transport(default_conf_usdt, mocker):
    """No future caller can accidentally reintroduce a listenKey query parameter."""
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)

    with pytest.raises(OperationalException, match="do not accept request parameters"):
        exchange._papi_request("listenKey", "PUT", {"listenKey": "do-not-send"})

    exchange._api.request.assert_not_called()


def test_pm_listen_key_raw_fallback_rejects_non_lifecycle_method(default_conf_usdt, mocker):
    """The unsigned helper cannot silently be reused for arbitrary PAPI calls."""
    exchange = get_patched_pm_exchange(mocker, default_conf_usdt)
    exchange._api.apiKey = "header-only-key"

    with pytest.raises(OperationalException, match="expected POST, PUT, or DELETE"):
        exchange._raw_papi_listen_key_request("GET")
