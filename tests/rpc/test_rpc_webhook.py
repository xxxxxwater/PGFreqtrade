# pragma pylint: disable=missing-docstring, C0103, protected-access

import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from requests import RequestException

from freqtrade.enums import ExitType, RPCMessageType
from freqtrade.rpc import RPC
from freqtrade.rpc.discord import Discord
from freqtrade.rpc.webhook import Webhook
from tests.conftest import get_patched_freqtradebot, log_has


def get_webhook_dict() -> dict:
    return {
        "enabled": True,
        "url": "https://maker.ifttt.com/trigger/freqtrade_test/with/key/c764udvJ5jfSlswVRukZZ2/",
        "webhookentry": {
            # Intentionally broken, as "entry" should have priority.
            "value1": "Buying {pair55555}",
        },
        "entry": {
            "value1": "Buying {pair}",
            "value2": "limit {limit:8f}",
            "value3": "{stake_amount:8f} {stake_currency}",
            "value4": "leverage {leverage:.1f}",
            "value5": "direction {direction}",
        },
        "webhookentrycancel": {
            "value1": "Cancelling Open Buy Order for {pair}",
            "value2": "limit {limit:8f}",
            "value3": "{stake_amount:8f} {stake_currency}",
            "value4": "leverage {leverage:.1f}",
            "value5": "direction {direction}",
        },
        "webhookentryfill": {
            "value1": "Buy Order for {pair} filled",
            "value2": "at {open_rate:8f}",
            "value3": "{stake_amount:8f} {stake_currency}",
            "value4": "leverage {leverage:.1f}",
            "value5": "direction {direction}",
        },
        "webhookexit": {
            "value1": "Selling {pair}",
            "value2": "limit {limit:8f}",
            "value3": "profit: {profit_amount:8f} {stake_currency} ({profit_ratio})",
        },
        "webhookexitcancel": {
            "value1": "Cancelling Open Sell Order for {pair}",
            "value2": "limit {limit:8f}",
            "value3": "profit: {profit_amount:8f} {stake_currency} ({profit_ratio})",
        },
        "webhookexitfill": {
            "value1": "Sell Order for {pair} filled",
            "value2": "at {close_rate:8f}",
            "value3": "",
        },
        "webhookstatus": {"value1": "Status: {status}", "value2": "", "value3": ""},
    }


def test__init__(mocker, default_conf):
    default_conf["webhook"] = {"enabled": True, "url": "https://DEADBEEF.com"}
    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    assert webhook._config == default_conf


def test_send_msg_webhook(default_conf, mocker):
    default_conf["webhook"] = get_webhook_dict()
    msg_mock = MagicMock()
    mocker.patch("freqtrade.rpc.webhook.Webhook._send_msg", msg_mock)
    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    # Make delivery synchronous so the async sender queue does not race assertions.
    mocker.patch.object(
        Webhook, "_enqueue", lambda self, payload, **_kwargs: self._send_msg(payload)
    )
    # Test buy
    msg_mock = MagicMock()
    mocker.patch("freqtrade.rpc.webhook.Webhook._send_msg", msg_mock)
    msg = {
        "type": RPCMessageType.ENTRY,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "leverage": 1.0,
        "direction": "Long",
        "limit": 0.005,
        "stake_amount": 0.8,
        "stake_amount_fiat": 500,
        "stake_currency": "BTC",
        "fiat_currency": "EUR",
    }
    webhook.send_msg(msg=msg)
    assert msg_mock.call_count == 1
    assert msg_mock.call_args[0][0]["value1"] == default_conf["webhook"]["entry"]["value1"].format(
        **msg
    )
    assert msg_mock.call_args[0][0]["value2"] == default_conf["webhook"]["entry"]["value2"].format(
        **msg
    )
    assert msg_mock.call_args[0][0]["value3"] == default_conf["webhook"]["entry"]["value3"].format(
        **msg
    )
    assert msg_mock.call_args[0][0]["value4"] == default_conf["webhook"]["entry"]["value4"].format(
        **msg
    )
    assert msg_mock.call_args[0][0]["value5"] == default_conf["webhook"]["entry"]["value5"].format(
        **msg
    )
    # Test short
    msg_mock.reset_mock()

    msg = {
        "type": RPCMessageType.ENTRY,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "leverage": 2.0,
        "direction": "Short",
        "limit": 0.005,
        "stake_amount": 0.8,
        "stake_amount_fiat": 500,
        "stake_currency": "BTC",
        "fiat_currency": "EUR",
    }
    webhook.send_msg(msg=msg)
    assert msg_mock.call_count == 1
    assert msg_mock.call_args[0][0]["value1"] == default_conf["webhook"]["entry"]["value1"].format(
        **msg
    )
    assert msg_mock.call_args[0][0]["value2"] == default_conf["webhook"]["entry"]["value2"].format(
        **msg
    )
    assert msg_mock.call_args[0][0]["value3"] == default_conf["webhook"]["entry"]["value3"].format(
        **msg
    )
    assert msg_mock.call_args[0][0]["value4"] == default_conf["webhook"]["entry"]["value4"].format(
        **msg
    )
    assert msg_mock.call_args[0][0]["value5"] == default_conf["webhook"]["entry"]["value5"].format(
        **msg
    )
    # Test buy cancel
    msg_mock.reset_mock()

    msg = {
        "type": RPCMessageType.ENTRY_CANCEL,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "leverage": 1.0,
        "direction": "Long",
        "limit": 0.005,
        "stake_amount": 0.8,
        "stake_amount_fiat": 500,
        "stake_currency": "BTC",
        "fiat_currency": "EUR",
    }
    webhook.send_msg(msg=msg)
    assert msg_mock.call_count == 1
    assert msg_mock.call_args[0][0]["value1"] == default_conf["webhook"]["webhookentrycancel"][
        "value1"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value2"] == default_conf["webhook"]["webhookentrycancel"][
        "value2"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value3"] == default_conf["webhook"]["webhookentrycancel"][
        "value3"
    ].format(**msg)
    # Test short cancel
    msg_mock.reset_mock()

    msg = {
        "type": RPCMessageType.ENTRY_CANCEL,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "leverage": 2.0,
        "direction": "Short",
        "limit": 0.005,
        "stake_amount": 0.8,
        "stake_amount_fiat": 500,
        "stake_currency": "BTC",
        "fiat_currency": "EUR",
    }
    webhook.send_msg(msg=msg)
    assert msg_mock.call_count == 1
    assert msg_mock.call_args[0][0]["value1"] == default_conf["webhook"]["webhookentrycancel"][
        "value1"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value2"] == default_conf["webhook"]["webhookentrycancel"][
        "value2"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value3"] == default_conf["webhook"]["webhookentrycancel"][
        "value3"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value4"] == default_conf["webhook"]["webhookentrycancel"][
        "value4"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value5"] == default_conf["webhook"]["webhookentrycancel"][
        "value5"
    ].format(**msg)
    # Test buy fill
    msg_mock.reset_mock()

    msg = {
        "type": RPCMessageType.ENTRY_FILL,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "leverage": 1.0,
        "direction": "Long",
        "open_rate": 0.005,
        "stake_amount": 0.8,
        "stake_amount_fiat": 500,
        "stake_currency": "BTC",
        "fiat_currency": "EUR",
    }
    webhook.send_msg(msg=msg)
    assert msg_mock.call_count == 1
    assert msg_mock.call_args[0][0]["value1"] == default_conf["webhook"]["webhookentryfill"][
        "value1"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value2"] == default_conf["webhook"]["webhookentryfill"][
        "value2"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value3"] == default_conf["webhook"]["webhookentryfill"][
        "value3"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value4"] == default_conf["webhook"]["webhookentrycancel"][
        "value4"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value5"] == default_conf["webhook"]["webhookentrycancel"][
        "value5"
    ].format(**msg)
    # Test short fill
    msg_mock.reset_mock()

    msg = {
        "type": RPCMessageType.ENTRY_FILL,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "leverage": 2.0,
        "direction": "Short",
        "open_rate": 0.005,
        "stake_amount": 0.8,
        "stake_amount_fiat": 500,
        "stake_currency": "BTC",
        "fiat_currency": "EUR",
    }
    webhook.send_msg(msg=msg)
    assert msg_mock.call_count == 1
    assert msg_mock.call_args[0][0]["value1"] == default_conf["webhook"]["webhookentryfill"][
        "value1"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value2"] == default_conf["webhook"]["webhookentryfill"][
        "value2"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value3"] == default_conf["webhook"]["webhookentryfill"][
        "value3"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value4"] == default_conf["webhook"]["webhookentrycancel"][
        "value4"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value5"] == default_conf["webhook"]["webhookentrycancel"][
        "value5"
    ].format(**msg)
    # Test sell
    msg_mock.reset_mock()

    msg = {
        "type": RPCMessageType.EXIT,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "gain": "profit",
        "limit": 0.005,
        "amount": 0.8,
        "order_type": "limit",
        "open_rate": 0.004,
        "current_rate": 0.005,
        "profit_amount": 0.001,
        "profit_ratio": 0.20,
        "stake_currency": "BTC",
        "sell_reason": ExitType.STOP_LOSS.value,
    }
    webhook.send_msg(msg=msg)
    assert msg_mock.call_count == 1
    assert msg_mock.call_args[0][0]["value1"] == default_conf["webhook"]["webhookexit"][
        "value1"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value2"] == default_conf["webhook"]["webhookexit"][
        "value2"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value3"] == default_conf["webhook"]["webhookexit"][
        "value3"
    ].format(**msg)
    # Test sell cancel
    msg_mock.reset_mock()
    msg = {
        "type": RPCMessageType.EXIT_CANCEL,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "gain": "profit",
        "limit": 0.005,
        "amount": 0.8,
        "order_type": "limit",
        "open_rate": 0.004,
        "current_rate": 0.005,
        "profit_amount": 0.001,
        "profit_ratio": 0.20,
        "stake_currency": "BTC",
        "sell_reason": ExitType.STOP_LOSS.value,
    }
    webhook.send_msg(msg=msg)
    assert msg_mock.call_count == 1
    assert msg_mock.call_args[0][0]["value1"] == default_conf["webhook"]["webhookexitcancel"][
        "value1"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value2"] == default_conf["webhook"]["webhookexitcancel"][
        "value2"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value3"] == default_conf["webhook"]["webhookexitcancel"][
        "value3"
    ].format(**msg)
    # Test Sell fill
    msg_mock.reset_mock()
    msg = {
        "type": RPCMessageType.EXIT_FILL,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "gain": "profit",
        "close_rate": 0.005,
        "amount": 0.8,
        "order_type": "limit",
        "open_rate": 0.004,
        "current_rate": 0.005,
        "profit_amount": 0.001,
        "profit_ratio": 0.20,
        "stake_currency": "BTC",
        "sell_reason": ExitType.STOP_LOSS.value,
    }
    webhook.send_msg(msg=msg)
    assert msg_mock.call_count == 1
    assert msg_mock.call_args[0][0]["value1"] == default_conf["webhook"]["webhookexitfill"][
        "value1"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value2"] == default_conf["webhook"]["webhookexitfill"][
        "value2"
    ].format(**msg)
    assert msg_mock.call_args[0][0]["value3"] == default_conf["webhook"]["webhookexitfill"][
        "value3"
    ].format(**msg)

    for msgtype in [RPCMessageType.STATUS, RPCMessageType.WARNING, RPCMessageType.STARTUP]:
        # Test notification
        msg = {"type": msgtype, "status": "Unfilled sell order for BTC cancelled due to timeout"}
        msg_mock = MagicMock()
        mocker.patch("freqtrade.rpc.webhook.Webhook._send_msg", msg_mock)
        webhook.send_msg(msg)
        assert msg_mock.call_count == 1
        assert msg_mock.call_args[0][0]["value1"] == default_conf["webhook"]["webhookstatus"][
            "value1"
        ].format(**msg)
        assert msg_mock.call_args[0][0]["value2"] == default_conf["webhook"]["webhookstatus"][
            "value2"
        ].format(**msg)
        assert msg_mock.call_args[0][0]["value3"] == default_conf["webhook"]["webhookstatus"][
            "value3"
        ].format(**msg)


def test_exception_send_msg(default_conf, mocker, caplog):
    caplog.set_level(logging.DEBUG)
    default_conf["webhook"] = get_webhook_dict()
    del default_conf["webhook"]["entry"]
    del default_conf["webhook"]["webhookentry"]

    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    webhook.send_msg({"type": RPCMessageType.ENTRY})
    assert log_has(f"Message type '{RPCMessageType.ENTRY}' not configured for webhooks", caplog)

    default_conf["webhook"] = get_webhook_dict()
    default_conf["webhook"]["strategy_msg"] = {"value1": "{DEADBEEF:8f}"}
    msg_mock = MagicMock()
    mocker.patch("freqtrade.rpc.webhook.Webhook._send_msg", msg_mock)
    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    msg = {
        "type": RPCMessageType.STRATEGY_MSG,
        "msg": "hello world",
    }
    webhook.send_msg(msg)
    assert log_has(
        "Problem calling Webhook. Please check your webhook configuration. Exception: 'DEADBEEF'",
        caplog,
    )

    # Test no failure for not implemented but known messagetypes
    for e in RPCMessageType:
        msg = {"type": e, "status": "whatever"}
        webhook.send_msg(msg)

    # Test no failure for not implemented but known messagetypes
    for e in RPCMessageType:
        msg = {"type": e, "status": "whatever"}
        webhook.send_msg(msg)


def test__send_msg(default_conf, mocker, caplog):
    default_conf["webhook"] = get_webhook_dict()
    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    msg = {"value1": "DEADBEEF", "value2": "ALIVEBEEF", "value3": "FREQTRADE"}
    post = MagicMock()
    mocker.patch("freqtrade.rpc.webhook.post", post)
    webhook._send_msg(msg)

    assert post.call_count == 1
    assert post.call_args[1] == {"data": msg, "timeout": 10}
    assert post.call_args[0] == (default_conf["webhook"]["url"],)

    post = MagicMock(side_effect=RequestException)
    mocker.patch("freqtrade.rpc.webhook.post", post)
    webhook._send_msg(msg)
    assert log_has("Could not call webhook url. Exception: ", caplog)


def test__send_msg_with_json_format(default_conf, mocker, caplog):
    default_conf["webhook"] = get_webhook_dict()
    default_conf["webhook"]["format"] = "json"
    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    msg = {"text": "Hello"}
    post = MagicMock()
    mocker.patch("freqtrade.rpc.webhook.post", post)
    webhook._send_msg(msg)

    assert post.call_args[1] == {"json": msg, "timeout": 10}


def test__send_msg_with_raw_format(default_conf, mocker, caplog):
    default_conf["webhook"] = get_webhook_dict()
    default_conf["webhook"]["format"] = "raw"
    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    msg = {"data": "Hello"}
    post = MagicMock()
    mocker.patch("freqtrade.rpc.webhook.post", post)
    webhook._send_msg(msg)

    assert post.call_args[1] == {
        "data": msg["data"],
        "headers": {"Content-Type": "text/plain"},
        "timeout": 10,
    }


def test_send_msg_discord(default_conf, mocker):
    default_conf["discord"] = {"enabled": True, "webhook_url": "https://webhookurl..."}
    msg_mock = MagicMock()
    mocker.patch("freqtrade.rpc.webhook.Webhook._send_msg", msg_mock)
    discord = Discord(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    # Make delivery synchronous so the async sender queue does not race assertions.
    mocker.patch.object(Webhook, "_enqueue", lambda self, payload: self._send_msg(payload))

    msg = {
        "type": RPCMessageType.EXIT_FILL,
        "trade_id": 1,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "direction": "Long",
        "gain": "profit",
        "close_rate": 0.005,
        "amount": 0.8,
        "order_type": "limit",
        "open_date": datetime.now() - timedelta(days=1),
        "close_date": datetime.now(),
        "open_rate": 0.004,
        "current_rate": 0.005,
        "profit_amount": 0.001,
        "profit_ratio": 0.20,
        "stake_currency": "BTC",
        "enter_tag": "enter_tagggg",
        "exit_reason": ExitType.STOP_LOSS.value,
    }
    discord.send_msg(msg=msg)

    assert msg_mock.call_count == 1
    assert "embeds" in msg_mock.call_args_list[0][0][0]
    assert "title" in msg_mock.call_args_list[0][0][0]["embeds"][0]
    assert "color" in msg_mock.call_args_list[0][0][0]["embeds"][0]
    assert "fields" in msg_mock.call_args_list[0][0][0]["embeds"][0]


def test_nested_payload_format(default_conf, mocker):
    webhook_config = {
        "enabled": True,
        "url": "https://example.com",
        "format": "json",
        "status": {
            "msgtype": "text",
            "text": {"content": "Status update: {status}", "contentlist": ["{status}"]},
        },
    }
    default_conf["webhook"] = webhook_config

    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    # Make delivery synchronous so the async sender queue does not race assertions.
    mocker.patch.object(Webhook, "_enqueue", lambda self, payload: self._send_msg(payload))

    msg = {
        "type": RPCMessageType.STATUS,
        "status": "running",
    }

    post = mocker.patch("freqtrade.rpc.webhook.post")
    webhook.send_msg(msg)

    expected_payload = {
        "msgtype": "text",
        "text": {"content": "Status update: running", "contentlist": ["running"]},
    }

    post.assert_called_once_with("https://example.com", json=expected_payload, timeout=10)


def test_webhook_async_delivery_background_thread(default_conf, mocker):
    """
    Webhook send_msg must enqueue and return immediately; the dedicated background
    thread performs the (potentially slow, blocking) HTTP delivery so the trading
    main loop is never blocked.
    """
    import threading

    default_conf["webhook"] = get_webhook_dict()
    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)

    delivered = threading.Event()
    sent_payload: dict = {}

    def fake_send(payload):
        sent_payload.update(payload)
        delivered.set()

    mocker.patch.object(webhook, "_send_msg", side_effect=fake_send)
    msg = {
        "type": RPCMessageType.ENTRY,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "leverage": 1.0,
        "direction": "Long",
        "limit": 0.005,
        "stake_amount": 0.8,
        "stake_amount_fiat": 500,
        "stake_currency": "BTC",
        "fiat_currency": "EUR",
    }

    webhook.send_msg(msg=msg)

    # send_msg returns immediately (it only enqueues); the background thread
    # performs delivery asynchronously.
    assert delivered.wait(timeout=3), "background thread did not deliver the message"
    assert "Buying ETH/BTC" in sent_payload.get("value1", "")
    webhook.cleanup()


def test_webhook_queue_full_counts_dropped(default_conf, mocker):
    """
    When the async queue is full, send_msg must not block; the message is dropped and
    counted, and the drop is exposed through health().
    """
    default_conf["webhook"] = get_webhook_dict()
    default_conf["webhook"]["queue_maxsize"] = 1
    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)

    # Do not consume the queue (block the sender) so we can overflow it.
    mocker.patch.object(webhook, "_sender_loop", MagicMock())
    msg = {
        "type": RPCMessageType.ENTRY,
        "exchange": "Binance",
        "pair": "ETH/BTC",
        "leverage": 1.0,
        "direction": "Long",
        "limit": 0.005,
        "stake_amount": 0.8,
        "stake_amount_fiat": 500,
        "stake_currency": "BTC",
        "fiat_currency": "EUR",
    }
    webhook.send_msg(msg=msg)
    webhook.send_msg(msg=msg)

    assert webhook._queue.qsize() == 1
    assert webhook._dropped == 1
    health = webhook.health()
    assert health["dropped"] == 1
    assert health["queue_size"] == 1
    webhook.cleanup()


def test_webhook_reserves_delivery_capacity_for_safety_alerts(default_conf, mocker):
    """Normal traffic cannot evict warning/exception notifications from the alert lane."""
    default_conf["webhook"] = get_webhook_dict()
    default_conf["webhook"]["queue_maxsize"] = 1
    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    webhook._stop_event.set()
    webhook._sender_thread.join(timeout=2)

    webhook._enqueue({"kind": "normal"})
    webhook._enqueue({"kind": "normal-overflow"})
    webhook._enqueue({"kind": "warning"}, critical=True)

    assert webhook._queue.qsize() == 1
    assert webhook._critical_queue.qsize() == 1
    assert webhook._dropped == 1
    assert webhook.health()["queue_size"] == 2
    webhook.cleanup()


def test_webhook_critical_queue_overflow_attempts_direct_delivery(default_conf, mocker):
    """Safety alerts are sent directly as a last resort instead of being dropped."""
    default_conf["webhook"] = get_webhook_dict()
    default_conf["webhook"]["queue_maxsize"] = 1
    webhook = Webhook(RPC(get_patched_freqtradebot(mocker, default_conf)), default_conf)
    webhook._stop_event.set()
    webhook._sender_thread.join(timeout=2)
    send_mock = mocker.patch.object(webhook, "_send_msg")

    for index in range(webhook._critical_queue_maxsize):
        webhook._enqueue({"kind": f"warning-{index}"}, critical=True)
    webhook._enqueue({"kind": "emergency"}, critical=True)

    send_mock.assert_called_once_with({"kind": "emergency"})
    assert webhook._dropped == 0
    webhook.cleanup()
