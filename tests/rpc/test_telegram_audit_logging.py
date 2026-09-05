# pragma pylint: disable=missing-docstring, protected-access, unused-argument, invalid-name

"""
Audit logging for inbound Telegram interactions.

Production requirement: trading-class commands (/fx, /pm_close, ...) and the
force-exit inline buttons must be traceable to a user and timestamp in the
bot log, so any operator action can be reconstructed afterwards.
"""

import asyncio
import logging
from unittest.mock import MagicMock

import pytest
from telegram import CallbackQuery, Chat, Message, MessageEntity, Update, User

from freqtrade.rpc import RPC
from freqtrade.rpc.telegram import Telegram
from freqtrade.util.datetime_helpers import dt_now
from tests.conftest import get_patched_freqtradebot


@pytest.fixture(autouse=True)
def mock_exchange_loop(mocker):
    mocker.patch("freqtrade.exchange.exchange.Exchange._init_async_loop")


def make_telegram(mocker, default_conf):
    mocker.patch.multiple(
        "freqtrade.rpc.telegram.Telegram",
        _init=MagicMock(),
        _send_msg=MagicMock(),
        _start_thread=MagicMock(),
    )
    ftbot = get_patched_freqtradebot(mocker, default_conf)
    telegram = Telegram(RPC(ftbot), default_conf)
    telegram._loop = MagicMock()
    return telegram


def command_update(text: str, user_id: int = 5432, username: str = "trader") -> Update:
    entities = None
    if text.startswith("/"):
        entities = [MessageEntity("bot_command", 0, len(text.split()[0]))]
    message = Message(
        0,
        dt_now(),
        Chat(1235, 0),
        from_user=User(user_id, "test", is_bot=False, username=username),
        text=text,
        entities=entities,
    )
    return Update(0, message=message)


async def run_handler(telegram, update, context=None):
    if context is None:
        context = MagicMock()
        context.user_data = {}
    return await telegram._log_inbound_update(update, context)


def test_force_exit_command_is_audit_logged(mocker, default_conf, caplog):
    telegram = make_telegram(mocker, default_conf)
    update = command_update("/fx XRP/USDT:USDT")
    with caplog.at_level(logging.INFO):
        asyncio.run(run_handler(telegram, update))
    assert "Telegram inbound command" in caplog.text
    assert "chat_id=1235" in caplog.text
    assert "user=@trader(5432)" in caplog.text
    assert "/fx XRP/USDT:USDT" in caplog.text


def test_pm_close_confirm_is_audit_logged(mocker, default_conf, caplog):
    telegram = make_telegram(mocker, default_conf)
    update = command_update("/pm_close all CONFIRM")
    with caplog.at_level(logging.INFO):
        asyncio.run(run_handler(telegram, update))
    assert "Telegram inbound command" in caplog.text
    assert "/pm_close all CONFIRM" in caplog.text


def test_plain_message_logged_without_command_tag(mocker, default_conf, caplog):
    telegram = make_telegram(mocker, default_conf)
    update = command_update("hello there")
    with caplog.at_level(logging.INFO):
        asyncio.run(run_handler(telegram, update))
    assert "Telegram inbound message" in caplog.text
    assert "hello there" in caplog.text


def test_force_exit_inline_callback_is_audit_logged(mocker, default_conf, caplog):
    telegram = make_telegram(mocker, default_conf)
    callback = CallbackQuery(
        id="1",
        from_user=User(5432, "test", is_bot=False, username="trader"),
        chat_instance="ci-1",
        data="force_exit__1",
    )
    update = Update(0, callback_query=callback)
    with caplog.at_level(logging.INFO):
        asyncio.run(run_handler(telegram, update))
    assert "Telegram inbound callback" in caplog.text
    assert "force_exit__1" in caplog.text
    assert "user=@trader(5432)" in caplog.text


def test_user_without_username_uses_full_name(mocker, default_conf, caplog):
    telegram = make_telegram(mocker, default_conf)
    update = command_update("/start", username=None)
    with caplog.at_level(logging.INFO):
        asyncio.run(run_handler(telegram, update))
    assert "Telegram inbound command" in caplog.text
    assert "user=test(5432)" in caplog.text


def test_non_text_non_callback_update_is_silent(mocker, default_conf, caplog):
    telegram = make_telegram(mocker, default_conf)
    update = Update(0)
    with caplog.at_level(logging.INFO):
        asyncio.run(run_handler(telegram, update))
    assert "Telegram inbound" not in caplog.text


def test_op_id_is_logged_and_stashed_on_context(mocker, default_conf, caplog):
    import re as _re

    telegram = make_telegram(mocker, default_conf)
    context = MagicMock()
    context.user_data = {}
    update = command_update("/fx XRP/USDT:USDT")
    with caplog.at_level(logging.INFO):
        asyncio.run(run_handler(telegram, update, context))
    match = _re.search(r"op=([0-9a-f]{8})", caplog.text)
    assert match, f"op id missing in log: {caplog.text}"
    assert context.user_data["audit_op_id"] == match.group(1)


def test_long_message_is_truncated_and_sanitized(mocker, default_conf, caplog):
    telegram = make_telegram(mocker, default_conf)
    update = command_update("A" * 300 + "\nsecond line with secret=abc")
    with caplog.at_level(logging.INFO):
        asyncio.run(run_handler(telegram, update))
    line = [l for l in caplog.text.splitlines() if "Telegram inbound" in l][0]
    assert "..." in line
    assert "second line" not in line
    assert "secret=abc" not in line
