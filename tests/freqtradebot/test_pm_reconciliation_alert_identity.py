from datetime import UTC, datetime, timedelta

from freqtrade.persistence import PMOrderIntent
from tests.freqtradebot.test_pm_recovery import make_pm_bot


def _conf(default_conf_usdt):
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
    conf["exchange"]["portfolio_margin_risk"] = {
        "user_stream_enabled": False,
        "reconciliation_warning_reminder_minutes": 30,
    }
    return conf


def _intent(client_id, state="PREPARED"):
    row = PMOrderIntent(
        client_id=client_id,
        kind="order",
        pair="ETH/USDT:USDT",
        side="buy",
        order_type="market",
        amount=1.0,
        reduce_only=False,
        state=state,
    )
    PMOrderIntent.session.add(row)
    PMOrderIntent.session.commit()
    return row


def test_reconciliation_signature_changes_when_client_id_changes_at_same_count(
    mocker, default_conf_usdt
):
    bot = make_pm_bot(mocker, _conf(default_conf_usdt))
    a = _intent("intent-A")
    sig_a = bot._pm_reconciliation_incident_signature({"errors": []})
    PMOrderIntent.session.delete(a)
    PMOrderIntent.session.commit()
    _intent("intent-B")
    sig_b = bot._pm_reconciliation_incident_signature({"errors": []})

    assert sig_a != sig_b


def test_reconciliation_signature_changes_on_intent_state_transition(mocker, default_conf_usdt):
    bot = make_pm_bot(mocker, _conf(default_conf_usdt))
    row = _intent("intent-A", "PREPARED")
    sig_prepared = bot._pm_reconciliation_incident_signature({"errors": []})
    row.state = "UNKNOWN"
    PMOrderIntent.session.commit()
    sig_unknown = bot._pm_reconciliation_incident_signature({"errors": []})

    assert sig_prepared != sig_unknown


def test_identical_incident_gets_low_frequency_reminder(mocker, default_conf_usdt):
    bot = make_pm_bot(mocker, _conf(default_conf_usdt))
    _intent("intent-A")
    sig = bot._pm_reconciliation_incident_signature({"errors": []})
    send, reminder, interval = bot._pm_reconciliation_warning_due(sig)
    assert send is True and reminder is False and interval == 30

    send, reminder, _ = bot._pm_reconciliation_warning_due(sig)
    assert send is False and reminder is False

    bot._pm_last_reconciliation_warning_at = datetime.now(UTC) - timedelta(minutes=31)
    send, reminder, _ = bot._pm_reconciliation_warning_due(sig)
    assert send is True and reminder is True
