from scripts.binance_pm_readonly_probe import build_summary, nonzero_balances, nonzero_positions


def test_nonzero_balances_filters_empty_balances():
    balances = [
        {
            "asset": "USDT",
            "totalWalletBalance": "0",
            "crossMarginFree": "0",
            "crossMarginLocked": "0",
        },
        {
            "asset": "BTC",
            "totalWalletBalance": "0.1",
            "crossMarginFree": "0.09",
            "crossMarginLocked": "0.01",
        },
    ]

    result = nonzero_balances(balances)

    assert result == [
        {
            "asset": "BTC",
            "totalWalletBalance": "0.1",
            "crossMarginFree": "0.09",
            "crossMarginLocked": "0.01",
        }
    ]


def test_nonzero_positions_filters_empty_positions():
    positions = [
        {"symbol": "BTCUSDT", "positionAmt": "0", "initialMargin": "0"},
        {
            "symbol": "ETHUSDT",
            "positionSide": "BOTH",
            "positionAmt": "0.5",
            "entryPrice": "3000",
            "markPrice": "3010",
            "unRealizedProfit": "5",
            "initialMargin": "100",
            "leverage": "5",
        },
    ]

    result = nonzero_positions(positions)

    assert result == [
        {
            "symbol": "ETHUSDT",
            "positionSide": "BOTH",
            "positionAmt": "0.5",
            "entryPrice": "3000",
            "markPrice": "3010",
            "unRealizedProfit": "5",
            "initialMargin": "100",
            "leverage": "5",
        }
    ]


def test_build_summary_uses_pm_account_fields():
    account = {
        "accountStatus": "NORMAL",
        "uniMMR": "2.5",
        "accountEquity": "10000",
        "accountInitialMargin": "500",
        "accountMaintMargin": "100",
    }

    result = build_summary(account, [], [])

    assert result["account"] == {
        "accountStatus": "NORMAL",
        "uniMMR": "2.5",
        "accountEquity": "10000",
        "accountInitialMargin": "500",
        "accountMaintMargin": "100",
    }
    assert result["counts"] == {
        "balances": 0,
        "nonzero_balances": 0,
        "um_positions": 0,
        "nonzero_um_positions": 0,
    }
