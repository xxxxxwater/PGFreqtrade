"""Strict parsing of account-specific Binance UM leverage brackets."""
import math


def number(value, *, minimum=0.0):
    result = float(value)
    if not math.isfinite(result) or result < minimum:
        raise ValueError("invalid non-finite or negative bracket/exposure value")
    return result


def parse_brackets(rows, markets):
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list) or not rows:
        raise ValueError("empty or invalid PM bracket response")
    symbols = {m['id']: pair for pair, m in markets.items()
               if m.get('linear') and m.get('swap') and m.get('settle') == 'USDT'}
    result = {}
    for row in rows:
        symbol = row['symbol']
        if symbol not in symbols:
            continue
        pair = symbols[symbol]
        if pair in result:
            raise ValueError(f"duplicate PM brackets: {symbol}")
        # Account-specific notional coefficient scales the bracket schedule,
        # including cum to preserve maintenance-margin continuity.
        coef = number(row.get('notionalCoef', 1), minimum=1e-12)
        tiers = []
        previous_cap = 0.0
        previous_leverage = float('inf')
        for bracket in row['brackets']:
            floor = number(bracket['notionalFloor']) * coef
            cap = number(bracket['notionalCap']) * coef
            leverage = number(bracket['initialLeverage'], minimum=1)
            mmr = number(bracket['maintMarginRatio'])
            cum = number(bracket['cum']) * coef
            if floor != previous_cap or cap <= floor or leverage > previous_leverage or mmr > 1:
                raise ValueError(f"incomplete/inconsistent PM brackets: {symbol}")
            tiers.append({'minNotional': floor, 'maxNotional': cap,
                          'maxLeverage': leverage, 'maintenanceMarginRate': mmr,
                          'info': {'cum': cum}})
            previous_cap, previous_leverage = cap, leverage
        if not tiers:
            raise ValueError(f"missing PM brackets: {symbol}")
        result[pair] = tiers
    if not result:
        raise ValueError('no matching USDT perpetual PM brackets')
    return result


def allowed_leverage(tiers, notional):
    notional = number(notional)
    for tier in tiers:
        # At an exact boundary use the next (more conservative) bracket.
        if tier['minNotional'] <= notional < tier['maxNotional']:
            return tier['maxLeverage']
    raise ValueError('projected notional outside verified PM brackets')
