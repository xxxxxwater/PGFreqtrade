from __future__ import annotations

"""Utility helpers for Coinbase Advanced futures strategies in the test branch."""

from typing import Iterable


def normalize_coinbase_futures_pair(pair: str, settle: str = 'USDC') -> str:
    if ':' in pair:
        return pair
    if '/' in pair:
        return f'{pair}:{settle}'
    return pair


def normalize_coinbase_futures_pairs(pairs: Iterable[str], settle: str = 'USDC') -> list[str]:
    return list(dict.fromkeys(normalize_coinbase_futures_pair(p, settle=settle) for p in pairs))


def default_coinbase_btc_reference_pair(settle: str = 'USDC') -> str:
    return f'BTC/USDC:{settle}'
