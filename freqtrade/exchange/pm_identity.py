"""Unambiguous instrument identity at Binance Portfolio Margin boundaries.

Freqtrade's persisted identity remains CCXT's settled contract symbol (for
example ``DASH/USDT:USDT``).  Exchange IDs such as ``DASHUSDT`` are not unique
across spot and derivatives.  Resolve them only in a known PM namespace using
loaded market metadata, never by the first matching ID or string concatenation.
"""

from collections.abc import Mapping
from typing import Any

from freqtrade.exceptions import OperationalException


def canonical_pm_pair(
    markets: Mapping[str, Any],
    instrument: str,
    *,
    namespace: str = "um",
    allow_legacy_alias: bool = True,
) -> str:
    """Resolve a PM raw ID, settled pair, or unambiguous legacy pair.

    ``namespace`` must come from the PM endpoint/event, not from the symbol's
    spelling. Unknown/ambiguous instruments fail closed. A legacy ``BASE/QUOTE``
    is accepted only when one eligible contract exists in that namespace; an
    explicit settle suffix is never stripped or silently changed. Inactive
    loaded markets remain eligible so historical/terminal events can be owned.
    """
    if namespace not in {"um", "cm"}:
        raise OperationalException(f"Unsupported Binance PM instrument namespace {namespace!r}.")
    if not isinstance(instrument, str) or not instrument:
        raise OperationalException("Binance PM instrument identity is missing.")

    matches: set[str] = set()
    # Persisted settled keys are exact lookups, avoiding an all-markets scan for
    # every Order/Trade examined during recovery. Only wire IDs/legacy aliases
    # need collision detection across the market catalogue.
    candidates = ((instrument, markets.get(instrument)),) if ":" in instrument else markets.items()
    for pair, market in candidates:
        if not isinstance(market, Mapping) or not isinstance(pair, str):
            continue
        # Contract and settle metadata are necessary even when an alias happens
        # to match. Minimal metadata may omit contract/linear flags, but must
        # positively identify inverse mode and a settled contract symbol.
        settle = market.get("settle")
        if (
            ":" not in pair
            or "/" not in pair
            or not isinstance(settle, str)
            or not settle
            or pair.split(":", 1)[1].split("-", 1)[0] != settle
            or market.get("symbol", pair) != pair
            or market.get("spot") is True
            or market.get("contract") is False
            or market.get("option") is True
        ):
            continue
        if namespace == "um":
            if (
                settle not in {"USDT", "USDC"}
                or market.get("inverse") is not False
                or market.get("linear") is False
            ):
                continue
        elif market.get("inverse") is not True or market.get("linear") is True:
            continue

        is_legacy = allow_legacy_alias and ":" not in instrument and "/" in instrument
        if (
            instrument == pair
            or instrument == market.get("id")
            or (is_legacy and instrument == pair.split(":", 1)[0])
        ):
            matches.add(pair)

    if len(matches) == 1:
        return next(iter(matches))
    reason = "ambiguous" if matches else "unknown or not a settled contract"
    raise OperationalException(
        f"Binance PM {namespace.upper()} instrument {instrument!r} is {reason}; "
        "refusing to guess its ownership."
    )
