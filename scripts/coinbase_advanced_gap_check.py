#!/usr/bin/env python3
"""List current known gap areas in the Coinbase Advanced futures test branch."""

GAPS = [
    'Real account validation for live Coinbase Advanced derivatives metadata',
    'Exact order response schema verification for reduceOnly / close semantics',
    'Funding fee and mark/index price validation against live responses',
    'Live liquidation formula verification against Coinbase account outputs',
    'Dedicated integration tests requiring installed ccxt + freqtrade deps',
]


def main() -> None:
    print('== Coinbase Advanced Futures Test Branch Known Gaps ==')
    for idx, gap in enumerate(GAPS, start=1):
        print(f'{idx}. {gap}')


if __name__ == '__main__':
    main()
