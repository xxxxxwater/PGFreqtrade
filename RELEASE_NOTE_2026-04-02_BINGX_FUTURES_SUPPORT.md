# Release Note - 2026-04-02 - BingX Futures Support

## Scope

This branch adds first-class BingX perpetual futures support to the PGFreqtrade test copy under
`/data/test_futures_bingx/PGFreqtrade`.

## What Changed

- Enabled BingX futures trading mode support in `freqtrade/exchange/bingx.py`
- Added cross and isolated margin compatibility
- Added one-way position mode initialization for futures startup
- Added BingX-specific leverage handling using `side=BOTH`
- Added on-exchange futures stoploss support with mark/last trigger price mapping
- Added regression tests for BingX futures initialization, leverage prep, and stoploss behavior
- Updated exchange documentation to list BingX futures as supported
- Added user-data example configs for BingX futures dry-run and live usage

## Config Files Added

- `user_data/config.bingx_futures.example.json`
- `user_data/config.bingx_futures.live-template.json`

## Validation Status

- Python syntax validation completed successfully
- Pytest execution was not completed in this environment because `pytest` is not installed

## Notes

- The provided BingX futures configs are sanitized templates and do not contain live credentials
- Futures support is designed around BingX one-way mode to match Freqtrade net-position behavior
