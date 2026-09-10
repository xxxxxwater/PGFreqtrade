# PM full-loop release review — 2026-09-10

Base: 1489907. Combines the operator's review/pm-full-loop-hardening-20260910
work with the network/protection review. No strategy, ROI, DCA, liquidity,
stake, risk-ceiling or memory configuration is changed in this release.

## Behavior

- symbolConfig transient failure uses only a previously account-verified universe
  for continuity; its degraded health independently blocks exposure increases.
  No successful snapshot means TemporaryError, never public permission guessing.
  Signed exception URLs are not exposed in health status.
- Market read failures are caught narrowly inside the read stages, not by
  broadly swallowing DependencyException. Bounded cooperative retries preserve
  order management, protective checks and exits. A complete fresh analysis
  batch clears the cycle hold, not ownership/risk/operator gates.
  Budget-disabled PM execution has the same failure behavior.
- Deterministic PM notional-cap rejection rejects only the relevant order.
  It does not STOP the worker. Existing thresholds remain unchanged.
- Fresh ACK verification grace blocks new exposure and does NOT claim protection.
  Grace requires exact origin Trade, client ID, canonical instrument, exit side,
  reduceOnly and adequate request quantity. Its clock uses the earliest durable
  create/dispatch/ACK timestamp: subsequent LINK/reconcile cannot extend it.
- Every new stop candidate is preceded by lookup of existing durable local stop
  lifecycles. An unverified candidate is lookup-only, including after DB reload.
  The preflight is not a quantity-coverage verdict: known undersized old stops
  can still be resized after DCA. No absence-to-canceled conversion.
- Algo open/history lookup matches either exact algoId or clientAlgoId and keeps
  the local lookup key stable. Invalid response shapes are temporary uncertainty,
  not evidence of an empty history. Direct/history triggered children both use
  the normal child-fill resolver.
- Telegram rebuild wording refers to durable business state, not restart.
  Long incident display IDs are compact hashes; dedupe retains full identity.
- /pm_status exposes whitelist health and verification-pending state.
  Entry logs identify the strategy callback rather than a manual user rejection.

## Evidence and boundaries

Combined source regression with an isolated PostgreSQL 17:
911 passed, 0 failed, 0 skipped (2026-09-10). Formatting-only cleanup followed;
the final image must repeat acceptance before production replacement.

Tests cover metadata disconnect/recovery, read-retry bounds, permanent-error
propagation, exit/protection continuity, no duplicate pending stop after reload,
DCA undersized-old-stop handling, dual protection, trigger/partial lifecycle,
grace identity/quantity/timestamp counterexamples, durable Telegram, dispatch
crash windows and PostgreSQL locks.

The 12-second budget is cooperative, not a preemptive HTTP deadline. Neither
network recovery nor a successful trade guarantees successful risk reduction
during an exchange-wide outage. A persistent unresolvable stop lifecycle remains
fail-closed and requires recovery evidence; no timer fabricates a terminal state.
XAG success does not validate SKHYNIX-specific permissions.

Release procedure: exact commit image -> image-local regression with scratch
PG17 -> core SHA comparison -> database snapshot -> single bot recreation ->
state/user-stream/heartbeat/gates/backup/memory verification. Preserve manual
positions and operator STOPPED/PAUSED; never force RUNNING merely to pass checks.
