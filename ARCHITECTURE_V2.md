# refactor-v2 architecture

## Latency contract

The autobuy hot path keeps the original low-latency shape:

1. Discover a fresh item.
2. Deduplicate by item key.
3. Apply cheap synchronous filters/decision logic.
4. Enqueue without waiting for Telegram notifications.
5. Dispatch purchase work concurrently.
6. Fire the configured first wave in parallel.
7. Cancel losing requests as soon as a terminal purchase result is known.
8. Persist/notify after the critical purchase decision.

The no-account-check path must not acquire a mandatory account-validation step and must not add fixed sleeps.

## New boundaries

- `domain/` — immutable market and purchase models plus pure decision logic.
- `filters/` — cheap synchronous filter evaluation.
- `purchase/` — purchase policy, runner seam, and idempotency guard.
- `market/` — market-specific decisions and adaptive rate-limit state.
- `buyer/` — concurrent per-user dispatch queue.
- `storage/` — repository protocols for persistent state.
- `runtime/` — in-memory user runtime state.
- `metrics/` — small latency/event primitives.

## Rate limiting

`market/rate_limit.py` is intentionally observational until a bucket is explicitly exhausted by server headers. Responses with remaining quota do not sleep. `X-RateLimit-Reset` is treated as an absolute timestamp; unknown/malformed values are ignored rather than guessed.

## Migration rule

The legacy `main.py` remains the compatibility shell while the boundaries are introduced incrementally. Do not rewrite the buyer or discovery loop wholesale without a measured regression comparison against the current hot path.
