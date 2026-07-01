# Test & Stress Validation Notes

## Results

| Scenario | Configuration | Messages | Result |
|----------|--------------|----------|--------|
| E1 — Happy path | 1 worker | 50 unique + 10 duplicates | ✅ 5000/5000 |
| E3 — Worker restart | 1 worker, restart mid-processing | 50 unique + 10 duplicates | ✅ 5000/5000 |
| E4 — Multi-worker | 3 workers, restart all | 50 unique + 10 duplicates | ✅ 5000/5000 |
| Stress — 10× load | 3 workers, restart all | 500 unique + 100 duplicates | ⚠️ Retry latency saturation |

## Scenarios

### E1 — Happy path baseline

Standard load with no restarts. All 50 unique orders are processed with
idempotency correctly skipping the 10 duplicates. ~30% of payment calls require
one or two retries due to the flaky downstream. Converges in under 25 seconds.

### E3 — Single worker restart

Proves resilience: load is sent, the worker is killed mid-processing, and the
new worker reclaims pending messages via `XCLAIM` and reprocesses them. Orders
that were in-flight at shutdown are preserved (left pending, not dead-lettered)
and recovered. No messages lost.

### E4 — Multi-worker restart

Same resilience test with 3 concurrent workers scaled horizontally. On restart,
all three workers race to reclaim pending entries from the consumer group.
A random stagger (0–1.5s) in `claim_pending()` combined with `DELETE` +
`SETNX` on idempotency keys spreads out the recovery to avoid double-claim
collisions. Under 60 messages the stagger works reliably.

### Stress — 10× load (600 messages)

With 500 unique orders, 100 duplicates, FAILURE_RATE=0.3, and 5-retry backoff
(up to 15s per order), the cumulative retry latency across 3 workers saturates
the pipeline under the default 300-second check timeout. The system is
**correct** (no overcharge, no data loss) but does not converge within the
time budget.

## Considerations for production

- **Retry budget**: a per-message retry cap of 15s is acceptable for 50 orders
  but becomes a bottleneck at scale. Production would add a **circuit breaker**
  on the payments service to stop retries after N consecutive failures,
  preventing cascade saturation.
- **Pending recovery**: multi-worker recovery uses DELETE + SETNX with a random
  stagger. This is not fully atomic across workers. Production would replace it
  with a **Redis Lua script** for a true compare-and-swap claim.
- **Stream trimming**: Redis Streams grow unbounded. `MAXLEN` should be set
  to cap stream size at ~1000 entries in production to bound memory usage.
