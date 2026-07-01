# ADR-001: <short title for the decision you made>
## Status
Proposed

## Context
What is the system, and what conditions does it actually have to survive? (Duplicate
deliveries, worker restarts, a flaky downstream...). What was wrong with the prototype?

## Decision

### Delivery & consistency semantics
Which did you choose — at-most-once / at-least-once / effectively-once — and **why**?
What does that imply the consumer must guarantee?

We chose at-least-once delivery using Redis Streams consumer groups with XREADGROUP/XACK. This guarantees no message loss on worker restart: unacknowledged messages remain in the pending entries list and are reclaimed via XCLAIM on restart. The tradeoff is that consumers must be idempotent to handle duplicate deliveries safely.

### Idempotency
How do you make re-processing the same order a no-op? What's the key, where does the
state live, and what's the race you had to avoid?

Idempotency is achieved via a Redis Set (processed_orders) keyed by order_id. The worker checks membership before charging and atomically adds the order_id after a successful charge. This guarantees duplicate deliveries (at-least-once semantics, upstream duplicates) result in exactly one charge. The window between ledger update and SADD is a known vulnerability — a crash in that window would cause a double charge on retry. Fully closing this gap requires an idempotent downstream API or transactional outbox, noted for future work

### Failure handling
Retries, backoff, timeouts. How do you tell a *transient* failure from a *permanent*
one? Where do poison messages go? How do you keep one bad message from halting everything?

## Tradeoffs & alternatives

### Build vs adopt: Redis Streams vs Kafka / SQS / managed broker
We used Redis Streams to keep setup light. Would you keep it? At what point (throughput,
durability, team, ordering, retention needs) would you switch, and to what?

### From CI to CD
This repo stops at CI. How would you take it to continuous delivery — image promotion,
environments (dev/stage/prod), rollout strategy (blue-green / canary), and would you run
GitOps (ArgoCD / Flux)? Reason about it; don't build it.

### Scaling to 100×
What breaks first at 100× throughput, and what would you change? Name the next bottleneck.

## Consequences
What's better now. What's still weak / what you'd do next with more time.
