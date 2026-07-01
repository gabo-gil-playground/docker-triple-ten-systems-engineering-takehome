# ADR-001: At-least-once order processing with idempotent charging on Redis Streams
## Status
Proposed

## Context
What is the system, and what conditions does it actually have to survive? (Duplicate
deliveries, worker restarts, a flaky downstream...). What was wrong with the prototype?

The prototype order-to-payment pipeline used bare XREAD without consumer groups, no error handling, and no idempotency. Under real conditions — duplicate deliveries from the upstream producer, worker restarts, and a flaky third-party payments service (30% 500 errors, 10% hangs) — the prototype silently dropped orders and double-charged customers. The system needed correctness (every customer charged exactly what they ordered, no more, no less) and resilience (surviving worker restarts and downstream failures) without modifying the payments service.

## Decision

### Delivery & consistency semantics
Which did you choose — at-most-once / at-least-once / effectively-once — and **why**?
What does that imply the consumer must guarantee?

We chose at-least-once delivery using Redis Streams consumer groups with XREADGROUP/XACK. This guarantees no message loss on worker restart: unacknowledged messages remain in the pending entries list and are reclaimed via XCLAIM on restart. The tradeoff is that consumers must be idempotent to handle duplicate deliveries safely.

### Idempotency
How do you make re-processing the same order a no-op? What's the key, where does the
state live, and what's the race you had to avoid?

Idempotency is achieved via per-order Redis keys (order:{order_id}) acting as a state machine. A SETNX atomically transitions the key to "processing" before the charge call, and a SET transitions it to "done" (with 24h TTL) after a successful charge and ledger update. This bounds memory growth for production workloads and enables crash recovery to distinguish in-flight messages ("processing") from completed ones ("done"). The residual window between a successful charge and the "done" transition is a known vulnerability — a crash in that window results in a double charge on retry. Fully closing this gap requires an idempotent downstream payment API or a transactional outbox pattern, deferred for a production iteration.

### Failure handling
Retries, backoff, timeouts. How do you tell a *transient* failure from a *permanent*
one? Where do poison messages go? How do you keep one bad message from halting everything?

Failed payment calls are retried with exponential backoff (base 1s, multiplier 2, max 5 attempts) plus random jitter (0–30% of delay) to avoid thundering herd in multi-worker deployments. HTTP 5xx, timeouts, and connection errors are treated as transient and retried. Messages that exhaust all retries remain in the consumer group's pending entries list and are periodically reclaimed by a background recovery loop, preventing silent data loss without requiring a worker restart.

## Tradeoffs & alternatives

### Build vs adopt: Redis Streams vs Kafka / SQS / managed broker
We used Redis Streams to keep setup light. Would you keep it? At what point (throughput,
durability, team, ordering, retention needs) would you switch, and to what?

Redis Streams were retained for this exercise to keep setup lightweight and dependency-free. At 100× throughput or with stricter durability requirements (e.g., financial audit trails, message retention beyond memory), we would adopt Kafka for its partitioned log, configurable retention, and replay capability. The switch point is when the team needs message replays spanning days, exactly-once semantics via Kafka transactions, or when Redis memory becomes the bounding constraint on stream growth.

### From CI to CD
This repo stops at CI. How would you take it to continuous delivery — image promotion,
environments (dev/stage/prod), rollout strategy (blue-green / canary), and would you run
GitOps (ArgoCD / Flux)? Reason about it; don't build it.

Images would be promoted through environments via a registry-based pipeline: CI builds and pushes to GHCR on main merges, a deployment pipeline (GitHub Actions or ArgoCD) promotes tagged images to dev → stage → prod. Rollout would use a canary strategy (10% → 50% → 100% over 5-minute intervals) with health-check gating on the /health endpoint. GitOps via ArgoCD adds auditability, drift detection, and rollback — but for a single-service pipeline this exercise, a simple docker compose pull && up in the deployment job suffices. ArgoCD becomes valuable at 3+ services.

### Scaling to 100×
What breaks first at 100× throughput, and what would you change? Name the next bottleneck.

The first bottleneck is the single-threaded worker: at 100× throughput, one Python process cannot keep up with the stream rate. Mitigation: horizontal scaling by adding more worker instances within the same consumer group (Redis Streams distributes messages across consumers). The second bottleneck is the payments service — at 100×, 30% failure rate with 5-retry backoff creates cascading latency. Mitigation: circuit breaker that stops calling payments after N consecutive failures. The third bottleneck is Redis memory: unbounded streams need maxlen trimming or a retention policy.

## Consequences
What's better now. What's still weak / what you'd do next with more time.

The pipeline now correctly handles duplicate deliveries, worker restarts, and transient payment failures. Every customer is charged exactly the correct amount under the conditions defined in the acceptance check. What remains: no dead-letter queue for non-retryable poison messages, no structured logging or metrics, and the residual crash window between charge and idempotency marker is documented but not closed. With more time: add a dead-letter stream, structured observability with OpenTelemetry, and a Redis Lua script to atomically update the ledger and idempotency marker.