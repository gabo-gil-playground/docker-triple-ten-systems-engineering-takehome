# AI Notes

This role is partly about *correcting* AI output, so we want to see how you use it — and
where your judgment overrode it. Keep this short.

## A prompt I used
* "Please, act as backend software engineer expert in distributed systems and analyze current project's design and implementation. Your task is identify bugs, potential issues and not compliance / not best practice friendly logic or code. Create a list of bugs ordered by critical to low. Create a second list of points of improvement ordered by priority from high to low."

## Something the AI got wrong or oversimplified — and how I caught it
* What the AI suggested: "Use a Redis Set (processed_orders) with SISMEMBER before charging and SADD after — simple, O(1), and guarantees exactly-one charge."
* Why it was oversimplified: The AI ignored three failure modes specific to this system.
  * First, Redis data loss is correlated: both the stream and the Set live in the same Redis instance — a restart or partial RDB restore could recover messages from the stream but lose the idempotency Set, silently enabling double charges.
  * Second, the Set grows unbounded, which is fine for 60 test messages but a memory leak in production.
  * Third, the window between INCRBY ledger and SADD is a crash vulnerability: if the worker dies in that window, the retry-detection key is absent and the customer is charged twice.
* What I did instead: Kept the Set approach for this exercise (single Redis instance, no persistence, 60 messages — the risks don't materialize here) but documented in the ADR that production requires: per-order keys with unique UUID + TTL expiration to bound memory, an idempotency token passed to the downstream payment API (or an idempotent payment endpoint) and ideally a transactional outbox to close the ledger-vs-marker gap.

<If you genuinely used no AI on this task, say so here and tell us how you would have
used it and what you'd have double-checked.>
