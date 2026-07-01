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
* What I did instead: Accepted the Set approach as a starting point but later evolved it to per-order idempotency keys (order:{order_id}) acting as a state machine: SETNX atomically claims "processing" before the charge, and SET transitions to "done" with a 24h TTL after success. This addresses two of the three identified failure modes — unbounded memory growth (TTL caps it) and the ledger-vs-marker gap (the "processing" state enables crash recovery to distinguish in-flight from completed orders). The correlated data-loss risk remains and is documented in the ADR.
