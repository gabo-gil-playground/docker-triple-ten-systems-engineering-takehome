"""Order worker.

Consumes orders from a Redis Stream using consumer groups for at-least-once delivery.
On restart, reclaims any pending messages that were not acknowledged before the crash.

Idempotency is enforced via per-order Redis keys acting as a state machine
("processing" → "done") with a 24h TTL, preventing duplicate charges and bounding
memory growth.
"""
import json
import os
import random
import socket
import time

import redis
import requests

REDIS_URL = os.environ["REDIS_URL"]
PAYMENTS_URL = os.environ["PAYMENTS_URL"]
ORDERS_STREAM = "orders"
DEAD_LETTER_STREAM = "orders:dead"
GROUP_NAME = "workers"
CONSUMER_NAME = f"{socket.gethostname()}-{os.getpid()}"

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
RETRY_BASE_DELAY = 1.0
IDEMPOTENCY_TTL = 86400  # 24 hours

r = redis.from_url(REDIS_URL, decode_responses=True)


def idempotency_key(order_id):
    return f"order:{order_id}"


def try_claim_order(order_id, force=False):
    key = idempotency_key(order_id)
    state = r.get(key)
    if state == "done":
        print(f"skipping duplicate {order_id}", flush=True)
        return False
    if state == "processing":
        if not force:
            print(f"skipping in-flight {order_id}", flush=True)
            return False
    acquired = r.set(key, "processing", nx=not force)
    if not acquired:
        print(f"skipping already claimed {order_id}", flush=True)
        return False
    return True


def mark_order_done(order_id):
    r.set(idempotency_key(order_id), "done", ex=IDEMPOTENCY_TTL)


def create_consumer_group():
    try:
        r.xgroup_create(ORDERS_STREAM, GROUP_NAME, id="0", mkstream=True)
        print(f"consumer group '{GROUP_NAME}' created", flush=True)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            print(f"consumer group '{GROUP_NAME}' already exists", flush=True)
        else:
            raise


def claim_pending():
    pending = r.xpending_range(ORDERS_STREAM, GROUP_NAME, min="-", max="+", count=100)
    if not pending:
        return
    for entry in pending:
        claimed = r.xclaim(
            ORDERS_STREAM,
            GROUP_NAME,
            CONSUMER_NAME,
            min_idle_time=2000,
            message_ids=[entry["message_id"]],
        )
        if not claimed:
            continue
        for msg_id, fields in claimed:
            order = json.loads(fields["data"])
            print(f"reclaiming pending {order['order_id']}", flush=True)
            if not try_claim_order(order["order_id"], force=True):
                r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)
                continue
            success = process(order)
            if success:
                mark_order_done(order["order_id"])
                r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)
            else:
                r.xadd(DEAD_LETTER_STREAM, {"data": fields["data"]})
                r.incr("dead_letter_count")
                r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)


def process(order):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                f"{PAYMENTS_URL}/charge",
                json={"order_id": order["order_id"], "amount_cents": order["amount_cents"]},
                timeout=(3, 10),
            )
            resp.raise_for_status()
        except requests.exceptions.RequestException as exc:
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                jitter = random.uniform(0, delay * 0.3)
                total_delay = delay + jitter
                print(
                    f"payment failed for {order['order_id']} (attempt {attempt}/{MAX_RETRIES}), "
                    f"retrying in {total_delay:.1f}s",
                    flush=True,
                )
                time.sleep(total_delay)
                continue
            print(
                f"payment failed for {order['order_id']} after {MAX_RETRIES} attempts, "
                f"moving to dead-letter",
                flush=True,
            )
            return False
        break

    r.incrby(f"ledger:{order['customer_id']}", order["amount_cents"])
    r.incr("processed_count")
    print(f"processed {order['order_id']} for {order['customer_id']}", flush=True)
    return True


def main():
    print(f"worker started (consumer: {CONSUMER_NAME})", flush=True)
    create_consumer_group()
    claim_pending()

    while True:
        resp = r.xreadgroup(
            GROUP_NAME, CONSUMER_NAME, {ORDERS_STREAM: ">"}, count=10, block=5000
        )
        if not resp:
            claim_pending()
            continue
        for _stream, messages in resp:
            for msg_id, fields in messages:
                order = json.loads(fields["data"])
                if not try_claim_order(order["order_id"]):
                    r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)
                    continue
                success = process(order)
                if success:
                    mark_order_done(order["order_id"])
                    r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)
                else:
                    r.xadd(DEAD_LETTER_STREAM, {"data": fields["data"]})
                    r.incr("dead_letter_count")
                    r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)


if __name__ == "__main__":
    main()
