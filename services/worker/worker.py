"""Order worker.

Consumes orders from a Redis Stream using consumer groups for at-least-once delivery.
On restart, reclaims any pending messages that were not acknowledged before the crash.

Idempotency is enforced via per-order Redis keys acting as a state machine
("processing" → "done") with a 24h TTL, preventing duplicate charges and bounding
memory growth.
"""
import json
import logging
import os
import random
import signal
import socket
import sys
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

_shutdown = False

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":%(message)s}',
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("worker")


def _handle_shutdown(signum, frame):
    global _shutdown
    if not _shutdown:
        _shutdown = True
        log.info(json.dumps({"event": "shutdown_received"}))


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)

r = redis.from_url(REDIS_URL, decode_responses=True)


def idempotency_key(order_id):
    return f"order:{order_id}"


def try_claim_order(order_id, force=False):
    key = idempotency_key(order_id)
    state = r.get(key)
    if state == "done":
        log.info(json.dumps({"event": "skip_duplicate", "order_id": order_id}))
        return False
    if state == "processing":
        if not force:
            log.info(json.dumps({"event": "skip_inflight", "order_id": order_id}))
            return False
    acquired = r.set(key, "processing", nx=not force)
    if not acquired:
        log.info(json.dumps({"event": "skip_already_claimed", "order_id": order_id}))
        return False
    return True


def mark_order_done(order_id):
    r.set(idempotency_key(order_id), "done", ex=IDEMPOTENCY_TTL)


def create_consumer_group():
    try:
        r.xgroup_create(ORDERS_STREAM, GROUP_NAME, id="0", mkstream=True)
        log.info(json.dumps({"event": "group_created", "group": GROUP_NAME}))
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            log.info(json.dumps({"event": "group_exists", "group": GROUP_NAME}))
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
            log.info(json.dumps({"event": "reclaim_pending", "order_id": order["order_id"]}))
            if not try_claim_order(order["order_id"], force=True):
                r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)
                continue
            success = process(order)
            if success:
                mark_order_done(order["order_id"])
                r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)
            elif _shutdown:
                log.info(json.dumps(
                    {"event": "leave_pending", "order_id": order["order_id"],
                     "reason": "shutdown"}))
                return
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
                log.warning(json.dumps(
                    {"event": "payment_retry", "order_id": order["order_id"],
                     "customer_id": order["customer_id"], "attempt": attempt,
                     "max_retries": MAX_RETRIES, "retry_in": round(total_delay, 1)}))
                time.sleep(total_delay)
                if _shutdown:
                    log.info(json.dumps(
                        {"event": "shutdown_skip_retries", "order_id": order["order_id"]}))
                    return False
                continue
            log.error(json.dumps(
                {"event": "payment_exhausted", "order_id": order["order_id"],
                 "customer_id": order["customer_id"], "attempts": MAX_RETRIES}))
            return False
        break

    r.incrby(f"ledger:{order['customer_id']}", order["amount_cents"])
    r.incr("processed_count")
    log.info(json.dumps(
        {"event": "processed", "order_id": order["order_id"],
         "customer_id": order["customer_id"],
         "amount_cents": order["amount_cents"]}))
    return True


def main():
    log.info(json.dumps({"event": "worker_started", "consumer": CONSUMER_NAME}))
    create_consumer_group()
    claim_pending()

    while not _shutdown:
        try:
            resp = r.xreadgroup(
                GROUP_NAME, CONSUMER_NAME, {ORDERS_STREAM: ">"}, count=10, block=5000
            )
        except Exception:
            if _shutdown:
                break
            raise
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
                elif _shutdown:
                    log.info(json.dumps(
                        {"event": "leave_pending", "order_id": order["order_id"],
                         "reason": "shutdown"}))
                    break
                else:
                    r.xadd(DEAD_LETTER_STREAM, {"data": fields["data"]})
                    r.incr("dead_letter_count")
                    r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)

    log.info(json.dumps({"event": "worker_shutdown"}))


if __name__ == "__main__":
    main()
