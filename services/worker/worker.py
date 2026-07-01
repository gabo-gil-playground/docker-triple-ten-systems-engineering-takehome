"""Order worker.

Consumes orders from a Redis Stream using consumer groups for at-least-once delivery.
On restart, reclaims any pending messages that were not acknowledged before the crash.
"""
import json
import os
import socket

import redis
import requests

REDIS_URL = os.environ["REDIS_URL"]
PAYMENTS_URL = os.environ["PAYMENTS_URL"]
ORDERS_STREAM = "orders"
GROUP_NAME = "workers"
CONSUMER_NAME = f"{socket.gethostname()}-{os.getpid()}"
PROCESSED_SET = "processed_orders"

r = redis.from_url(REDIS_URL, decode_responses=True)


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
            if r.sismember(PROCESSED_SET, order["order_id"]):
                print(f"skipping duplicate {order['order_id']}", flush=True)
                r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)
                continue
            success = process(order)
            if success:
                r.sadd(PROCESSED_SET, order["order_id"])
                r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)


def process(order):
    try:
        resp = requests.post(
            f"{PAYMENTS_URL}/charge",
            json={"order_id": order["order_id"], "amount_cents": order["amount_cents"]},
            timeout=(3, 10),
        )
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        print(
            f"payment failed for {order['order_id']} ({order['customer_id']}): {exc}",
            flush=True,
        )
        return False

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
            continue
        for _stream, messages in resp:
            for msg_id, fields in messages:
                order = json.loads(fields["data"])
                if r.sismember(PROCESSED_SET, order["order_id"]):
                    print(f"skipping duplicate {order['order_id']}", flush=True)
                    r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)
                    continue
                success = process(order)
                if success:
                    r.sadd(PROCESSED_SET, order["order_id"])
                    r.xack(ORDERS_STREAM, GROUP_NAME, msg_id)


if __name__ == "__main__":
    main()
