import argparse
import csv
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from pymongo import MongoClient

TICK = 0.1  # seconds


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_int(v):
    return int(v) if v else None


def to_float(v):
    return float(v) if v else None


def rows(path):
    while True:  # loop the file forever
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                yield r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="data/sample.csv")
    ap.add_argument("--topic", default="events")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--mongo", default="mongodb://127.0.0.1:27018")
    args = ap.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20})
    ctl_col = MongoClient(args.mongo, serverSelectionTimeoutMS=3000)["shop"]["control"]
    ctl_col.update_one({"_id": "replayer"}, {"$setOnInsert": {"rate": 200, "paused": False}}, upsert=True)

    def send(event):
        producer.produce(args.topic, key=event["user_session"], value=json.dumps(event))

    def inject(spec):
        pid = int(spec["product_id"])
        views, carts = int(spec["views"]), int(spec["carts"])
        users = [random.randint(900_000_000, 999_999_999) for _ in range(max(10, views // 5))]
        for i in range(views + carts):
            u = random.choice(users)
            send({"event_time": now_iso(),
                  "event_type": "view" if i < views else "cart",
                  "product_id": pid, "category_id": None, "category_code": None,
                  "brand": None, "price": 100.0, "user_id": u,
                  "user_session": f"inj-{u}-{uuid.uuid4().hex[:6]}"})
        print(f"injected campaign: product {pid}, {views} views, {carts} carts")

    it = rows(args.file)
    carry, sent, last_print = 0.0, 0, time.time()
    while True:
        t0 = time.time()
        ctl = ctl_col.find_one({"_id": "replayer"}) or {}
        if ctl.get("inject"):
            inject(ctl["inject"])
            ctl_col.update_one({"_id": "replayer"}, {"$unset": {"inject": ""}})
        if not ctl.get("paused", False):
            carry += ctl.get("rate", 200) * TICK
            n = int(carry)
            carry -= n
            for _ in range(n):
                r = next(it)
                send({"event_time": now_iso(), "event_type": r["event_type"],
                      "product_id": to_int(r["product_id"]), "category_id": to_int(r["category_id"]),
                      "category_code": r["category_code"] or None, "brand": r["brand"] or None,
                      "price": to_float(r["price"]), "user_id": to_int(r["user_id"]),
                      "user_session": r["user_session"]})
                sent += 1
        producer.poll(0)
        if time.time() - last_print > 5:
            print(f"sent {sent} events, rate={ctl.get('rate')}, paused={ctl.get('paused')}")
            last_print = time.time()
        time.sleep(max(0, TICK - (time.time() - t0)))


if __name__ == "__main__":
    main()
