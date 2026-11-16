import argparse
import csv
import json
import time

from confluent_kafka import Producer


def to_iso(ts: str) -> str:
    # "2019-10-01 00:00:00 UTC" -> "2019-10-01T00:00:00Z"
    return ts.replace(" UTC", "").replace(" ", "T") + "Z"


def to_int(v):
    return int(v) if v else None


def to_float(v):
    return float(v) if v else None


def main():
    ap = argparse.ArgumentParser(description="Replay a CSV of e-commerce events into Kafka")
    ap.add_argument("--file", default="data/sample.csv")
    ap.add_argument("--topic", default="events")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--rate", type=float, default=500, help="events per second (0 = as fast as possible)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = all)")
    args = ap.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20})

    sent = 0
    start = time.time()
    with open(args.file, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if args.limit and sent >= args.limit:
                break

            event = {
                "event_time": to_iso(row["event_time"]),
                "event_type": row["event_type"],
                "product_id": to_int(row["product_id"]),
                "category_id": to_int(row["category_id"]),
                "category_code": row["category_code"] or None,
                "brand": row["brand"] or None,
                "price": to_float(row["price"]),
                "user_id": to_int(row["user_id"]),
                "user_session": row["user_session"],
            }

            # key = user_session -> all events of a session go to the same partition
            producer.produce(args.topic, key=event["user_session"], value=json.dumps(event))
            producer.poll(0)
            sent += 1

            if args.rate > 0:
                delay = (start + sent / args.rate) - time.time()
                if delay > 0:
                    time.sleep(delay)

            if sent % 10000 == 0:
                elapsed = time.time() - start
                print(f"sent {sent} events ({sent / elapsed:.0f} events/s)")

    producer.flush()
    print(f"done: {sent} events in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
