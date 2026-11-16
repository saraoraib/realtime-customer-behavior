"""End-to-end benchmark: replayer -> Kafka -> Spark -> MongoDB.
Needs running: docker compose, spark/stream_to_mongo.scala, producer/replayer.py.
Run inside producer/.venv:  python bench/measure.py
"""
import argparse, csv, random, statistics, time
from pymongo import MongoClient
from confluent_kafka import Consumer, TopicPartition

ap = argparse.ArgumentParser()
ap.add_argument("--rates", type=int, nargs="+", default=[200, 1000, 3000])
ap.add_argument("--probes", type=int, default=10)
ap.add_argument("--warmup", type=int, default=20)
ap.add_argument("--burst", type=int, default=50000)
ap.add_argument("--mongo", default="mongodb://127.0.0.1:27018")
ap.add_argument("--bootstrap", default="localhost:9092")
a = ap.parse_args()

db = MongoClient(a.mongo)["shop"]
cons = Consumer({"bootstrap.servers": a.bootstrap, "group.id": "bench"})

def produced(topic="events"):
    parts = cons.list_topics(topic, timeout=10).topics[topic].partitions
    return sum(cons.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts)

def control(**kw):
    db.control.update_one({"_id": "replayer"}, {"$set": kw}, upsert=True)

def new_pid():
    return random.randint(2_000_000_000, 2_100_000_000)  # fits Int, never a real product

def wait_for(pid, n, timeout):
    t_end = time.time() + timeout
    while time.time() < t_end:
        m = max((d.get("views", 0) for d in db.product_windows.find({"product_id": pid}, {"views": 1})), default=0)
        if m >= n:
            return time.time()
        time.sleep(0.05)
    return None

rows = []
for rate in a.rates:
    print(f"\n== target rate {rate} events/s ==")
    control(rate=rate, paused=False)
    time.sleep(a.warmup)
    o0, t0 = produced(), time.time()
    lats = []
    for i in range(a.probes):
        pid = new_pid()
        ts = time.time()
        control(inject={"product_id": pid, "views": 1, "carts": 0})
        te = wait_for(pid, 1, 60)
        lats.append((te - ts) if te else None)
        print(f"  probe {i+1}: " + (f"{lats[-1]:.2f} s" if te else "TIMEOUT (>60 s)"))
        time.sleep(3)
    o1, t1 = produced(), time.time()
    ok = sorted(x for x in lats if x is not None)
    achieved = (o1 - o0) / (t1 - t0)
    row = dict(target_rate=rate, achieved_rate=round(achieved),
               lat_median_s=round(statistics.median(ok), 2) if ok else None,
               lat_p95_s=round(ok[max(0, int(len(ok) * 0.95) - 1)], 2) if ok else None,
               lat_max_s=round(ok[-1], 2) if ok else None, timeouts=len(lats) - len(ok))
    rows.append(row); print(" ", row)
    control(paused=True); time.sleep(30)   # let any backlog drain

print("\n== burst drain test ==")
control(rate=50, paused=False); time.sleep(10)
pid = new_pid(); ts = time.time()
control(inject={"product_id": pid, "views": a.burst, "carts": 0})
te = wait_for(pid, a.burst, 300)
burst = dict(burst_events=a.burst, drain_s=round(te - ts, 2) if te else None,
             events_per_s=round(a.burst / (te - ts)) if te else None)
print(" ", burst)
control(rate=500, paused=True)

with open("bench/results.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
print("\nSaved bench/results.csv")
