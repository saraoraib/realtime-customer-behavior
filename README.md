# Real-Time Customer Behavior Intelligence

Streaming analytics for small online stores: which products get many views but few add-to-carts, and which are trending, updated within seconds.
Course project, Big Data Analytics 489124 (An-Najah National University), by Sara Oraib.

**Pipeline:** CSV replayer -> Kafka -> Spark Structured Streaming (Scala) -> MongoDB -> Spark SQL analytics -> Streamlit dashboard.
**Probabilistic structures:** HyperLogLog (distinct users) and Count-Min Sketch (heavy hitters), both compared against exact results.

## Data
REES46 "eCommerce behavior data from multi category store" (Kaggle:
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
We use the first 2,000,000 events of `2019-Oct.csv`:

```bash
head -n 2000001 2019-Oct.csv > data/sample.csv     # header + 2M events (gitignored)
```

Each event is sent to Kafka as JSON with the CSV columns (`event_time` as ISO-8601, `event_type` = view/cart/purchase, `product_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`); the Kafka key is `user_session`.

## Architecture
| Stage | Component | Detail |
|---|---|---|
| Source | `producer/replayer.py` | loops the CSV, stamps events with the current time, rate adjustable live |
| Queue | Kafka (KRaft, Docker) | topic `events`, 3 partitions |
| Streaming | `spark/stream_to_mongo.scala` | window 5 min, slide 1 min, watermark 2 min, output mode `update`, sink `foreachBatch` upsert |
| Streaming | `spark/stream_cms.scala` | streaming Count-Min Sketch + exact baseline, top-10 products by views |
| Storage | MongoDB 7, db `shop` | see schema below |
| Batch SQL | `spark/batch_analytics.scala` | joins windows with a products dimension, category funnel, low-conversion products |
| Evaluation | `spark/eval_sketches.scala` | sweeps HLL precision and CMS epsilon on the full sample |
| UI | `dashboard/app.py` (+ `pages/1_Live_CMS.py`) | Streamlit |
| Benchmark | `bench/measure.py` | end-to-end latency and throughput |

### MongoDB collections (db `shop`)
- `product_windows`: one doc per (product, window): `product_id, window_start, window_end, views, carts, purchases, unique_users` (HLL estimate). Indexes: `(product_id, window_start)`, `window_start`.
- `products`: dimension table: `product_id, category_code, brand, avg_price, category`. Index on `product_id`.
- `category_funnel`, `product_insights`: results of the Spark SQL batch job.
- `sketch_eval`: HLL/CMS parameter sweep results (error, bound, memory, time).
- `cms_latest`, `cms_history`: live Count-Min Sketch top-10 vs exact, error and memory per batch.
- `control`: one doc used by the dashboard to control the replayer (rate, pause, inject a campaign).

Window times are stored in UTC (local time is UTC+2).

## Run it
Requirements: Docker Desktop, Python 3.12, Git Bash (on Windows). Run all commands from the repo root.
On Windows Git Bash prefix `docker exec` with `MSYS_NO_PATHCONV=1`.

### 1. Start the services
```bash
docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
docker exec mongo mongosh --quiet --eval "db=db.getSiblingDB('shop'); db.product_windows.createIndex({product_id:1,window_start:-1}); db.product_windows.createIndex({window_start:-1}); db.products.createIndex({product_id:1})"
```
Ports on the host: Kafka `localhost:9092`, MongoDB `localhost:27018` (Docker maps 27018 -> 27017 to avoid clashing with a local MongoDB). Inside Docker the services are `kafka:19092` and `mongo:27017`.

### 2. Python environments
```bash
python -m venv producer/.venv && source producer/.venv/Scripts/activate
pip install confluent-kafka pymongo
deactivate
python -m venv dashboard/.venv && source dashboard/.venv/Scripts/activate
pip install -r dashboard/requirements.txt
deactivate
```
(On Linux/macOS use `bin/activate` instead of `Scripts/activate`.)

### 3. Streaming job (leave running)
```bash
docker exec spark rm -rf /tmp/ckpt_product_windows
docker exec spark /opt/spark/bin/spark-shell --driver-memory 2g --conf spark.jars.ivy=/tmp/.ivy --conf spark.sql.shuffle.partitions=6 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 -i /app/stream_to_mongo.scala
```

### 4. Send events (new terminal)
```bash
source producer/.venv/Scripts/activate
python producer/replayer.py
```
Start Spark first, then the replayer (the streaming query reads only new events).

### 5. Dashboard (new terminal)
```bash
source dashboard/.venv/Scripts/activate
python -m streamlit run dashboard/app.py
```
Tabs: Product alerts, Product timeline, Sketch accuracy, Control (rate, pause, inject a "campaign"), Analytics. The sidebar page **Live CMS** shows the streaming Count-Min Sketch.

### Optional jobs
Use the same `spark-shell` command as step 3 with a different script:
- `-i /app/eval_sketches.scala`: HLL/CMS parameter sweep (needs `data/sample.csv`), fills `sketch_eval`.
- `-i /app/batch_analytics.scala`: Spark SQL analytics (run it after some streaming data exists).
- `-i /app/stream_cms.scala`: live Count-Min Sketch (use instead of, not together with, the main job on a small machine).
- `python bench/measure.py` (producer venv; needs the streaming job and replayer running): latency and throughput benchmark.

## Probabilistic structures
- **HyperLogLog**: Spark `approx_count_distinct(user_id, 0.02)` for distinct users per product and window (about 4096 registers).
- **Count-Min Sketch**: width = ceil(e / epsilon), depth = ceil(ln(1/delta)); epsilon = 0.001, delta = 0.01 gives 2719 x 5 counters (about 54 KB). Error is compared with the exact count and with the bound epsilon x N.
  In the live run (45,096 views, 12,619 distinct products): top-10 recall = 1, max error 9 vs bound 45, memory 54 KB vs 900 KB for the exact map.

## Benchmark (single run, one laptop; Kafka, Spark local and MongoDB on the same machine)
| Target rate | Achieved into Kafka | Latency median | p95 | Probes timed out |
|---|---|---|---|---|
| 200 ev/s | 197 | 4.4 s | 7.3 s | 0/10 |
| 1000 ev/s | 620 (replayer limit) | 10.7 s | 15.6 s | 0/10 |
| 3000 ev/s | 2847 | saturated | saturated | 10/10 |

Burst test: 50,000 events for one product drained in 19.9 s (2508 ev/s). This is an upper bound with light Mongo writes, not a sustained rate: with thousands of different products every batch writes many more documents. Latency is measured as the time from injecting one event until it appears in `product_windows`, so it includes the micro-batch trigger and the MongoDB write.

## Limitations
- HyperLogLog is not faster than an exact count on one machine; its benefit is bounded memory and mergeability.
- The streaming Count-Min Sketch lives in the driver memory and is reset when the job restarts.
- About 30% of products have no `category_code`; they are grouped as `unknown`.
- The dataset contains purchases without a preceding cart event, so view-to-cart rates are approximate.
- Benchmark numbers come from a single run.
