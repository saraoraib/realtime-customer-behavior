import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.util.SizeEstimator
import scala.collection.mutable

val uri = "mongodb://mongo:27017"
val K = 10
val EPS = 0.001
val DELTA = 0.01

// ---------- Count-Min Sketch ----------
class CMS(val eps: Double, val delta: Double) {
  val w = math.ceil(math.E / eps).toInt
  val d = math.ceil(math.log(1.0 / delta)).toInt
  private val P = 2147483647L
  private val rnd = new scala.util.Random(42)
  private val a = Array.fill(d)(1L + rnd.nextInt(Int.MaxValue - 1))
  private val b = Array.fill(d)(rnd.nextInt(Int.MaxValue).toLong)
  val table = Array.ofDim[Int](d, w)
  private def h(i: Int, x: Long): Int = (((a(i) * (x % P) + b(i)) % P) % w).toInt
  def add(x: Long, c: Int): Unit = { var i = 0; while (i < d) { table(i)(h(i, x)) += c; i += 1 } }
  def estimate(x: Long): Long = {
    var m = Int.MaxValue; var i = 0
    while (i < d) { m = math.min(m, table(i)(h(i, x))); i += 1 }
    m.toLong
  }
}

case class TopItem(product_id: Long, cms_estimate: Long, exact_count: Long)
case class CmsLatest(_id: String, ts: java.sql.Timestamp, batch_id: Long, total_views: Long,
                     width: Int, depth: Int, epsilon: Double, delta: Double, bound: Double,
                     recall_at_10: Double, max_abs_error_top10: Long,
                     cms_memory_bytes: Long, exact_memory_bytes: Long,
                     distinct_products: Long, top: Seq[TopItem])
case class CmsHist(ts: java.sql.Timestamp, batch_id: Long, total_views: Long, bound: Double,
                   recall_at_10: Double, max_abs_error_top10: Long,
                   cms_memory_bytes: Long, exact_memory_bytes: Long)

val cms = new CMS(EPS, DELTA)
val exact = mutable.HashMap[Long, Long]()      // exact baseline, kept ONLY to measure the error
val cand = mutable.HashMap[Long, Long]()       // heavy-hitter candidates: product -> CMS estimate
var total = 0L
val memCms = SizeEstimator.estimate(cms.table)
var memExact = 0L

def toMongo(df: DataFrame, coll: String, replace: Boolean): Unit = {
  val w = df.write.format("mongodb").mode("append")
    .option("connection.uri", uri).option("database", "shop").option("collection", coll)
  (if (replace) w.option("operationType", "replace") else w).save()
}

def process(batch: DataFrame, batchId: Long): Unit = {
  val counts = batch.filter(col("event_type") === "view")
    .groupBy("product_id").count().collect()
  if (counts.isEmpty) return
  counts.foreach { r =>
    val id = r.getLong(0); val c = r.getLong(1)
    cms.add(id, c.toInt)
    exact.update(id, exact.getOrElse(id, 0L) + c)
    total += c
    val est = cms.estimate(id)
    if (cand.contains(id) || cand.size < K) cand(id) = est
    else {
      val (mk, mv) = cand.minBy(_._2)
      if (est > mv) { cand.remove(mk); cand(id) = est }
    }
  }
  if (batchId % 5 == 0) memExact = SizeEstimator.estimate(exact)

  val topCms = cand.keys.toSeq.map(k => (k, cms.estimate(k))).sortBy(-_._2).take(K)
  val topExact = exact.toSeq.sortBy(-_._2).take(K)
  val exactSet = topExact.map(_._1).toSet
  val recall = topCms.count(t => exactSet.contains(t._1)).toDouble / math.min(K, exact.size)
  val maxErr = topCms.map { case (k, e) => math.abs(e - exact.getOrElse(k, 0L)) }.max
  val top = topCms.map { case (k, e) => TopItem(k, e, exact.getOrElse(k, 0L)) }
  val now = new java.sql.Timestamp(System.currentTimeMillis())
  val bound = EPS * total

  import spark.implicits._
  toMongo(Seq(CmsLatest("latest", now, batchId, total, cms.w, cms.d, EPS, DELTA, bound,
    recall, maxErr, memCms, memExact, exact.size.toLong, top)).toDF(), "cms_latest", true)
  toMongo(Seq(CmsHist(now, batchId, total, bound, recall, maxErr, memCms, memExact)).toDF(),
    "cms_history", false)
}

// ---------- source: Kafka ----------
val schema = new StructType()
  .add("event_time", StringType).add("event_type", StringType)
  .add("product_id", LongType).add("category_id", LongType)
  .add("category_code", StringType).add("brand", StringType)
  .add("price", DoubleType).add("user_id", LongType)
  .add("user_session", StringType)

val raw = (spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "kafka:19092")
  .option("subscribe", "events")
  .option("startingOffsets", "latest")
  .load())

val events = (raw
  .select(from_json(col("value").cast("string"), schema).as("e"))
  .select("e.*")
  .filter(col("product_id").isNotNull))

val q = (events.writeStream
  .trigger(Trigger.ProcessingTime("2 seconds"))
  .foreachBatch { (df: DataFrame, id: Long) => process(df, id) }
  .start())

q.awaitTermination()
