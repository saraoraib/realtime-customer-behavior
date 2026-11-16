import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = new StructType()
  .add("event_time", StringType)
  .add("event_type", StringType)
  .add("product_id", LongType)
  .add("category_id", LongType)
  .add("category_code", StringType)
  .add("brand", StringType)
  .add("price", DoubleType)
  .add("user_id", LongType)
  .add("user_session", StringType)

val raw = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka:19092")
  .option("subscribe", "events")
  .option("startingOffsets", "latest")
  .load())

val events = (raw
  .select(from_json(col("value").cast("string"), schema).as("e"))
  .select("e.*")
  .withColumn("event_time", to_timestamp(col("event_time")))
  .filter(col("event_time").isNotNull && col("product_id").isNotNull))

val agg = (events
  .withWatermark("event_time", "2 minutes")
  .groupBy(window(col("event_time"), "5 minutes", "1 minute"), col("product_id"))
  .agg(
    count(when(col("event_type") === "view", 1)).as("views"),
    count(when(col("event_type") === "cart", 1)).as("carts"),
    count(when(col("event_type") === "purchase", 1)).as("purchases"),
    approx_count_distinct("user_id").as("unique_users")))

val q = (agg.writeStream
  .format("console")
  .outputMode("update")
  .option("truncate", "false")
  .option("numRows", 10)
  .start())

q.awaitTermination()
