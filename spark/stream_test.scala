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
  .select(from_json(col("value").cast("string"), schema).as("e"), col("partition"))
  .select("e.*", "partition")
  .withColumn("event_time", to_timestamp(col("event_time"))))

val q = (events.writeStream
  .format("console")
  .outputMode("append")
  .option("truncate", "false")
  .option("numRows", 5)
  .start())

q.awaitTermination()
