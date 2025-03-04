import logging
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, from_json, struct, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

logger = logging.getLogger(__name__)

unique_group_id = f"stock-data-consumer-{uuid.uuid4()}"

try:
    spark = (SparkSession.builder
             .appName("StockDataConsumer")
             .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.apache.spark:spark-avro_2.12:3.5.4,"
                     "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0")
             .config("spark.cassandra.connection.host", "127.0.0.1")
             .config("spark.cassandra.connection.port", "9042")
             .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
             .config("spark.sql.streaming.stopTimeout", "60000")
             .getOrCreate())
except Exception as e:
    logger.error(f"Failed to create Spark Session: {e}", exc_info=True)
    raise

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("Date", TimestampType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Ticker", StringType(), True)
])

try:
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "stock-data")
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", "false")
          .option("kafka.group.id", unique_group_id)
          .option("kafka.session.timeout.ms", "10000")
          .option("kafka.heartbeat.interval.ms", "3000")
          .load())
except Exception as e:
    logger.error(f"Failed to create Kafka stream: {e}", exc_info=True)
    raise

stock_df = (df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*"))

def log_batch(df, epoch_id):
    try:
        logger.info(f"Processing Batch {epoch_id}")
        df.show(5, truncate=False)
        logger.info(f"Batch size: {df.count()} records")
    except Exception as e:
        logger.error(f"Error in log_batch: {e}", exc_info=True)

moving_avg = (stock_df.withWatermark("Date", "5 minutes")
              .groupBy(window("Date", "5 minutes"), col("Ticker"))
              .agg(avg("Close").alias("avg_close")))

cassandra_df = moving_avg.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("Ticker").alias("ticker"),
    col("avg_close")
)


try:
    query = (cassandra_df.writeStream
             .foreachBatch(log_batch)
             .outputMode("append")
             .format("org.apache.spark.sql.cassandra")
             .option("keyspace", "stock_analysis")
             .option("table", "stock_moving_averages")
             .option("checkpointLocation", "/tmp/cassandra-checkpoints")
             .start())

    if query.awaitTermination(timeout=7200):
        logger.info("Streaming query completed successfully")
    else:
        logger.warning("Streaming query timed out")

    if query.exception():
        logger.error(f"Streaming query failed: {query.exception()}")

except Exception as e:
    logger.error(f"Error in streaming process: {e}", exc_info=True)
finally:
    if 'query' in locals() and query.isActive:
        query.stop()
        logger.info("Streaming query stopped")