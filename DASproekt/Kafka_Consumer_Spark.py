import logging
import uuid
import psycopg2
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, sum, window, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

logger = logging.getLogger(__name__)

# 🚀 Initialize Spark Session
spark = (SparkSession.builder
         .appName("StockDataConsumer")
         .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                 "org.postgresql:postgresql:42.5.1")  # PostgreSQL Driver
         .config("spark.sql.shuffle.partitions", "16")  # Optimize for parallelism
         .config("spark.driver.memory", "16g")
         .config("spark.executor.memory", "8g")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# 🏗️ Define Schema
schema = StructType([
    StructField("Date", TimestampType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Ticker", StringType(), True)
])

# 🔄 Kafka Stream Configuration
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "stock-data")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("kafka.fetch.min.bytes", "50000")  # Ensure frequent fetches
      .option("kafka.max.partition.fetch.bytes", "10485760")  # Reduce to 10MB
      .option("maxOffsetsPerTrigger", "5000000")  # Increase batch size for more data
      .option("spark.streaming.kafka.maxRatePerPartition", "2000000")  # Process more messages per partition
      .option("spark.sql.shuffle.partitions", "16")
      .load())

stock_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# 📊 Aggregate Stock Data
aggregated_data = (stock_df.withWatermark("Date", "10 minutes")
                    .groupBy(window("Date", "5 minutes"), col("Ticker").alias("ticker"))
                    .agg(
                        min("Open").alias("min_open"),
                        max("High").alias("max_high"),
                        min("Low").alias("min_low"),
                        avg("Close").alias("avg_close"),
                        sum("Volume").alias("total_volume")
                    ))

# 🎯 PostgreSQL Configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "stock_data"
PG_USER = "postgres"
PG_PASSWORD = "Cheddar92$"
PG_TABLE = "stock_aggregated_data"

# 🚀 Write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    try:
        if batch_df.isEmpty():
            print(f"⚠️ [BATCH {batch_id}] No data received. Skipping insert.")
            return

        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()

        batch_data = [(row['window']['start'], row['window']['end'], row['ticker'],
                       row['min_open'], row['max_high'], row['min_low'], row['avg_close'], row['total_volume'])
                      for row in batch_df.collect()]

        if batch_data:
            insert_query = f"""
                INSERT INTO {PG_TABLE} (window_start, window_end, ticker, min_open, max_high, min_low, avg_close, total_volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (window_start, ticker) DO UPDATE
                SET min_open = EXCLUDED.min_open,
                    max_high = EXCLUDED.max_high,
                    min_low = EXCLUDED.min_low,
                    avg_close = EXCLUDED.avg_close,
                    total_volume = EXCLUDED.total_volume;
            """
            cursor.executemany(insert_query, batch_data)
            conn.commit()
            print(f"✅ [BATCH {batch_id}] Inserted {len(batch_data)} records into PostgreSQL")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ [BATCH {batch_id}] Database Error: {e}")

# 🚀 Start Streaming Query
query = (aggregated_data.writeStream
         .foreachBatch(write_to_postgres)
         .outputMode("append")
         .trigger(processingTime="30 seconds")  # Ensures continuous processing
         .start())

query.awaitTermination()