from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StringType, DoubleType

# Define Schema
schema = StructType().add("symbol", StringType()) \
                     .add("Open", DoubleType()) \
                     .add("High", DoubleType()) \
                     .add("Low", DoubleType()) \
                     .add("Close", DoubleType()) \
                     .add("Volume", DoubleType())

# Initialize Spark
spark = SparkSession.builder.appName("StockProcessing").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read Kafka Stream
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_data") \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize JSON
df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# Perform Aggregation (Example: Moving Average)
agg_df = df.groupBy("symbol").agg(avg("Close").alias("avg_price"))

# Write to Console (or Kafka)
query = agg_df.writeStream.outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
