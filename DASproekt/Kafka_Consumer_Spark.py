from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Иницијализирај Spark Streaming Session
spark = (SparkSession.builder
    .appName("StockDataConsumer")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.spark:spark-avro_2.12:3.5.4")
    .getOrCreate())

# Дефинирај schema за податоците што доаѓаат од Kafka
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Ticker", StringType(), True)
])


# Читање на податоци од Kafka
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "stock-data")
      .option("startingOffsets", "earliest")  # или "latest" ако сакаш нови податоци
      .load())

# Дебаг тест: Проверка дали податоците пристигнуваат правилно
df_test = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")  # Ги вади сите полиња


# Претворање на вредностите од Kafka во DataFrame со дефинирана структура
stock_df = (df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select(
                col("data.Date").cast(TimestampType()).alias("Datetime"),
                col("data.Open").alias("Open"),
                col("data.High").alias("High"),
                col("data.Low").alias("Low"),
                col("data.Close").alias("Close"),
                col("data.Volume").alias("Volume"),
                col("data.Ticker").alias("Ticker")
            ))

# Агрегација: Движечки просек на "Close" во 5-минутен прозорец
moving_avg = (stock_df.withWatermark("Datetime", "5 minutes")
              .groupBy(window("Datetime", "5 minutes"), col("Ticker"))
              .agg(avg("Close").alias("Avg_Close")))

# Прикажи резултати на конзола
query = (df_test.writeStream
              .outputMode("append")
              .format("console")
              .option("truncate", False)  # Ова спречува кратење на податоците
              .start())

query.awaitTermination()
