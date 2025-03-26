import psycopg2
import psycopg2.extras
import pandas as pd
import time
import json
import threading
import datetime
import os
from confluent_kafka import Producer
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max as spark_max, avg, sum, window, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pytz

# PostgreSQL Config
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "stock_data"
PG_USER = "postgres"
PG_PASSWORD = "Cheddar92$"

# Kafka Config
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stock-data"

producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'acks': 'all',
    'linger.ms': 5,
    'batch.size': 65536,
    'compression.type': 'snappy',
    'queue.buffering.max.messages': 1000000,
    'queue.buffering.max.kbytes': 2097151,
}

producer = Producer(producer_config)

failed_batches = []
failed_lock = threading.Lock()
sema = threading.Semaphore(3)

def get_tickers_from_csv_file(csv_path='ticker_list.csv'):
    df = pd.read_csv(csv_path)
    return df['Ticker'].dropna().tolist()

def clear_stock_realtime_if_2pm():
        print("🧹 Clearing stock_realtime table at 2:00 PM Skopje time...")
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM stock_realtime;")
            conn.commit()
            cursor.close()
            conn.close()
            print("✅ Cleared stock_realtime table.")
        except Exception as e:
            print(f"❌ Error clearing stock_realtime: {e}")

def get_tickers_from_stock_data():
    return get_tickers_from_csv_file('ticker_list.csv')

def fetch_and_send_large_batch(ticker_batch):
    global failed_batches
    try:
        data = yf.download(tickers=ticker_batch, period='1d', group_by='ticker', threads=True, progress=False)

        if data.empty:
            print(f"⚠️ Empty data for batch: {ticker_batch[0]}-{ticker_batch[-1]}")
            return

        skopje_tz = pytz.timezone('Europe/Skopje')
        current_time = datetime.datetime.now(skopje_tz).isoformat()

        for ticker in ticker_batch:
            try:
                if len(ticker_batch) > 1:
                    if ticker not in data.columns.get_level_values(0):
                        print(f"⚠️ Ticker {ticker} missing in fetched data.")
                        continue
                    ticker_data = data[ticker]
                else:
                    ticker_data = data

                if ticker_data.empty:
                    print(f"⚠️ No data for {ticker}.")
                    continue

                latest_row = ticker_data.tail(1).iloc[0]
                if pd.isna(latest_row['Close']) or pd.isna(latest_row['Volume']):
                    print(f"⚠️ Skipping {ticker}: Close or Volume is NaN")
                    continue

                stock_data = {
                    "Date": current_time,
                    "Open": round(latest_row['Open'], 2),
                    "High": round(latest_row['High'], 2),
                    "Low": round(latest_row['Low'], 2),
                    "Close": round(latest_row['Close'], 2),
                    "Volume": int(latest_row['Volume']),
                    "Ticker": ticker
                }

                producer.produce(TOPIC_NAME, json.dumps(stock_data))
                print(f"📩 Sent: {ticker} | Close: {stock_data['Close']}")

            except Exception as inner_e:
                print(f"❌ Error processing {ticker}: {inner_e}")
                with open("missing_tickers.log", "a") as f:
                    f.write(f"{ticker} - error: {inner_e}\n")

        producer.flush()

    except Exception as e:
        print(f"❌ Batch fetch error for {ticker_batch[0]}-{ticker_batch[-1]}: {e}")
        if "Too Many Requests" in str(e):
            print("⏳ Rate limit hit. Waiting 120 seconds before retrying batch...")
            failed_batches.append(ticker_batch)
            time.sleep(120)
        else:
            time.sleep(5)

def start_batch_producer(batch_size=500, end_time_limit=None):
    global failed_batches
    tickers = get_tickers_from_stock_data()
    print(f"✅ Found {len(tickers)} tickers.")

    total_tickers = len(tickers)
    ticker_batches = [tickers[i:i + batch_size] for i in range(0, total_tickers, batch_size)]

    skopje_tz = pytz.timezone('Europe/Skopje')

    while True:
        now = datetime.datetime.now(skopje_tz)
        if end_time_limit and now.time() >= end_time_limit:
            print(f"⏹ Reached end time {end_time_limit}. Stopping batch producer.")
            break

        round_start = time.time()
        print(f"🚀 [Skopje {now.time()}] Starting round of {len(ticker_batches)} batches...")

        for i, batch in enumerate(ticker_batches):
            fetch_and_send_large_batch(batch)
            time.sleep(45)

        if failed_batches:
            print(f"🔁 Retrying {len(failed_batches)} failed batches due to rate limits...")
            retry_batches = failed_batches.copy()
            failed_batches.clear()
            for batch in retry_batches:
                fetch_and_send_large_batch(batch)
                time.sleep(30)

        elapsed = time.time() - round_start
        print(f"⏳ Round complete in {elapsed:.2f}s. Waiting 120 seconds before next round...\n")

        time.sleep(120)

def create_spark_session():
    return (SparkSession.builder
            .appName("StockDataBatchRealTimeConsumer")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
            .config("spark.sql.shuffle.partitions", "64")
            .config("spark.driver.memory", "12g")
            .config("spark.executor.memory", "8g")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.kafka.maxRatePerPartition", "20000")
            .config("spark.sql.session.timeZone", "Europe/Skopje")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," 
                                           "org.postgresql:postgresql:42.5.1")
            .master("local[*]")
            .getOrCreate())

schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Ticker", StringType(), True)
])

def process_spark_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"⚠️ [BATCH {batch_id}] No data received.")
        return

    print(f"📊 [BATCH {batch_id}] Processing {batch_df.count()} records")
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
    cursor = conn.cursor()

    insert_query = f"""
        INSERT INTO stock_realtime (window_start, window_end, ticker, min_open, max_high, min_low, avg_close, close_price, total_volume)
        VALUES %s
        ON CONFLICT (window_start, ticker) DO UPDATE
        SET max_high = EXCLUDED.max_high,
            min_low = EXCLUDED.min_low,
            avg_close = EXCLUDED.avg_close,
            close_price = EXCLUDED.close_price,
            total_volume = EXCLUDED.total_volume;
    """

    batch_data = [(row["window"]["start"], row["window"]["end"], row["Ticker"],
                   row["min_open"], row["max_high"], row["min_low"], row["avg_close"], row["close_price"], row["total_volume"])
                  for row in batch_df.collect()]

    psycopg2.extras.execute_values(cursor, insert_query, batch_data)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ [BATCH {batch_id}] Inserted {len(batch_data)} records")

def process_kafka_stream(spark, end_time_limit=None):
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BROKER)
          .option("subscribe", TOPIC_NAME)
          .option("startingOffsets", "latest")
          .option("maxOffsetsPerTrigger", "100000")
          .load())

    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))

    aggregated_data = (parsed_df
        .groupBy(window(to_timestamp(col("data.Date")).cast("timestamp").alias("local_time"), "10 seconds"), col("data.Ticker"))
        .agg(min("data.Open").alias("min_open"),
             spark_max("data.High").alias("max_high"),
             min("data.Low").alias("min_low"),
             avg("data.Close").alias("avg_close"),
             spark_max("data.Close").alias("close_price"),
             sum("data.Volume").alias("total_volume")))

    query = (aggregated_data
             .writeStream
             .foreachBatch(process_spark_batch)
             .outputMode("update")
             .trigger(processingTime="25 seconds")
             .start())

    skopje_tz = pytz.timezone('Europe/Skopje')
    while query.isActive:
        now = datetime.datetime.now(skopje_tz).time()
        if end_time_limit and now >= end_time_limit:
            print(f"⏹ Reached end time {end_time_limit}. Stopping consumer.")
            query.stop()
            break
        time.sleep(30)

def start_consumer(end_time_limit=None):
    spark = create_spark_session()
    process_kafka_stream(spark, end_time_limit=end_time_limit)

if __name__ == "__main__":
    skopje_tz = pytz.timezone('Europe/Skopje')
    cleared_today = False

    while True:
        now = datetime.datetime.now(skopje_tz)
        current_time = now.time()
        start_time = datetime.time(hour=14, minute=30)
        end_time = datetime.time(hour=21, minute=0)

        if current_time.hour == 0 and 0 <=current_time.minute < 5:
            cleared_today = False
            print("🔁 Reset cleared_today flag at midnight.")

        if current_time.hour == 14 and 0 <= current_time.minute < 20 and not cleared_today:
            clear_stock_realtime_if_2pm()
            cleared_today = True

        if start_time <= current_time <= end_time:
            print(f"🔄 Starting batch producer and consumer at Skopje time {current_time}...")
            producer_thread = threading.Thread(target=start_batch_producer, kwargs={"batch_size": 500, "end_time_limit": end_time}, daemon=True)
            producer_thread.start()
            start_consumer(end_time_limit=end_time)
            break

        print(f"⏳ Waiting for 2:30 PM Skopje time to start (current time: {current_time})...")
        time.sleep(60)
