from confluent_kafka import Producer
import json
import os
import pandas as pd
import time

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stock-data"

producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'acks': 'all',  # Acknowledge once the leader writes the message
    'linger.ms': 1000,  # Wait to batch messages
    'batch.size': 16384,  # Send messages in batches
    'compression.type': 'snappy'  # Compress messages
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}]')

def send_stock_data():
    data_folder = "stock_data"
    if not os.path.exists(data_folder):
        print("⚠️ No data available. Run the scraper first.")
        return

    batch = []  # Store messages in a batch
    batch_size = 100  # Process 50 messages at once
    delay = 0.01  # Add a delay of 500ms between batches

    for file in os.listdir(data_folder):
        if file.endswith(".csv") and file != "summary.csv":
            stock_df = pd.read_csv(os.path.join(data_folder, file))
            ticker = file.replace(".csv", "")
            for _, row in stock_df.iterrows():
                stock_data = row.to_dict()
                stock_data["Ticker"] = ticker
                batch.append(json.dumps(stock_data).encode('utf-8'))

                # Send in batches of 50 messages
                if len(batch) >= batch_size:
                    for msg in batch:
                        producer.produce(TOPIC_NAME, msg, callback=delivery_report)
                    producer.flush()  # Send all at once
                    batch.clear()  # Reset batch
                    time.sleep(delay)  # Add delay between batches

    # Send remaining messages
    if batch:
        for msg in batch:
            producer.produce(TOPIC_NAME, msg, callback=delivery_report)
        producer.flush()

    print("✅ All messages sent!")

if __name__ == "__main__":
    send_stock_data()
