from confluent_kafka import Producer
import json
import time
import pandas as pd
import os

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stock-data"

producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
}

producer = Producer(producer_config)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def send_stock_data():
    data_folder = "stock_data"
    if not os.path.exists(data_folder):
        print("No data available. Run the scraper first.")
        return

    for file in os.listdir(data_folder):
        if file.endswith(".csv") and file != "summary.csv":
            stock_df = pd.read_csv(os.path.join(data_folder, file))
            ticker = file.replace(".csv", "")
            for _, row in stock_df.iterrows():
                stock_data = row.to_dict()
                stock_data["Ticker"] = ticker

                # Convert data to JSON string
                msg = json.dumps(stock_data)

                # Produce the message
                producer.produce(TOPIC_NAME, msg.encode('utf-8'), callback=delivery_report)

                # Wait for any outstanding messages to be delivered
                producer.poll(0)

                print(f"Sent: {stock_data}")
                time.sleep(1)

    # Wait for all messages to be delivered
    producer.flush()


if __name__ == "__main__":
    send_stock_data()