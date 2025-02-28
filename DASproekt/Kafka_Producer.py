from kafka import KafkaProducer
import json
import time
import pandas as pd
import os

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stock-data"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

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
                producer.send(TOPIC_NAME, stock_data)
                print(f"Sent: {stock_data}")
                time.sleep(1)

if __name__ == "__main__":
    send_stock_data()
