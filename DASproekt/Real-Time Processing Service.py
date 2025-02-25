from kafka import KafkaProducer
import json
import yfinance as yf
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_send(symbol: str):
    stock = yf.Ticker(symbol)
    data = stock.history(period="1d", interval="1m").iloc[-1].to_dict()
    data["symbol"] = symbol
    producer.send("stock_data", data)
    print(f"Sent: {data}")

if __name__ == "__main__":
    while True:
        fetch_and_send("AAPL")
        time.sleep(60)  # Fetch data every minute
