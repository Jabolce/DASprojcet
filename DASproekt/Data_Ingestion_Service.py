import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
import time

# PostgreSQL Config
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "stock_data"
PG_USER = "postgres"
PG_PASSWORD = "Cheddar92$"

# Ensure stock_data folder exists
if not os.path.exists('stock_data'):
    os.makedirs('stock_data')

def get_tickers_from_csv_file(csv_path='ticker_list.csv'):
    df = pd.read_csv(csv_path)
    return df['Ticker'].dropna().to_list()

def get_tickers_from_stock_data():
    return get_tickers_from_csv_file('ticker_list.csv')

def fetch_data_for_tickers(tickers, batch_size=20):
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    all_postgres_data = []

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        print(f"📦 Fetching 1-day data for batch: {batch}")
        try:
            data = yf.download(
                tickers=batch,
                period="1d",
                group_by="ticker",
                threads=True,
                auto_adjust=True,
                progress=False
            )

            for ticker in batch:
                ticker_data = data[ticker] if len(batch) > 1 else data

                if ticker_data.empty:
                    print(f"⚠️ No 1-day data for {ticker}. Skipping...")
                    continue

                ticker_data = ticker_data.reset_index()
                ticker_data['Date'] = pd.to_datetime(ticker_data['Date'], errors='coerce').dt.date
                ticker_data['Ticker'] = ticker

                for _, row in ticker_data.iterrows():
                    volume_value = row['Volume']
                    if pd.isna(volume_value):
                        volume_value = 0
                    else:
                        if isinstance(volume_value, pd.Series):
                            volume_value = volume_value.iloc[0]
                        volume_value = int(volume_value)

                    all_postgres_data.append((
                        row['Date'], round(row['Open'], 2), round(row['High'], 2),
                        round(row['Low'], 2), round(row['Close'], 2),
                        volume_value, ticker
                    ))

        except Exception as e:
            print(f"❌ Batch error for {batch}: {e}")
            continue

        time.sleep(5)  # to avoid rate limits

    return all_postgres_data


def insert_into_postgres(data):
    if not data:
        print("⚠️ No data to insert into PostgreSQL.")
        return

    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD
        )
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO stock_history (date, open, high, low, close, volume, ticker)
            VALUES %s
            ON CONFLICT (date, ticker) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """

        psycopg2.extras.execute_values(cursor, insert_query, data)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Inserted/updated {len(data)} records into PostgreSQL.")
    except Exception as e:
        print(f"❌ PostgreSQL error: {e}")

if __name__ == "__main__":
    print("🔍 Reading tickers from stock_data folder...")
    tickers = get_tickers_from_stock_data()
    if not tickers:
        print("⚠️ No tickers found in stock_data folder.")
    else:
        print(f"✅ Found {len(tickers)} tickers. Fetching data...")
        data = fetch_data_for_tickers(tickers)
        insert_into_postgres(data)
