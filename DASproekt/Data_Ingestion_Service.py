import time

import yfinance as yf
import pandas as pd
from flask import Flask, render_template, request, jsonify
import os
import random
from datetime import datetime, timedelta

app = Flask(__name__)

if not os.path.exists('stock_data'):
    os.makedirs('stock_data')

def get_all_us_tickers():
    exchanges = {
        'nasdaq': 'https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_tickers.txt',
        'nyse': 'https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_tickers.txt',
        'amex': 'https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex_tickers.txt'
    }
    tickers = []
    for exchange, url in exchanges.items():
        try:
            exchange_tickers = pd.read_csv(url, header=None)[0].tolist()
            tickers.extend(exchange_tickers)
        except Exception as e:
            print(f"Error fetching tickers from {exchange}: {e}")
    return list(set(tickers))


def fetch_stock_data(tickers, period="1mo"):
    all_data = {}
    batch_size = 25
    max_retries = 5

    for i, start in enumerate(range(0, len(tickers), batch_size)):
        batch = tickers[start:start + batch_size]
        attempt = 0

        while attempt < max_retries:
            try:
                data = yf.download(tickers=batch, period=period, group_by='ticker', auto_adjust=True, threads=True)

                for ticker in batch:
                    try:
                        ticker_data = data[ticker] if len(batch) > 1 else data
                        if ticker_data.empty:
                            continue
                        ticker_data = ticker_data.reset_index()
                        ticker_data['Ticker'] = ticker
                        file_path = f'stock_data/{ticker}.csv'
                        ticker_data.to_csv(file_path, index=False)
                        all_data[ticker] = {k: str(v) if not pd.isna(v) else "" for k, v in ticker_data.iloc[-1].to_dict().items()}
                    except Exception as e:
                        print(f"Error processing {ticker}: {e}")

                break

            except Exception as e:
                print(f"Error downloading batch (Attempt {attempt + 1}): {e}")
                attempt += 1
                sleep_time = min(30 * (2 ** attempt), 120)
                print(f"Retrying after {sleep_time} seconds...")
                time.sleep(sleep_time)

        if (i + 1) % 3 == 0:
            sleep_time = random.randint(10, 20)
            print(f"Sleeping for {sleep_time} seconds to avoid rate limit...")
            time.sleep(sleep_time)

    # Save summary CSV
    summary_df = pd.DataFrame.from_dict(all_data, orient='index')
    summary_df.to_csv('stock_data/summary.csv', na_rep="")

    return all_data


@app.route('/')
def home():
    if not os.path.exists('stock_data/summary.csv'):
        return render_template('home.html', has_data=False)
    mod_time = os.path.getmtime('stock_data/summary.csv')
    mod_datetime = datetime.fromtimestamp(mod_time)
    data_age = datetime.now() - mod_datetime
    summary = pd.read_csv('stock_data/summary.csv')
    stats = {
        'total_stocks': len(summary),
        'last_updated': mod_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        'data_age_hours': round(data_age.total_seconds() / 3600, 1)
    }
    return render_template('home.html', has_data=True, stats=stats)

@app.route('/run-scraper')
def run_scraper():
    period = request.args.get('period', '1mo')
    tickers = get_all_us_tickers()
    result = fetch_stock_data(tickers, period)
    return jsonify({'status': 'success', 'tickers_processed': len(result)})

@app.route('/view-data')
def view_data():
    if not os.path.exists('stock_data/summary.csv'):
        return "No data available. Please run the scraper first."
    summary = pd.read_csv('stock_data/summary.csv')
    return render_template('view_data.html', data=summary.to_dict(orient='records'), columns=summary.columns.tolist())

if __name__ == "__main__":
    app.run(debug=True)
