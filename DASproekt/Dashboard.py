import psycopg2
from flask import Flask, render_template, jsonify, request, Response
import json
import time
import pytz
from datetime import datetime
from datetime import timedelta


app = Flask(__name__)
ANALYSIS_DIR = "analysis_reports"

# PostgreSQL Config
PG_HOST = "stockdata-eu.postgres.database.azure.com"
PG_PORT = "5432"
PG_DATABASE = "stock_data"
PG_USER = "bingbong"
PG_PASSWORD = "AzureTest123!"

def get_stock_symbols():
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM stock_history;")
        symbols = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return symbols
    except Exception as e:
        print(f"❌ Error fetching stock symbols: {e}")
        return []

def get_historical_data(symbol=None, range_="10y"):
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            dbname=PG_DATABASE, user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
        data = []

        today = datetime.now().date()

        if range_ == "1d":
            # Only today's intraday data
            query = """
                SELECT window_start, close_price FROM stock_realtime
                WHERE ticker = %s AND DATE(window_start) = CURRENT_DATE
                ORDER BY window_start;
            """
            cursor.execute(query, (symbol,))
            data = [{"window_start": row[0].isoformat(), "close_price": row[1]} for row in cursor.fetchall()]

        elif range_ == "5d":
            since_date = today - timedelta(days=5)

            # 1. Get last 4–5 days from stock_history
            history_query = """
                SELECT date AS window_start, close AS close_price FROM stock_history
                WHERE ticker = %s AND date >= %s
                ORDER BY date;
            """
            cursor.execute(history_query, (symbol, since_date))
            history_data = [{"window_start": row[0].isoformat(), "close_price": row[1]} for row in cursor.fetchall()]

            # 2. Add today's intraday from stock_realtime
            realtime_query = """
                SELECT window_start, close_price FROM stock_realtime
                WHERE ticker = %s AND DATE(window_start) = CURRENT_DATE
                ORDER BY window_start;
            """
            cursor.execute(realtime_query, (symbol,))
            realtime_data = [{"window_start": row[0].isoformat(), "close_price": row[1]} for row in cursor.fetchall()]

            data = history_data + realtime_data

        else:
            # Longer ranges — use stock_history + today's intraday if not yet in stock_history
            days_map = {
                "1mo": 30,
                "6mo": 180,
                "1y": 365,
                "5y": 1825,
                "10y": 3650
            }

            days = days_map.get(range_, 3650)
            since_date = today - timedelta(days=days)

            # 1. Fetch historical data
            history_query = """
                SELECT date AS window_start, close AS close_price FROM stock_history
                WHERE ticker = %s AND date >= %s
                ORDER BY date;
            """
            cursor.execute(history_query, (symbol, since_date))
            history_data = cursor.fetchall()
            data = [{"window_start": row[0].isoformat(), "close_price": row[1]} for row in history_data]

            # 2. If today's date is NOT in stock_history, fetch intraday from stock_realtime
            history_dates = {row[0] for row in history_data}
            if today not in history_dates:
                realtime_query = """
                    SELECT window_start, close_price FROM stock_realtime
                    WHERE ticker = %s AND DATE(window_start) = CURRENT_DATE
                    ORDER BY window_start;
                """
                cursor.execute(realtime_query, (symbol,))
                realtime_data = [{"window_start": row[0].isoformat(), "close_price": row[1]} for row in cursor.fetchall()]
                data += realtime_data

        cursor.close()
        conn.close()
        return data

    except Exception as e:
        print(f"❌ Error fetching historical data: {e}")
        return []

def is_market_open():
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    market_open = now_eastern.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_eastern.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_eastern <= market_close

def stream_live_data(symbol=None):
    def event_stream():
        last_sent = None
        while True:
            try:
                conn = psycopg2.connect(
                    host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
                cursor = conn.cursor()

                if is_market_open():
                    query = """
                        SELECT window_start, close_price, ticker 
                        FROM stock_realtime 
                        WHERE ticker = %s 
                        ORDER BY window_start DESC LIMIT 1;
                    """
                else:
                    query = """
                        SELECT window_start, close_price, ticker 
                        FROM stock_realtime
                        WHERE ticker = %s AND DATE(window_start) = CURRENT_DATE 
                        ORDER BY window_start DESC LIMIT 1;
                    """

                cursor.execute(query, (symbol,))
                row = cursor.fetchone()

                if row:
                    data = {
                        'window_start': row[0].isoformat(),
                        'close_price': row[1],
                        'ticker': row[2].upper()
                    }
                    if data != last_sent:
                        yield f"data: {json.dumps(data)}\n\n"
                        last_sent = data

                cursor.close()
                conn.close()
            except Exception as e:
                print(f"❌ Error fetching live data: {e}")

            time.sleep(2 if is_market_open() else 60)

    return Response(event_stream(), mimetype='text/event-stream')


@app.route('/')
def index():
    return render_template('graph.html')

@app.route('/get-stock-symbols')
def stock_symbols():
    return jsonify(get_stock_symbols())

@app.route('/historical-data')
def historical_data():
    symbol = request.args.get("symbol")
    range_ = request.args.get("range", default="10y")  # Default to 10 years if not provided
    print(f"📥 Requested time range: {range_}")

    return jsonify(get_historical_data(symbol, range_))


@app.route('/live-data')
def live_data():
    symbol = request.args.get("symbol")
    return stream_live_data(symbol)

@app.route('/last-price')
def last_price():
    symbol = request.args.get("symbol")
    if not symbol:
        return jsonify({"error": "No symbol provided"}), 400
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT window_start, close_price FROM stock_realtime
            WHERE ticker = %s AND DATE(window_start) = CURRENT_DATE
            ORDER BY window_start DESC LIMIT 1;
        """, (symbol,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            return jsonify({"window_start": row[0].isoformat(), "close_price": row[1]})
        else:
            return jsonify({"error": "No data found"}), 404
    except Exception as e:
        print(f"❌ Error fetching last price: {e}")
        return jsonify({"error": "Server error"}), 500

@app.route('/forecast-data')
def forecast_data():
    symbol = request.args.get("symbol")
    if not symbol:
        return jsonify({"error": "No symbol provided"}), 400

    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            dbname=PG_DATABASE, user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT forecast_date, yhat, yhat_lower, yhat_upper
            FROM stock_analysis
            WHERE ticker = %s
            ORDER BY forecast_date ASC;
        """, (symbol,))
        rows = cursor.fetchall()
        forecast_data = [
            {
                "ds": row[0].isoformat(),
                "yhat": row[1],
                "yhat_lower": row[2],
                "yhat_upper": row[3]
            }
            for row in rows
        ]

        cursor.close()
        conn.close()

        return jsonify(forecast_data)

    except Exception as e:
        print(f"❌ Error fetching forecast data from DB: {e}")
        return jsonify({"error": "Server error"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
