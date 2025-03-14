import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import io
import base64
from flask import Flask, render_template, request, jsonify
from sqlalchemy import create_engine
import os

# Set matplotlib to use a non-interactive backend
matplotlib.use('Agg')

# Create Flask app
app = Flask(__name__)

# Database credentials
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "stock_data"
PG_USER = "postgres"
PG_PASSWORD = "Cheddar92$"

# Create SQLAlchemy engine
engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}')

try:
    with engine.connect() as connection:
        result = connection.execute("SELECT 1;")
        print("Database Connected Successfully!")
except Exception as e:
    print(f"Database Connection Failed: {e}")

# Function to get available tickers
def get_tickers():
    try:
        query = "SELECT DISTINCT ticker FROM stock_moving_averages;"
        df = pd.read_sql(query, engine)
        return df['ticker'].tolist()
    except Exception as e:
        print(f"Error getting tickers: {e}")
        return []


# Function to generate plot
def generate_plot(tickers=None, days=500):
    try:
        # Validate input
        if not isinstance(days, int) or days <= 0:
            days = 500  # Default value

        # Construct query based on selected tickers
        if tickers and len(tickers) > 0:
            tickers_str = "', '".join(tickers)
            query = f"""
                SELECT window_start, ticker, avg_close FROM stock_moving_averages 
                WHERE ticker IN ('{tickers_str}')
                AND window_start >= (
                    SELECT MIN(window_start) FROM (
                        SELECT DISTINCT window_start FROM stock_moving_averages
                        WHERE ticker IN ('{tickers_str}')
                        ORDER BY window_start DESC
                        LIMIT {days}
                    ) AS recent_dates
                )
                ORDER BY window_start ASC;
            """
        else:
            query = f"""
                SELECT window_start, ticker, avg_close FROM stock_moving_averages 
                WHERE window_start >= (
                    SELECT MIN(window_start) FROM (
                        SELECT DISTINCT window_start FROM stock_moving_averages
                        ORDER BY window_start DESC
                        LIMIT {days}
                    ) AS recent_dates
                )
                ORDER BY window_start ASC;
            """

        # Execute the query
        df = pd.read_sql(query, engine)

        if df.empty:
            return generate_empty_plot()

        df['window_start'] = pd.to_datetime(df['window_start'])

        plt.figure(figsize=(12, 6))
        for ticker in df['ticker'].unique():
            ticker_df = df[df['ticker'] == ticker]
            plt.plot(ticker_df['window_start'], ticker_df['avg_close'], label=ticker)

        plt.xlabel('Date')
        plt.ylabel('Average Closing Price')
        plt.title('Stock Moving Averages')
        plt.legend()
        plt.grid()
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        plt.close()
        buf.seek(0)

        return base64.b64encode(buf.read()).decode('utf-8')
    except Exception as e:
        print(f"Error generating plot: {e}")
        return ""


# Function to generate an empty plot
def generate_empty_plot():
    plt.figure(figsize=(12, 6))
    plt.text(0.5, 0.5, 'No data available', horizontalalignment='center',
             verticalalignment='center', transform=plt.gca().transAxes, fontsize=14)
    plt.xlabel('Date')
    plt.ylabel('Average Closing Price')
    plt.title('Stock Moving Averages - No Data')
    plt.grid()

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


# Routes
@app.route('/')
def index():
    try:
        tickers = get_tickers()
        img_str = generate_plot()
        return render_template('index.html', img_data=img_str, tickers=tickers)
    except Exception as e:
        return f"An error occurred: {e}", 500


@app.route('/update_chart', methods=['POST'])
def update_chart():
    try:
        selected_tickers = request.form.getlist('tickers')
        days = request.form.get('days', 500)
        days = int(days) if days.isdigit() else 500
        img_str = generate_plot(selected_tickers, days)
        return jsonify({'img_data': img_str})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
