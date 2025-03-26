import pandas as pd
from prophet import Prophet
import os
from sqlalchemy import create_engine, text
from concurrent.futures import ProcessPoolExecutor

# PostgreSQL Config
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "stock_data"
PG_USER = "postgres"
PG_PASSWORD = "Cheddar92$"

# Output directory
REPORT_DIR = "analysis_reports"
os.makedirs(REPORT_DIR, exist_ok=True)

# SQLAlchemy engine
DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
engine = create_engine(DB_URI)

def get_tickers_from_csv_file(csv_path='ticker_list.csv'):
    df = pd.read_csv(csv_path)
    return df['Ticker'].dropna().tolist()

def get_dynamic_tickers_from_csv():
    return get_tickers_from_csv_file('ticker_list.csv')

def fetch_historical_data(symbol):
    """Fetch historical stock data from stock_history for a given ticker."""
    query = """
        SELECT date, close FROM stock_history
        WHERE ticker = %s ORDER BY date;
    """
    df = pd.read_sql(query, engine, params=(symbol,))
    df['date'] = pd.to_datetime(df['date'])
    df.rename(columns={'date': 'ds', 'close': 'y'}, inplace=True)
    return df

def generate_forecast(df, periods=30):
    """Generate 1-month forecast on business days only using Prophet."""
    model = Prophet(daily_seasonality=True)
    model.fit(df)

    future = model.make_future_dataframe(periods=periods, freq='B')  # 'B' = business day
    forecast = model.predict(future)

    # Keep only forecast rows with dates after the last date in df
    latest_actual_date = df['ds'].max()
    forecast = forecast[forecast['ds'] > latest_actual_date]

    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

def save_forecast(symbol, forecast_df):
    forecast_df = forecast_df.copy()
    forecast_df["ticker"] = symbol
    forecast_df.rename(columns={"ds": "forecast_date"}, inplace=True)

    with engine.begin() as connection:
        # Delete old forecasts for this ticker
        connection.execute(text("DELETE FROM stock_analysis WHERE ticker = :ticker"), {"ticker": symbol})

        # Insert new forecast data
        forecast_df[["ticker", "forecast_date", "yhat", "yhat_lower", "yhat_upper"]].to_sql(
            "stock_analysis",
            con=connection,
            if_exists="append",
            index=False
        )

def run_analysis(symbol):
    print(f"📊 Running forecast for {symbol}...")
    df = fetch_historical_data(symbol)

    if df.empty or df['y'].dropna().shape[0] < 2:
        print(f"⚠️ Not enough data for {symbol}. Skipping...")
        return

    forecast_df = generate_forecast(df, periods=7)
    save_forecast(symbol, forecast_df)

if __name__ == "__main__":
    ticker_list = get_dynamic_tickers_from_csv()
    print(f"🔍 Found {len(ticker_list)} tickers for analysis: {ticker_list}")

    with ProcessPoolExecutor(max_workers=8) as executor:
        executor.map(run_analysis, ticker_list)
