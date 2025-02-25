import requests
import logging
from flask import Flask, jsonify

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# Create a Flask application
app = Flask(__name__)

# Fetch data from Alpha Vantage using the environment variable API key
def fetch_external_data():
    api_key = "4CLDXO0484YNISTF"  # Hardcoded API key for testing purposes
    if not api_key:
        logging.error("API key for Alpha Vantage not found.")
        return {"error": "API key not found"}

    # Alpha Vantage endpoint for intraday data (for AAPL stock in this example)
    api_url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",  # Intraday data
        "symbol": "AAPL",  # Example stock symbol (Apple)
        "interval": "5min",  # Data interval (5 minutes)
        "apikey": api_key
    }

    # Make the API request
    response = requests.get(api_url, params=params)

    if response.status_code != 200:
        logging.error(f"Failed to fetch external data: {response.status_code}")
        return {"error": "Failed to fetch data"}

    # Return the response as JSON
    return response.json()

# Create a route to display the data in the browser
@app.route('/stock_data', methods=['GET'])
def stock_data():
    data = fetch_external_data()

    # If there is an error in fetching data
    if "error" in data:
        return jsonify(data), 500  # Return error with 500 status code

    # If data contains time series, return it in a nice format
    if "Time Series (5min)" in data:
        time_series = data["Time Series (5min)"]
        most_recent_time = list(time_series.keys())[0]
        return jsonify({
            "Most recent data for AAPL": {
                "time": most_recent_time,
                "data": time_series[most_recent_time]
            }
        })
    else:
        return jsonify({"error": "No time series data found."}), 404

# Run the Flask application
if __name__ == '__main__':
    app.run(debug=True)
