from fastapi import FastAPI
import psycopg2

app = FastAPI()

def get_aggregated_data(symbol: str):
    conn = psycopg2.connect("dbname=stocks user=postgres password=secret")
    cur = conn.cursor()
    cur.execute("SELECT * FROM aggregated_data WHERE symbol = %s", (symbol,))
    return cur.fetchall()

@app.get("/processed/{symbol}")
def processed_data(symbol: str):
    return get_aggregated_data(symbol)
