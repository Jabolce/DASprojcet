import psycopg2

PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "stock_data"
PG_USER = "postgres"
PG_PASSWORD = "Cheddar92$"

create_table_query = """
CREATE TABLE IF NOT EXISTS stock_aggregated_data (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    ticker TEXT,
    min_open DOUBLE PRECISION,
    max_high DOUBLE PRECISION,
    min_low DOUBLE PRECISION,
    avg_close DOUBLE PRECISION,
    total_volume DOUBLE PRECISION,
    PRIMARY KEY (window_start, ticker)
);
"""

def create_postgres_table():
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("✅ Table 'stock_aggregated_data' created successfully.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error creating table: {e}")

if __name__ == "__main__":
    create_postgres_table()