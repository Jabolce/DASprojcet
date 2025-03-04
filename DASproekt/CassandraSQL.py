from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Create keyspace
keyspace_creation_query = """
CREATE KEYSPACE IF NOT EXISTS stock_analysis
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
"""
session.execute(keyspace_creation_query)

# Create table for moving averages with handling for potential duplicates
table_creation_query = """
CREATE TABLE IF NOT EXISTS stock_analysis.stock_moving_averages (
    ticker TEXT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_close DOUBLE,
    PRIMARY KEY ((ticker), window_start, window_end)
) WITH CLUSTERING ORDER BY (window_start DESC, window_end DESC)
AND default_time_to_live = 86400;  // Optional: data expires after 24 hours
"""
session.execute(table_creation_query)

print("Cassandra keyspace and table created successfully.")