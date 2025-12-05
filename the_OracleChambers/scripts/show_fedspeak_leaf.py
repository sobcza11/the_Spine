import duckdb

print("\nReading Parquet file...")
df = duckdb.query(
    "SELECT * FROM 'data/processed/FedSpeak/combined_policy_leaf.parquet' LIMIT 50"
).df()

print("\n=== FIRST 50 ROWS ===\n")
print(df)

print("\n=== SCHEMA ===\n")
schema = duckdb.query(
    "PRAGMA table_info('data/processed/FedSpeak/combined_policy_leaf.parquet')"
).df()
print(schema)
