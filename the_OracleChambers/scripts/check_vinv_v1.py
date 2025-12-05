import duckdb

con = duckdb.connect()

df = con.execute(
    "SELECT * FROM read_parquet('data/processed/p_Equity_US/vinv_signal.parquet') LIMIT 20"
).fetchdf()

print(df)
