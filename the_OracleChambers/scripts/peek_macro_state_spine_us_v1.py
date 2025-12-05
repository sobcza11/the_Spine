import duckdb

con = duckdb.connect()

df = con.execute(
    """
    SELECT *
    FROM read_parquet('data/processed/fusion/macro_state_spine_us.parquet')
    ORDER BY date
    LIMIT 20
    """
).fetchdf()

print(df)
