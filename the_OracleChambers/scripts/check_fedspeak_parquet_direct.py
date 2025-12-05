import duckdb

parquet_path = "data/processed/FedSpeak/combined_policy_leaf_v1risk.parquet"

con = duckdb.connect()

df = con.execute(
    f"""
    SELECT
        event_id,
        event_date,
        inflation_risk,
        growth_risk,
        policy_bias
    FROM read_parquet('{parquet_path}')
    ORDER BY event_id
    LIMIT 20
    """
).fetchdf()

print(df)
