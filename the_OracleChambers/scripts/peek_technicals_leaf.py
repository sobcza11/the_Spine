import duckdb

# Connect to the Spine warehouse DuckDB
con = duckdb.connect("data/warehouse/spine.duckdb")

# Pull a small sample from the technicals leaf
df = con.execute(
    """
    SELECT *
    FROM technicals.technical_leaf
    ORDER BY date
    LIMIT 20
    """
).fetchdf()

print(df)

