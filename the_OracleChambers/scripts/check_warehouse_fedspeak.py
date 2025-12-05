import duckdb

con = duckdb.connect("data/warehouse/spine.duckdb")

df = con.execute(
    """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = 'fedspeak'
    ORDER BY table_name
    """
).fetchdf()

print(df)

