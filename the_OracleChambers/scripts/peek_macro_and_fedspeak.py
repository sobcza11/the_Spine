import duckdb

con = duckdb.connect("data/warehouse/spine.duckdb")

dates = con.execute(
    """
    SELECT DISTINCT DATE(event_date) AS d
    FROM fedspeak.combined_policy_leaf
    ORDER BY d
    LIMIT 20
    """
).fetchdf()
print("Sample FedSpeak dates:\n", dates)

# See what's in the macro & fedspeak schemas
schemas = con.execute(
    """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema IN ('macro', 'fedspeak')
    ORDER BY table_schema, table_name
    """
).fetchdf()
print("Tables in macro & fedspeak:\n", schemas, "\n")

# Column info for macro.econ_leaf
macro_cols = con.execute("PRAGMA table_info('macro.econ_leaf')").fetchdf()
print("macro.econ_leaf columns:\n", macro_cols, "\n")

# Column info for fedspeak.combined_policy_leaf
policy_cols = con.execute("PRAGMA table_info('fedspeak.combined_policy_leaf')").fetchdf()
print("fedspeak.combined_policy_leaf columns:\n", policy_cols, "\n")

# Join macro econ placeholder with FedSpeak policy leaf on date
joined = con.execute(
    """
    SELECT
        m.date,
        m.econ_score,
        f.event_id,
        f.event_date,
        f.inflation_risk,
        f.growth_risk,
        f.policy_bias
    FROM macro.econ_leaf AS m
    JOIN fedspeak.combined_policy_leaf AS f
      ON m.date = CAST(f.event_date AS DATE)
    LIMIT 10
    """
).fetchdf()

print("Joined sample (macro.econ_leaf × fedspeak.combined_policy_leaf):\n", joined)

