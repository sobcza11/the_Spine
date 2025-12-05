import duckdb

con = duckdb.connect("data/warehouse/spine.duckdb")

df = con.execute(
    """
    SELECT
        event_id,
        event_date,
        inflation_risk,
        growth_risk,
        policy_bias,
        beige_inflation_risk,
        beige_growth_risk,
        beige_policy_bias,
        minutes_inflation_risk,
        minutes_growth_risk,
        minutes_policy_bias
    FROM fedspeak.combined_policy_leaf
    ORDER BY event_date
    LIMIT 20
    """
).fetchdf()

print(df)
