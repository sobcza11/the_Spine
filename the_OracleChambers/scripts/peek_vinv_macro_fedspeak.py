import duckdb
import pandas as pd

print("\n[peek_vinv_macro_fedspeak] Connecting to DuckDB...\n")

con = duckdb.connect("data/warehouse/spine.duckdb")

sql = """
SELECT
    v.date,
    v.vinv_score,
    v.econ_score,
    v.inflation_score,
    v.wti_pressure_score,
    v.avg_policy_bias,
    v.avg_inflation_risk,
    v.avg_growth_risk
FROM equity.vinv_signal AS v
ORDER BY v.date
LIMIT 20
"""

df = con.execute(sql).fetchdf()

print("[peek_vinv_macro_fedspeak] Sample output:\n")
print(df)
print("\nRows returned:", len(df))

