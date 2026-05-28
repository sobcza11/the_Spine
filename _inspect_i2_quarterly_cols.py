from pathlib import Path
import pandas as pd

p = Path("data/i2_quarterly/i2_quarterly_statement_panel_v1.parquet")
df = pd.read_parquet(p)

print("COLUMNS:")
for c in df.columns:
    print(c)

print("\nCASH-LIKE COLUMNS:")
for c in df.columns:
    lc = c.lower()
    if any(x in lc for x in ["cash", "cap", "capital", "operat", "invest", "property", "plant", "equipment", "purchase"]):
        print(c)
