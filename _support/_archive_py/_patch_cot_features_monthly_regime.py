import pandas as pd
from datetime import datetime, timezone

P = "data/cot/cot_features_monthly_hybrid.parquet"
df = pd.read_parquet(P)

df["month"] = pd.to_datetime(df["month"])

# overwrite regime deterministically by time coverage
df["cot_regime"] = "unknown"
df.loc[df["month"] <= pd.Timestamp("1989-12-31"), "cot_regime"] = "dea_legacy"
df.loc[df["month"] >= pd.Timestamp("2006-06-30"), "cot_regime"] = "tff"

df["regime_patched_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

df.to_parquet(P, index=False)

print("patched:", P)
print(df["cot_regime"].value_counts(dropna=False))
print("month min/max:", df["month"].min(), df["month"].max())
