import pandas as pd
import numpy as np
from datetime import datetime, timezone
from pathlib import Path

IN_PATH  = "data/cot/cot_store_weekly_unified.parquet"
OUT_PATH = "data/cot/cot_features_monthly_hybrid.parquet"
QA_PATH  = "reports/cot/cot_features_monthly_hybrid_QA.md"

PRIMARY_GROUP  = "LEVERAGED_FUNDS"
ALT_GROUP      = "LEVERAGED_FUNDS"

df = pd.read_parquet(IN_PATH).copy()

# --- governance / sanity ---
df["report_date"] = pd.to_datetime(df["report_date"])
df["month"] = df["report_date"].dt.to_period("M").dt.to_timestamp("M")  # month end
df["cot_format"] = df["cot_format"].astype(str)
df["market_key"] = df["market_key"].astype(str).str.strip()

# =========================
# DEA legacy monthly
# =========================
dea = df[df["cot_format"] == "dea_legacy"].copy()

# last weekly print inside month per market
dea = dea.sort_values(["market_key","month","report_date"])
dea_m = (
    dea.groupby(["month","market_key"], as_index=False)
       .tail(1)
       .copy()
)

dea_m["cot_regime"] = "dea_legacy"
dea_m["cot_net_primary"] = dea_m["noncomm_net"] if "noncomm_net" in dea_m.columns else np.nan
dea_m["cot_net_primary_pct_oi"] = dea_m["noncomm_net_pct_oi"] if "noncomm_net_pct_oi" in dea_m.columns else np.nan
dea_m["cot_oi"] = dea_m["open_interest_all"] if "open_interest_all" in dea_m.columns else np.nan

dea_m["dea_traders_total"] = dea_m["traders_total"] if "traders_total" in dea_m.columns else np.nan
dea_m["dea_top4_long_pct"] = dea_m["top4_long_pct"] if "top4_long_pct" in dea_m.columns else np.nan

dea_out = dea_m[[
    "month","market_key","cot_regime",
    "cot_net_primary","cot_net_primary_pct_oi","cot_oi",
    "dea_traders_total","dea_top4_long_pct"
]].copy()

# =========================
# TFF monthly
# =========================
tff = df[df["cot_format"] == "tff"].copy()
tff["trader_group"] = tff["trader_group"].astype(str).str.strip()

if "trader_group" not in tff.columns:
    raise ValueError("TFF rows missing trader_group; expected per your schema output.")

tff = tff.sort_values(["market_key","trader_group","month","report_date"])
tff_m_last = (
    tff.groupby(["month","market_key","trader_group"], as_index=False)
       .tail(1)
       .copy()
)

def extract_group(gname: str, prefix: str) -> pd.DataFrame:
    g = tff_m_last[tff_m_last["trader_group"] == gname].copy()
    return pd.DataFrame({
        "month": g["month"].values,
        "market_key": g["market_key"].values,
        f"{prefix}_net": g["net_contracts"].values if "net_contracts" in g.columns else np.nan,
        f"{prefix}_net_pct_oi": g["group_net_pct_oi"].values if "group_net_pct_oi" in g.columns else np.nan,
        f"{prefix}_oi": g["total_oi_contracts"].values if "total_oi_contracts" in g.columns else np.nan,
        f"{prefix}_z_52w": g["net_contracts_zscore_52w"].values if "net_contracts_zscore_52w" in g.columns else np.nan,
    })

primary = extract_group(PRIMARY_GROUP, "tff_primary")
alt     = extract_group(ALT_GROUP, "tff_alt")

tff_w = primary.merge(alt, on=["month","market_key"], how="outer")

tff_w["cot_regime"] = "tff"
tff_w["cot_net_primary"] = tff_w.get("tff_primary_net")
tff_w["cot_net_primary_pct_oi"] = tff_w.get("tff_primary_net_pct_oi")
tff_w["cot_oi"] = tff_w.get("tff_primary_oi")

tff_w["cot_net_alt"] = tff_w.get("tff_alt_net")
tff_w["cot_net_alt_pct_oi"] = tff_w.get("tff_alt_net_pct_oi")
tff_w["cot_z_52w_primary"] = tff_w.get("tff_primary_z_52w")
tff_w["cot_z_52w_alt"] = tff_w.get("tff_alt_z_52w")

tff_out = tff_w[[
    "month","market_key","cot_regime",
    "cot_net_primary","cot_net_primary_pct_oi","cot_oi",
    "cot_net_alt","cot_net_alt_pct_oi",
    "cot_z_52w_primary","cot_z_52w_alt"
]].copy()

# =========================
# Hybrid union
# =========================
hybrid = pd.concat([dea_out, tff_out], ignore_index=True, sort=False)

hybrid["schema_version"] = "cot_features_monthly_hybrid_v1"
hybrid["built_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

hybrid["month"] = pd.to_datetime(hybrid["month"])
hybrid = hybrid.sort_values(["month","market_key","cot_regime"]).reset_index(drop=True)

# ---- type hygiene (pyarrow-safe) ----
import pandas as pd

def _numify(x):
    # handles "36,006", blanks, None
    return pd.to_numeric(
        pd.Series(x).astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce"
    )

for c in hybrid.columns:
    # only coerce the numeric feature columns (leave ids/dates/strings alone)
    if c.startswith("cot_") or c.startswith("dea_"):
        hybrid[c] = _numify(hybrid[c]).astype("float64")

# ---- guarantee cot_regime is always populated ----
hybrid["cot_regime"] = hybrid.get("cot_regime", pd.NA)
hybrid["cot_regime"] = hybrid["cot_regime"].astype("string")

hybrid.loc[
    hybrid["cot_regime"].isna() & hybrid["month"].notna() & (hybrid["month"] <= pd.Timestamp("1989-12-31")),
    "cot_regime"
] = "dea_legacy"

hybrid.loc[
    hybrid["cot_regime"].isna() & hybrid["month"].notna() & (hybrid["month"] >= pd.Timestamp("2006-06-30")),
    "cot_regime"
] = "tff"

hybrid["cot_regime"] = hybrid["cot_regime"].fillna("unknown")

print("cot_oi dtype:", hybrid["cot_oi"].dtype)
print("cot_oi sample non-null:", hybrid["cot_oi"].dropna().head(5).tolist())
print("cot_oi has commas?", hybrid["cot_oi"].astype(str).str.contains(",").any())

hybrid["cot_regime"] = hybrid["cot_regime"].astype("string").fillna("unknown")
Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
hybrid.to_parquet(OUT_PATH, index=False)

# ---- type hygiene (pyarrow-safe) ----
def _numify(s):
    # handles "36,006", blanks, None
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce")

for c in [
    "cot_net_primary", "cot_net_primary_pct_oi", "cot_oi",
    "cot_net_alt", "cot_net_alt_pct_oi",
    "cot_z_52w_primary", "cot_z_52w_alt",
    "dea_traders_total", "dea_top4_long_pct",
]:
    if c in hybrid.columns:
        hybrid[c] = _numify(hybrid[c])


# =========================
# QA report
# =========================
def nuniq(s):
    return int(pd.Series(s).nunique(dropna=True))

lines = []
lines.append("# COT Hybrid Monthly Features QA")
lines.append("")
lines.append(f"- built_at_utc: `{hybrid['built_at_utc'].iloc[0]}`")
lines.append(f"- schema_version: `{hybrid['schema_version'].iloc[0]}`")
lines.append(f"- input: `{IN_PATH}`")
lines.append(f"- output: `{OUT_PATH}`")
lines.append("")
lines.append("## Coverage")
lines.append(f"- rows: **{len(hybrid):,}**")
lines.append(f"- markets (market_key): **{nuniq(hybrid['market_key']):,}**")
lines.append(f"- month min: **{hybrid['month'].min().date()}**")
lines.append(f"- month max: **{hybrid['month'].max().date()}**")
lines.append("")
lines.append("### By regime")
for regime, g in hybrid.groupby("cot_regime"):
    lines.append(f"- {regime}: rows={len(g):,} | markets={nuniq(g['market_key']):,} | months={nuniq(g['month']):,}")

lines.append("")
lines.append("## Missingness (key outputs)")
key_cols = ["cot_net_primary","cot_net_primary_pct_oi","cot_oi","cot_net_alt","cot_net_alt_pct_oi","cot_z_52w_primary","cot_z_52w_alt"]
for c in key_cols:
    if c in hybrid.columns:
        lines.append(f"- {c}: nulls={int(hybrid[c].isna().sum()):,} ({hybrid[c].isna().mean():.2%})")

lines.append("")
lines.append("## Sanity checks")
pk = ["month","market_key","cot_regime"]
dups = int(hybrid.duplicated(pk).sum())
lines.append(f"- PK candidate: `{pk}` duplicates: **{dups}**")

lines.append("")
lines.append("## Samples")
lines.append("### Head")
lines.append(hybrid.head(8).to_markdown(index=False))
lines.append("")
lines.append("### Tail")
lines.append(hybrid.tail(8).to_markdown(index=False))

Path(QA_PATH).parent.mkdir(parents=True, exist_ok=True)
Path(QA_PATH).write_text("\n".join(lines), encoding="utf-8")

print("saved:", OUT_PATH)
print("saved:", QA_PATH)
print("rows:", len(hybrid), "cols:", len(hybrid.columns))
print("month min/max:", hybrid["month"].min(), hybrid["month"].max())
print("regimes:\n", hybrid["cot_regime"].value_counts())
