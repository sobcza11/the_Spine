from pathlib import Path
import json
import pandas as pd

# =============================================================================
# PATHS
# =============================================================================

REPO_ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

INPUT_PATH = REPO_ROOT / "data" / "spine_us" / "derived" / "etf_pmi_composite.parquet"

OUTPUT_DIR = REPO_ROOT / "artifacts" / "json" / "equities"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = OUTPUT_DIR / "etf_pmi_composite.json"


# =============================================================================
# LOAD
# =============================================================================

df = pd.read_parquet(INPUT_PATH).copy()

if df.empty:
    raise ValueError(f"Input parquet is empty: {INPUT_PATH}")

df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

df = df.dropna(subset=["as_of_date"]).copy()
df = df.sort_values(["pmi_type", "etf_symbol", "as_of_date"]).reset_index(drop=True)


# =============================================================================
# BUILD JSON STRUCTURE
# =============================================================================

payload = {
    "meta": {
        "dataset": "ETF_PMI_COMPOSITE",
        "source_system": "PMI_CANONICAL_PLUS_ETF_MAPPING",
        "date_min": df["as_of_date"].min().strftime("%Y-%m-%d"),
        "date_max": df["as_of_date"].max().strftime("%Y-%m-%d"),
        "etfs": sorted(df["etf_symbol"].dropna().unique().tolist()),
        "pmi_types": sorted(df["pmi_type"].dropna().unique().tolist()),
        "updated_at_utc": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "row_count": int(len(df)),
    },
    "latest": {},
    "history": {},
}

for pmi_type in sorted(df["pmi_type"].dropna().unique()):
    df_type = df[df["pmi_type"] == pmi_type].copy()

    payload["latest"][pmi_type] = {}
    payload["history"][pmi_type] = {}

    for etf_symbol in sorted(df_type["etf_symbol"].dropna().unique()):
        df_etf = df_type[df_type["etf_symbol"] == etf_symbol].copy()

        if df_etf.empty:
            continue

        latest = df_etf.iloc[-1]

        payload["latest"][pmi_type][etf_symbol] = {
            "as_of_date": latest["as_of_date"].strftime("%Y-%m-%d"),
            "pmi_composite": None if pd.isna(latest["pmi_composite"]) else round(float(latest["pmi_composite"]), 6),
            "pmi_composite_prev": None if pd.isna(latest["pmi_composite_prev"]) else round(float(latest["pmi_composite_prev"]), 6),
            "pmi_composite_mom": None if pd.isna(latest["pmi_composite_mom"]) else round(float(latest["pmi_composite_mom"]), 6),
            "weighted_pmi_sum": None if pd.isna(latest["weighted_pmi_sum"]) else round(float(latest["weighted_pmi_sum"]), 6),
            "weight_sum": None if pd.isna(latest["weight_sum"]) else round(float(latest["weight_sum"]), 6),
            "contributing_industries": int(latest["contributing_industries"]),
            "leaf_group": str(latest["leaf_group"]),
            "leaf_name": str(latest["leaf_name"]),
            "source_system": str(latest["source_system"]),
            "updated_at": None if pd.isna(latest["updated_at"]) else latest["updated_at"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        payload["history"][pmi_type][etf_symbol] = [
            {
                "as_of_date": row["as_of_date"].strftime("%Y-%m-%d"),
                "pmi_composite": None if pd.isna(row["pmi_composite"]) else round(float(row["pmi_composite"]), 6),
                "pmi_composite_prev": None if pd.isna(row["pmi_composite_prev"]) else round(float(row["pmi_composite_prev"]), 6),
                "pmi_composite_mom": None if pd.isna(row["pmi_composite_mom"]) else round(float(row["pmi_composite_mom"]), 6),
                "weighted_pmi_sum": None if pd.isna(row["weighted_pmi_sum"]) else round(float(row["weighted_pmi_sum"]), 6),
                "weight_sum": None if pd.isna(row["weight_sum"]) else round(float(row["weight_sum"]), 6),
                "contributing_industries": int(row["contributing_industries"]),
            }
            for _, row in df_etf.iterrows()
        ]


# =============================================================================
# SAVE
# =============================================================================

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print(f"saved: {OUTPUT_PATH}")
print("date range:", payload["meta"]["date_min"], "->", payload["meta"]["date_max"])
print("etfs:", payload["meta"]["etfs"])
print("pmi_types:", payload["meta"]["pmi_types"])
print("row_count:", payload["meta"]["row_count"])
print("\nlatest manu XLI:", payload["latest"].get("manu", {}).get("XLI"))
print("latest nonmanu XLI:", payload["latest"].get("nonmanu", {}).get("XLI"))

