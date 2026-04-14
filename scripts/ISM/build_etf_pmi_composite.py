from pathlib import Path
import pandas as pd

# =============================================================================
# PATHS
# =============================================================================

REPO_ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

MANU_PATH = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\IsoVector\_py\r2_tmp\us_ism_manu_pmi_by_industry_canonical.parquet")
NONMANU_PATH = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\IsoVector\_py\r2_tmp\us_ism_nonmanu_pmi_by_industry_canonical.parquet")
MAPPING_PATH = REPO_ROOT / "data" / "spine_us" / "mappings" / "etf_pmi_mapping.csv"

OUTPUT_DIR = REPO_ROOT / "data" / "spine_us" / "derived"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = OUTPUT_DIR / "etf_pmi_composite.parquet"


# =============================================================================
# LOAD
# =============================================================================

df_manu = pd.read_parquet(MANU_PATH).copy()
df_nonmanu = pd.read_parquet(NONMANU_PATH).copy()
df_map = pd.read_csv(MAPPING_PATH).copy()


# =============================================================================
# NORMALIZE
# =============================================================================

df_manu["as_of_date"] = pd.to_datetime(df_manu["as_of_date"], errors="coerce")
df_nonmanu["as_of_date"] = pd.to_datetime(df_nonmanu["as_of_date"], errors="coerce")

df_manu["pmi_type"] = "manu"
df_nonmanu["pmi_type"] = "nonmanu"

for df in (df_manu, df_nonmanu):
    df["sector_name"] = df["sector_name"].astype(str).str.replace("_pmi", "", regex=False).str.strip()
    df["pmi_value"] = pd.to_numeric(df["pmi_value"], errors="coerce")

df_map["pmi_type"] = df_map["pmi_type"].astype(str).str.strip().str.lower()
df_map["industry_name"] = df_map["industry_name"].astype(str).str.strip()
df_map["etf_symbol"] = df_map["etf_symbol"].astype(str).str.strip().str.upper()
df_map["weight"] = pd.to_numeric(df_map["weight"], errors="coerce")
df_map["active_flag"] = pd.to_numeric(df_map["active_flag"], errors="coerce").fillna(0).astype(int)

df_map = df_map[df_map["active_flag"] == 1].copy()

df_pmi = pd.concat([df_manu, df_nonmanu], axis=0, ignore_index=True)


# =============================================================================
# JOIN
# =============================================================================

df = df_pmi.merge(
    df_map,
    left_on=["pmi_type", "sector_name"],
    right_on=["pmi_type", "industry_name"],
    how="inner",
)

if df.empty:
    raise ValueError("ETF PMI composite join produced zero rows. Check PMI sector names vs mapping file.")

df["weighted_pmi"] = df["pmi_value"] * df["weight"]


# =============================================================================
# ETF COMPOSITE
# =============================================================================

df_comp = (
    df.groupby(["as_of_date", "pmi_type", "etf_symbol"], as_index=False)
      .agg(
          weighted_pmi_sum=("weighted_pmi", "sum"),
          weight_sum=("weight", "sum"),
          contributing_industries=("industry_name", "nunique"),
      )
)

df_comp["pmi_composite"] = df_comp["weighted_pmi_sum"] / df_comp["weight_sum"]

df_comp = df_comp.sort_values(["pmi_type", "etf_symbol", "as_of_date"]).reset_index(drop=True)

df_comp["pmi_composite_prev"] = (
    df_comp.groupby(["pmi_type", "etf_symbol"])["pmi_composite"].shift(1)
)

df_comp["pmi_composite_mom"] = df_comp["pmi_composite"] - df_comp["pmi_composite_prev"]


# =============================================================================
# CONTRIBUTOR DETAIL
# =============================================================================

df_detail = (
    df.groupby(["as_of_date", "pmi_type", "etf_symbol", "industry_name"], as_index=False)
      .agg(
          pmi_value=("pmi_value", "mean"),
          weight=("weight", "mean"),
          weighted_pmi=("weighted_pmi", "sum"),
      )
)

df_detail["contribution_pct_of_sum"] = df_detail.groupby(
    ["as_of_date", "pmi_type", "etf_symbol"]
)["weighted_pmi"].transform(lambda s: s / s.sum() if s.sum() != 0 else 0.0)

df_detail = df_detail.sort_values(
    ["pmi_type", "etf_symbol", "as_of_date", "weighted_pmi"],
    ascending=[True, True, True, False]
).reset_index(drop=True)


# =============================================================================
# FINAL OUTPUT
# =============================================================================

# Keep summary panel-ready rows
out = df_comp[
    [
        "as_of_date",
        "pmi_type",
        "etf_symbol",
        "pmi_composite",
        "pmi_composite_prev",
        "pmi_composite_mom",
        "weighted_pmi_sum",
        "weight_sum",
        "contributing_industries",
    ]
].copy()

out["leaf_group"] = "EQUITIES_PMI"
out["leaf_name"] = "ETF_PMI_COMPOSITE"
out["source_system"] = "PMI_CANONICAL_PLUS_ETF_MAPPING"
out["updated_at"] = pd.Timestamp.utcnow()

out = out[
    [
        "as_of_date",
        "pmi_type",
        "etf_symbol",
        "pmi_composite",
        "pmi_composite_prev",
        "pmi_composite_mom",
        "weighted_pmi_sum",
        "weight_sum",
        "contributing_industries",
        "leaf_group",
        "leaf_name",
        "source_system",
        "updated_at",
    ]
].sort_values(["pmi_type", "etf_symbol", "as_of_date"]).reset_index(drop=True)

out.to_parquet(OUTPUT_PATH, index=False)


# =============================================================================
# PRINT
# =============================================================================

print(f"saved: {OUTPUT_PATH}")
print("\nshape:", out.shape)
print("\ndate range:", out["as_of_date"].min(), "->", out["as_of_date"].max())
print("\netfs:", sorted(out["etf_symbol"].dropna().unique().tolist()))
print("\npmi_types:", sorted(out["pmi_type"].dropna().unique().tolist()))
print("\nlatest rows:")
print(out.groupby(["pmi_type", "etf_symbol"], group_keys=False).tail(1).to_string(index=False))

