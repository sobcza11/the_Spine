from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

ROOT = Path.cwd()

xlsx_path = ROOT / "data" / "ism" / "ism_pmi_transp.xlsx"
out_dir = ROOT / "data" / "ism"

out_dir.mkdir(parents=True, exist_ok=True)

# ============================================================
# CONFIG
# ============================================================

TARGETS = [
    ("manu_pmi", "manu", "pmi_value", "_pmi"),
    ("m_no", "manu", "new_orders_value", "_no"),

    ("serv_pmi", "nonmanu", "pmi_value", "_pmi"),
    ("serv_no", "nonmanu", "new_orders_value", "_no"),
]

# ============================================================
# HELPERS
# ============================================================

def melt_sheet(df, value_name, suffix):

    if "dates" not in df.columns:
        raise RuntimeError("Missing dates column")

    value_cols = [c for c in df.columns if c != "dates"]

    melted = df.melt(
        id_vars=["dates"],
        value_vars=value_cols,
        var_name="sector_col",
        value_name=value_name,
    )

    melted = melted.dropna(subset=[value_name])

    melted["sector_name"] = (
        melted["sector_col"]
        .str.replace(suffix, "", regex=False)
        .str.strip()
    )

    melted = melted.rename(columns={"dates": "as_of_date"})

    return melted[
        ["as_of_date", "sector_name", value_name]
    ].copy()

# ============================================================
# BUILD
# ============================================================

xls = pd.ExcelFile(xlsx_path)

print("\n[ISM SHEETS]")
for s in xls.sheet_names:
    print("-", s)

for sheet_name, pmi_type, value_name, suffix in TARGETS:

    print(f"\n[PROCESSING] {sheet_name}")

    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)

    df.columns = [str(c).strip() for c in df.columns]

    melted = melt_sheet(df, value_name, suffix)

    melted["as_of_date"] = pd.to_datetime(
        melted["as_of_date"]
    )

    melted["country"] = "US"
    melted["ccy"] = "USD"

    melted["leaf_group"] = "ISM"

    if value_name == "pmi_value":
        leaf_name = f"us_ism_{pmi_type}_pmi_by_industry"
    else:
        leaf_name = f"us_ism_{pmi_type}_no_by_industry"

    melted["leaf_name"] = leaf_name

    melted["source_system"] = (
        f"ISM_{pmi_type.upper()}_EXCEL"
    )

    melted["updated_at"] = datetime.now(timezone.utc)

    cols = [
        "as_of_date",
        "country",
        "ccy",
        "sector_name",
        value_name,
        "leaf_group",
        "leaf_name",
        "source_system",
        "updated_at",
    ]

    melted = melted[cols].sort_values(
        ["as_of_date", "sector_name"]
    )

    # --------------------------------------------------------
    # OUTPUT
    # --------------------------------------------------------

    if value_name == "pmi_value":
        out_name = (
            f"us_ism_{pmi_type}_pmi_by_industry_canonical.parquet"
        )
    else:
        out_name = (
            f"us_ism_{pmi_type}_no_by_industry_canonical.parquet"
        )

    out_path = out_dir / out_name

    melted.to_parquet(out_path, index=False)

    print(
        f"[OK] wrote {out_name} | "
        f"shape={melted.shape}"
    )

print("\n[DONE]")

