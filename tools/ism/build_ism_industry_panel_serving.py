from pathlib import Path
import json
import pandas as pd
from datetime import datetime, timezone

ROOT = Path.cwd()

ISM_DIR = ROOT / "data" / "ism"
OUT_EQ = ROOT / "data" / "serving" / "equities"
OUT_MACRO = ROOT / "data" / "macro" / "serving"

OUT_EQ.mkdir(parents=True, exist_ok=True)
OUT_MACRO.mkdir(parents=True, exist_ok=True)

FILES = {
    "manu_pmi": ISM_DIR / "us_ism_manu_pmi_by_industry_canonical.parquet",
    "manu_no": ISM_DIR / "us_ism_manu_no_by_industry_canonical.parquet",
    "serv_pmi": ISM_DIR / "us_ism_nonmanu_pmi_by_industry_canonical.parquet",
    "serv_no": ISM_DIR / "us_ism_nonmanu_no_by_industry_canonical.parquet",
}

def load_leaf(path, value_col, pmi_type):
    if not path.exists():
        raise FileNotFoundError(path)

    df = pd.read_parquet(path).copy()

    df = df.rename(columns={
        "as_of_date": "date",
        "sector_name": "industry",
    })

    df["date"] = pd.to_datetime(df["date"])
    df["industry"] = df["industry"].astype(str).str.strip()

    df["industry"] = (
        df["industry"]
        .str.replace("_pmi", "", regex=False)
        .str.replace("_no", "", regex=False)
        .str.strip()
    )

    df["pmi_type"] = pmi_type

    return df[["date", "industry", value_col, "pmi_type"]]

manu_pmi = load_leaf(FILES["manu_pmi"], "pmi_value", "manu")
manu_no = load_leaf(FILES["manu_no"], "new_orders_value", "manu")
serv_pmi = load_leaf(FILES["serv_pmi"], "pmi_value", "serv")
serv_no = load_leaf(FILES["serv_no"], "new_orders_value", "serv")

pmi = pd.concat([manu_pmi, serv_pmi], ignore_index=True)
no = pd.concat([manu_no, serv_no], ignore_index=True)

df = pmi.merge(
    no,
    on=["date", "industry", "pmi_type"],
    how="outer"
)

df = df.rename(columns={
    "pmi_value": "pmi_Idx",
    "new_orders_value": "no_Idx",
})

df = df.sort_values(["pmi_type", "industry", "date"]).reset_index(drop=True)

for col in ["pmi_Idx", "no_Idx"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df["pmi_3M_Δ"] = df.groupby(["pmi_type", "industry"])["pmi_Idx"].diff(3)
df["pmi_6M_Δ"] = df.groupby(["pmi_type", "industry"])["pmi_Idx"].diff(6)
df["no_3M_Δ"] = df.groupby(["pmi_type", "industry"])["no_Idx"].diff(3)
df["no_6M_Δ"] = df.groupby(["pmi_type", "industry"])["no_Idx"].diff(6)

df["Sig"] = (
    df["pmi_Idx"].fillna(0) * 0.50
    + df["no_Idx"].fillna(0) * 0.35
    + df["pmi_3M_Δ"].fillna(0) * 0.075
    + df["no_3M_Δ"].fillna(0) * 0.075
)

df["Wt"] = 1.0
df["updated_at"] = datetime.now(timezone.utc).isoformat()

df["date"] = df["date"].dt.strftime("%Y-%m-%d")

cols = [
    "date",
    "industry",
    "pmi_Idx",
    "no_Idx",
    "pmi_type",
    "pmi_3M_Δ",
    "pmi_6M_Δ",
    "no_3M_Δ",
    "no_6M_Δ",
    "Sig",
    "Wt",
    "updated_at",
]

df = df[cols]

records = json.loads(df.to_json(orient="records"))

industry_panel_path = OUT_EQ / "industry_panel_serving.json"
latest_path = OUT_MACRO / "ism_pmi_latest.json"

industry_panel_path.write_text(
    json.dumps(records, indent=2),
    encoding="utf-8"
)

latest_date = df["date"].max()
latest = df[df["date"] == latest_date].copy()
latest_records = json.loads(latest.to_json(orient="records"))

latest_path.write_text(
    json.dumps(latest_records, indent=2),
    encoding="utf-8"
)

print(f"[OK] wrote {industry_panel_path} | rows={len(records)}")
print(f"[OK] wrote {latest_path} | latest_date={latest_date} | rows={len(latest_records)}")

