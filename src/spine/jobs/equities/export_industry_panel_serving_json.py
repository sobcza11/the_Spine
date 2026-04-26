from pathlib import Path
import json
import pandas as pd

REPO_ROOT = Path.cwd()

IN_PATH = REPO_ROOT / "data" / "serving_build" / "industry_panel_serving.parquet"
OUT_PATH = REPO_ROOT / "data" / "serving" / "equities" / "industry_panel_serving.json"


def main():
    df = pd.read_parquet(IN_PATH).copy()

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["etf"] = df["etf"].astype(str).str.strip().str.upper()
    df["pmi_type"] = df["pmi_type"].astype(str).str.strip().str.lower()
    df["industry"] = df["industry"].astype(str).str.strip()

    numeric_cols = [
        "pmi_Idx",
        "no_Idx",
        "pmi_3M_Δ",
        "pmi_6M_Δ",
        "no_3M_Δ",
        "no_6M_Δ",
        "Sig",
        "Wt",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["etf", "pmi_type", "industry", "date"]).reset_index(drop=True)

    rows = []
    for _, row in df.iterrows():
        rows.append({
            "date": row["date"],
            "etf": row["etf"],
            "pmi_type": row["pmi_type"],
            "industry": row["industry"],
            "pmi_Idx": None if pd.isna(row["pmi_Idx"]) else float(row["pmi_Idx"]),
            "no_Idx": None if pd.isna(row["no_Idx"]) else float(row["no_Idx"]),
            "pmi_3M_Δ": None if pd.isna(row["pmi_3M_Δ"]) else float(row["pmi_3M_Δ"]),
            "pmi_6M_Δ": None if pd.isna(row["pmi_6M_Δ"]) else float(row["pmi_6M_Δ"]),
            "no_3M_Δ": None if pd.isna(row["no_3M_Δ"]) else float(row["no_3M_Δ"]),
            "no_6M_Δ": None if pd.isna(row["no_6M_Δ"]) else float(row["no_6M_Δ"]),
            "Sig": None if pd.isna(row["Sig"]) else float(row["Sig"]),
            "Wt": None if pd.isna(row["Wt"]) else float(row["Wt"]),
        })

    payload = {
        "meta": {
            "source": "industry_panel_serving.parquet",
            "latest_date": max(r["date"] for r in rows) if rows else None,
            "row_count": len(rows),
            "schema": [
                "date",
                "etf",
                "pmi_type",
                "industry",
                "pmi_Idx",
                "no_Idx",
                "pmi_3M_Δ",
                "pmi_6M_Δ",
                "no_3M_Δ",
                "no_6M_Δ",
                "Sig",
                "Wt",
            ],
        },
        "rows": rows,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print("DONE")
    print("Saved:", OUT_PATH)
    print("Rows:", len(rows))


if __name__ == "__main__":
    main()
    