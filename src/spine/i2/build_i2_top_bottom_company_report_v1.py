from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def latest_by_symbol(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["symbol", "year"])
    return (
        df.sort_values(["symbol", "year"])
        .groupby("symbol", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )


def build_i2_top_bottom_company_report_v1():
    root = Path.cwd()
    src = root / "data" / "i2" / "i2_corporate_fragility_propagation_v1.parquet"
    out = root / "data" / "i2"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing I2 fragility output: {src}")

    df = pd.read_parquet(src).copy()
    latest = latest_by_symbol(df)

    latest["i2_score"] = pd.to_numeric(latest["i2_score"], errors="coerce")
    latest["i2_fragility_pressure"] = pd.to_numeric(latest["i2_fragility_pressure"], errors="coerce")
    latest["i2_deterioration_pressure"] = pd.to_numeric(latest["i2_deterioration_pressure"], errors="coerce")

    base_cols = [
        "symbol",
        "year",
        "i2_score",
        "i2_state",
        "i2_fragility_pressure",
        "i2_escalation_state",
        "i2_deterioration_pressure",
        "i2_trajectory_state",
        "P",
        "F",
        "L",
        "D",
        "M",
        "X",
        "C",
        "S",
    ]

    available_cols = [c for c in base_cols if c in latest.columns]

    top_i2 = (
        latest.sort_values(["i2_score", "i2_fragility_pressure"], ascending=[False, True])
        .head(25)[available_cols]
        .reset_index(drop=True)
    )

    bottom_i2 = (
        latest.sort_values(["i2_score", "i2_fragility_pressure"], ascending=[True, False])
        .head(25)[available_cols]
        .reset_index(drop=True)
    )

    highest_fragility = (
        latest.sort_values(["i2_fragility_pressure", "i2_score"], ascending=[False, True])
        .head(25)[available_cols]
        .reset_index(drop=True)
    )

    fastest_deterioration = (
        latest.sort_values(["i2_deterioration_pressure", "i2_score"], ascending=[False, True])
        .head(25)[available_cols]
        .reset_index(drop=True)
    )

    report = {
        "component": "i2_top_bottom_company_report_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "symbol_count": int(latest["symbol"].nunique()),
        "latest_year_min": int(latest["year"].min()) if latest["year"].notna().any() else None,
        "latest_year_max": int(latest["year"].max()) if latest["year"].notna().any() else None,
        "average_i2_score": round(float(latest["i2_score"].mean()), 4),
        "average_fragility_pressure": round(float(latest["i2_fragility_pressure"].mean()), 4),
        "top_i2_count": int(len(top_i2)),
        "bottom_i2_count": int(len(bottom_i2)),
        "highest_fragility_count": int(len(highest_fragility)),
        "fastest_deterioration_count": int(len(fastest_deterioration)),
        "status": "i2_top_bottom_company_report_complete",
    }

    top_i2.to_parquet(out / "i2_top_25_companies_v1.parquet", index=False)
    bottom_i2.to_parquet(out / "i2_bottom_25_companies_v1.parquet", index=False)
    highest_fragility.to_parquet(out / "i2_highest_fragility_25_v1.parquet", index=False)
    fastest_deterioration.to_parquet(out / "i2_fastest_deterioration_25_v1.parquet", index=False)

    top_i2.to_json(out / "i2_top_25_companies_v1.json", orient="records", indent=2)
    bottom_i2.to_json(out / "i2_bottom_25_companies_v1.json", orient="records", indent=2)
    highest_fragility.to_json(out / "i2_highest_fragility_25_v1.json", orient="records", indent=2)
    fastest_deterioration.to_json(out / "i2_fastest_deterioration_25_v1.json", orient="records", indent=2)

    with open(out / "i2_top_bottom_company_report_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    with open(out / "i2_top_bottom_company_report_v1.md", "w", encoding="utf-8") as f:
        f.write("# I2 Top / Bottom Company Report\n\n")
        f.write(f"Generated: {report['generated_at_utc']}\n\n")
        f.write("## Summary\n")
        for k, v in report.items():
            if k not in ["component", "status"]:
                f.write(f"- {k}: {v}\n")

        f.write("\n## Top 25 I2 Companies\n")
        f.write(top_i2.to_markdown(index=False))

        f.write("\n\n## Bottom 25 I2 Companies\n")
        f.write(bottom_i2.to_markdown(index=False))

        f.write("\n\n## Highest Fragility 25\n")
        f.write(highest_fragility.to_markdown(index=False))

        f.write("\n\n## Fastest Deterioration 25\n")
        f.write(fastest_deterioration.to_markdown(index=False))

    print("I2 Top/Bottom Company Report complete")
    print("Symbols:", report["symbol_count"])
    print("Average I2:", report["average_i2_score"])
    print("Average Fragility:", report["average_fragility_pressure"])
    print("OUTPUT:", out / "i2_top_bottom_company_report_v1.md")


if __name__ == "__main__":
    build_i2_top_bottom_company_report_v1()
