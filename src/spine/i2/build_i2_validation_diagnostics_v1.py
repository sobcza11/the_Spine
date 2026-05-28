from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


EXPECTED_COLUMNS = [
    "symbol",
    "year",
    "i2_score",
    "i2_state",
    "i2_fragility_pressure",
    "i2_deterioration_pressure",
    "gross_margin",
    "operating_margin",
    "roa",
    "roe",
    "debt_to_equity",
    "current_ratio",
    "free_cash_flow",
]


def latest_by_symbol(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    return (
        df.sort_values(["symbol", "year"])
        .groupby("symbol", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )


def pct_missing(series):
    return round(float(series.isna().mean()), 4)


def build_i2_validation_diagnostics_v1():
    root = Path.cwd()
    out = root / "data" / "i2"
    out.mkdir(parents=True, exist_ok=True)

    src = root / "data" / "i2" / "i2_corporate_fragility_propagation_v1.parquet"

    if not src.exists():
        raise FileNotFoundError(f"Missing I2 propagation output: {src}")

    df = pd.read_parquet(src).copy()

    latest = latest_by_symbol(df)

    # =====================================================
    # REQUIRED COLUMN VALIDATION
    # =====================================================

    column_validation = []

    for col in EXPECTED_COLUMNS:
        exists = col in latest.columns

        column_validation.append({
            "column": col,
            "exists": exists,
            "missing_ratio": pct_missing(latest[col]) if exists else 1.0,
        })

    column_df = pd.DataFrame(column_validation)

    # =====================================================
    # SCORE DISTRIBUTION
    # =====================================================

    latest["i2_score"] = pd.to_numeric(latest["i2_score"], errors="coerce")
    latest["i2_fragility_pressure"] = pd.to_numeric(
        latest["i2_fragility_pressure"],
        errors="coerce"
    )

    score_distribution = {
        "min_i2_score": round(float(latest["i2_score"].min()), 4),
        "max_i2_score": round(float(latest["i2_score"].max()), 4),
        "mean_i2_score": round(float(latest["i2_score"].mean()), 4),
        "median_i2_score": round(float(latest["i2_score"].median()), 4),
        "std_i2_score": round(float(latest["i2_score"].std()), 4),
    }

    # =====================================================
    # TOP / BOTTOM VALIDATION
    # =====================================================

    top10 = (
        latest.sort_values("i2_score", ascending=False)
        .head(10)[["symbol", "i2_score", "i2_state", "i2_fragility_pressure"]]
    )

    bottom10 = (
        latest.sort_values("i2_score", ascending=True)
        .head(10)[["symbol", "i2_score", "i2_state", "i2_fragility_pressure"]]
    )

    fragility10 = (
        latest.sort_values("i2_fragility_pressure", ascending=False)
        .head(10)[["symbol", "i2_score", "i2_fragility_pressure", "i2_escalation_state"]]
    )

    # =====================================================
    # OUTLIER ANALYSIS
    # =====================================================

    latest["i2_zscore"] = (
        (latest["i2_score"] - latest["i2_score"].mean()) /
        latest["i2_score"].std()
    )

    outliers = latest[
        latest["i2_zscore"].abs() >= 2.5
    ][[
        "symbol",
        "i2_score",
        "i2_zscore",
        "i2_state",
    ]].sort_values("i2_zscore", ascending=False)

    # =====================================================
    # COVERAGE ANALYSIS
    # =====================================================

    year_counts = (
        latest.groupby("year")
        .size()
        .reset_index(name="company_count")
        .sort_values("year")
    )

    # =====================================================
    # GOVERNANCE READ
    # =====================================================

    governance_read = []

    if score_distribution["std_i2_score"] < 0.05:
        governance_read.append(
            "I2 score distribution may be overly compressed."
        )

    if score_distribution["max_i2_score"] <= 0.70:
        governance_read.append(
            "No companies currently reaching strong anti-fragile threshold."
        )

    if score_distribution["min_i2_score"] >= 0.30:
        governance_read.append(
            "Lower-tail fragility may not be sufficiently differentiated."
        )

    if len(outliers) == 0:
        governance_read.append(
            "Few statistical outliers detected in current scoring distribution."
        )

    if not governance_read:
        governance_read.append(
            "I2 scoring distribution appears structurally reasonable."
        )

    # =====================================================
    # SUMMARY
    # =====================================================

    summary = {
        "component": "i2_validation_diagnostics_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),

        "symbol_count": int(latest["symbol"].nunique()),

        "score_distribution": score_distribution,

        "missing_column_count": int((~column_df["exists"]).sum()),

        "high_missing_columns": column_df[
            column_df["missing_ratio"] >= 0.50
        ]["column"].tolist(),

        "outlier_count": int(len(outliers)),

        "governance_read": governance_read,

        "validation_status": (
            "reasonable_structural_distribution"
            if score_distribution["std_i2_score"] >= 0.05
            else "compressed_distribution_watch"
        ),

        "status": "i2_validation_diagnostics_complete",
    }

    # =====================================================
    # OUTPUTS
    # =====================================================

    column_df.to_parquet(
        out / "i2_validation_columns_v1.parquet",
        index=False
    )

    top10.to_parquet(
        out / "i2_validation_top10_v1.parquet",
        index=False
    )

    bottom10.to_parquet(
        out / "i2_validation_bottom10_v1.parquet",
        index=False
    )

    fragility10.to_parquet(
        out / "i2_validation_fragility10_v1.parquet",
        index=False
    )

    outliers.to_parquet(
        out / "i2_validation_outliers_v1.parquet",
        index=False
    )

    year_counts.to_parquet(
        out / "i2_validation_year_coverage_v1.parquet",
        index=False
    )

    with open(
        out / "i2_validation_diagnostics_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    md = []

    md.append("# I2 Validation Diagnostics")
    md.append("")
    md.append(f"Generated: {summary['generated_at_utc']}")
    md.append("")
    md.append("## Summary")
    md.append("")

    for k, v in summary.items():
        if k not in ["component", "status"]:
            md.append(f"- {k}: {v}")

    md.append("")
    md.append("## Score Distribution")
    md.append("")

    for k, v in score_distribution.items():
        md.append(f"- {k}: {v}")

    md.append("")
    md.append("## Governance Read")
    md.append("")

    for item in governance_read:
        md.append(f"- {item}")

    md.append("")
    md.append("## Top 10 I2")
    md.append("")
    md.append(top10.to_markdown(index=False))

    md.append("")
    md.append("## Bottom 10 I2")
    md.append("")
    md.append(bottom10.to_markdown(index=False))

    md.append("")
    md.append("## Highest Fragility")
    md.append("")
    md.append(fragility10.to_markdown(index=False))

    md.append("")
    md.append("## Coverage By Year")
    md.append("")
    md.append(year_counts.to_markdown(index=False))

    md_path = out / "i2_validation_diagnostics_v1.md"

    md_path.write_text(
        "\n".join(md),
        encoding="utf-8"
    )

    print("I2 Validation Diagnostics complete")
    print("Symbols:", summary["symbol_count"])
    print("Validation:", summary["validation_status"])
    print("Outliers:", summary["outlier_count"])
    print("OUTPUT:", md_path)


if __name__ == "__main__":
    build_i2_validation_diagnostics_v1()
