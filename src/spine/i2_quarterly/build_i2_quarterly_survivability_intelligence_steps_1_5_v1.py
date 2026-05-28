from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def classify_survivability(x):
    if x >= 0.80:
        return "systemic_survivability_regime"
    if x >= 0.65:
        return "fragile_survivability_regime"
    if x >= 0.50:
        return "elevated_survivability_regime"
    if x >= 0.35:
        return "watch_survivability_regime"
    return "stable_survivability_regime"


def build_i2_quarterly_survivability_intelligence_steps_1_5_v1():
    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_recursive_propagation_steps_6_10_v1.parquet"
    )

    out = root / "data" / "i2_quarterly"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing source: {src}")

    df = pd.read_parquet(src).copy()

    df["statement_date"] = pd.to_datetime(df["statement_date"], errors="coerce")

    df = (
        df
        .dropna(subset=["symbol", "statement_date"])
        .sort_values(["symbol", "statement_date"])
        .reset_index(drop=True)
    )

    # =====================================================
    # STEP 1 — MULTI-QUARTER SURVIVABILITY MEMORY ENGINE
    # =====================================================

    df["q_survivability_memory_4q"] = (
        df
        .groupby("symbol")["q_quarterly_systemicity_overlay"]
        .rolling(4, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
        .fillna(0)
        .round(4)
    )

    df["q_survivability_memory_max_4q"] = (
        df
        .groupby("symbol")["q_quarterly_systemicity_overlay"]
        .rolling(4, min_periods=1)
        .max()
        .reset_index(level=0, drop=True)
        .fillna(0)
        .round(4)
    )

    # =====================================================
    # STEP 2 — RECURSIVE SURVIVABILITY PERSISTENCE LAYER
    # =====================================================

    df["q_survivability_persistence_count_4q"] = (
        df
        .assign(_watch=(df["q_quarterly_systemicity_overlay"] >= 0.35).astype(int))
        .groupby("symbol")["_watch"]
        .rolling(4, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
        .astype(int)
    )

    df["q_survivability_persistence_score"] = (
        (
            df["q_survivability_memory_4q"] * 0.65
            + (df["q_survivability_persistence_count_4q"] / 4).clip(upper=1.0) * 0.35
        )
        .clip(upper=1.0)
        .round(4)
    )

    df["q_survivability_persistence_state"] = (
        df["q_survivability_persistence_score"]
        .apply(classify_survivability)
    )

    # =====================================================
    # STEP 3 — SECTOR FRAGILITY SYNCHRONIZATION ENGINE
    # =====================================================
    # If sector/industry metadata is not present yet, create a governed placeholder.

    sector_col = None
    for c in df.columns:
        if c.lower() in ["sector", "industry", "gics sector", "gics_sector"]:
            sector_col = c
            break

    if sector_col is None:
        df["sector_proxy"] = "unclassified_sector"
        sector_col = "sector_proxy"

    sector_sync = (
        df
        .groupby([sector_col, "statement_date"], as_index=False)
        .agg(
            q_sector_avg_systemicity=("q_quarterly_systemicity_overlay", "mean"),
            q_sector_avg_persistence=("q_survivability_persistence_score", "mean"),
            q_sector_symbol_count=("symbol", "nunique"),
            q_sector_watch_share=(
                "q_quarterly_systemicity_overlay",
                lambda x: float((x >= 0.35).mean())
            ),
        )
    )

    sector_sync["q_sector_fragility_synchronization"] = (
        (
            sector_sync["q_sector_avg_systemicity"] * 0.45
            + sector_sync["q_sector_avg_persistence"] * 0.35
            + sector_sync["q_sector_watch_share"] * 0.20
        )
        .clip(upper=1.0)
        .round(4)
    )

    df = df.merge(
        sector_sync[[
            sector_col,
            "statement_date",
            "q_sector_fragility_synchronization",
            "q_sector_watch_share",
        ]],
        on=[sector_col, "statement_date"],
        how="left"
    )

    # =====================================================
    # STEP 4 — CREDIT-CYCLE SURVIVABILITY TOPOLOGY
    # =====================================================
    # Uses available quarterly systemicity + persistence as credit-cycle proxy.
    # Later replacement target: live credit spreads / funding stress.

    df["q_credit_cycle_survivability_pressure"] = (
        (
            df["q_survivability_persistence_score"] * 0.45
            + df["q_corporate_fragility_conditioning"] * 0.35
            + df["q_sector_fragility_synchronization"].fillna(0) * 0.20
        )
        .clip(upper=1.0)
        .round(4)
    )

    df["q_credit_cycle_survivability_state"] = (
        df["q_credit_cycle_survivability_pressure"]
        .apply(classify_survivability)
    )

    # =====================================================
    # STEP 5 — MACRO ? CORPORATE RECURSIVE COUPLING LAYER
    # =====================================================

    fusion_path = root / "data" / "fusion" / "cross_engine_fusion_score_summary_v1.json"
    geoscen_path = root / "data" / "geoscen" / "geoscen_executive_synthesis_integration_v1.json"

    fusion_pressure = 0.0
    geoscen_pressure = 0.0

    if fusion_path.exists():
        fusion = json.loads(fusion_path.read_text(encoding="utf-8"))
        fusion_pressure = float(fusion.get("fusion_pressure", 0) or 0)

    if geoscen_path.exists():
        geoscen = json.loads(geoscen_path.read_text(encoding="utf-8"))
        geoscen_pressure = float(
            geoscen
            .get("geoscen_executive_state", {})
            .get("geoscen_pressure", 0)
            or 0
        )

    df["q_macro_corporate_recursive_coupling"] = (
        (
            df["q_credit_cycle_survivability_pressure"] * 0.50
            + fusion_pressure * 0.30
            + geoscen_pressure * 0.20
        )
        .clip(upper=1.0)
        .round(4)
    )

    df["q_macro_corporate_coupling_state"] = (
        df["q_macro_corporate_recursive_coupling"]
        .apply(classify_survivability)
    )

    # =====================================================
    # SUMMARY
    # =====================================================

    summary = {
        "component": "i2_quarterly_survivability_intelligence_steps_1_5_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rows": int(len(df)),
        "symbols": int(df["symbol"].nunique()),
        "dates": int(df["statement_date"].nunique()),
        "sector_column_used": sector_col,
        "average_survivability_memory_4q": round(float(df["q_survivability_memory_4q"].mean()), 4),
        "average_survivability_persistence": round(float(df["q_survivability_persistence_score"].mean()), 4),
        "average_sector_fragility_sync": round(float(df["q_sector_fragility_synchronization"].mean()), 4),
        "average_credit_cycle_survivability": round(float(df["q_credit_cycle_survivability_pressure"].mean()), 4),
        "average_macro_corporate_coupling": round(float(df["q_macro_corporate_recursive_coupling"].mean()), 4),
        "fusion_pressure_used": fusion_pressure,
        "geoscen_pressure_used": geoscen_pressure,
        "status": "quarterly_survivability_intelligence_steps_1_5_complete",
    }

    # =====================================================
    # SAVE
    # =====================================================

    df.to_parquet(
        out / "i2_quarterly_survivability_intelligence_steps_1_5_v1.parquet",
        index=False
    )

    df.head(500).to_json(
        out / "i2_quarterly_survivability_intelligence_steps_1_5_sample_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    sector_sync.to_parquet(
        out / "i2_quarterly_sector_fragility_synchronization_v1.parquet",
        index=False
    )

    sector_sync.to_json(
        out / "i2_quarterly_sector_fragility_synchronization_v1.json",
        orient="records",
        indent=2,
        date_format="iso"
    )

    with open(
        out / "i2_quarterly_survivability_intelligence_steps_1_5_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2, default=str)

    md = []
    md.append("# I2 Quarterly Survivability Intelligence Steps 1-5")
    md.append("")
    md.append(f"Generated: {summary['generated_at_utc']}")
    md.append("")
    md.append("## Summary")
    md.append("")
    for k, v in summary.items():
        if k not in ["component", "status"]:
            md.append(f"- {k}: {v}")
    md.append("")
    md.append("## Bottom Line")
    md.append("")
    md.append(
        "Quarterly I2 now contains multi-quarter survivability memory, persistence scoring, sector fragility synchronization, credit-cycle survivability pressure, and macro-corporate recursive coupling."
    )

    md_path = out / "i2_quarterly_survivability_intelligence_steps_1_5_v1.md"
    md_path.write_text("\n".join(md), encoding="utf-8")

    print("I2 Quarterly Survivability Intelligence Steps 1-5 complete")
    print("Rows:", summary["rows"])
    print("Symbols:", summary["symbols"])
    print("Dates:", summary["dates"])
    print("Avg memory 4Q:", summary["average_survivability_memory_4q"])
    print("Avg persistence:", summary["average_survivability_persistence"])
    print("Avg sector sync:", summary["average_sector_fragility_sync"])
    print("Avg credit cycle:", summary["average_credit_cycle_survivability"])
    print("Avg macro-corporate coupling:", summary["average_macro_corporate_coupling"])
    print("OUTPUT:", md_path)


if __name__ == "__main__":
    build_i2_quarterly_survivability_intelligence_steps_1_5_v1()
