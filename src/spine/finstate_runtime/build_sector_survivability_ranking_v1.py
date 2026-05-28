from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


def build_sector_survivability_ranking_v1():

    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_survivability_intelligence_steps_1_5_v1.parquet"
    )

    if not src.exists():
        raise FileNotFoundError(src)

    df = pd.read_parquet(src)

    sector_col = None

    for c in df.columns:
        if c.lower() in ["sector", "industry", "gics sector", "gics_sector", "sector_proxy"]:
            sector_col = c
            break

    if sector_col is None:
        df["sector_proxy"] = "unclassified_sector"
        sector_col = "sector_proxy"

    pressure_col = (
        "q_credit_cycle_survivability_pressure"
        if "q_credit_cycle_survivability_pressure" in df.columns
        else "q_macro_corporate_recursive_coupling"
    )

    ranking = (
        df.groupby(sector_col, as_index=False)
        .agg(
            avg_survivability_pressure=(pressure_col, "mean"),
            symbol_count=("symbol", "nunique")
        )
        .sort_values("avg_survivability_pressure", ascending=False)
    )

    ranking["rank"] = range(1, len(ranking) + 1)

    payload = {
        "component": "sector_survivability_ranking_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "sector_column": sector_col,
        "pressure_column": pressure_col,
        "sector_count": int(len(ranking)),
        "top_sectors": ranking.head(20).to_dict(orient="records"),
        "status": "sector_survivability_ranking_ready"
    }

    out = (
        root
        / "_offline_site"
        / "finstate_payloads"
        / "sector_survivability_ranking_v1.json"
    )

    out.write_text(
        json.dumps(payload, indent=2, default=str),
        encoding="utf-8"
    )

    print("Sector Survivability Ranking complete")
    print("Sectors:", payload["sector_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_sector_survivability_ranking_v1()
