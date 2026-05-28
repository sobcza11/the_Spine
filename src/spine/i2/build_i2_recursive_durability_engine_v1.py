from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


def score_rank(s, higher_is_better=True):
    x = pd.to_numeric(s, errors="coerce")
    r = x.rank(pct=True)
    if not higher_is_better:
        r = 1 - r
    return r.fillna(0.5)


def build_i2_recursive_durability_engine_v1():
    root = Path.cwd()
    src = root / "data" / "i2" / "i2_company_fundamental_panel_v1.parquet"
    out = root / "data" / "i2"
    out.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Missing company panel: {src}")

    df = pd.read_parquet(src).copy()
    df = df.sort_values(["symbol", "year"]).reset_index(drop=True)

    score_parts = []

    if "gross_margin" in df.columns:
        df["score_profitability"] = score_rank(df["gross_margin"], True)
        score_parts.append("score_profitability")

    if "roa" in df.columns:
        df["score_asset_quality"] = score_rank(df["roa"], True)
        score_parts.append("score_asset_quality")

    if "current_ratio" in df.columns:
        df["score_liquidity"] = score_rank(df["current_ratio"], True)
        score_parts.append("score_liquidity")

    if "debt_to_equity" in df.columns:
        df["score_leverage_control"] = score_rank(df["debt_to_equity"], False)
        score_parts.append("score_leverage_control")

    if "free_cash_flow" in df.columns:
        df["score_cash_flow"] = score_rank(df["free_cash_flow"], True)
        score_parts.append("score_cash_flow")

    if "revenue" in df.columns:
        df["revenue_growth"] = df.groupby("symbol")["revenue"].pct_change()
        df["score_growth_durability"] = score_rank(df["revenue_growth"], True)
        score_parts.append("score_growth_durability")

    if not score_parts:
        raise ValueError("No usable financial metric columns found for I2 durability scoring")

    df["i2_recursive_durability_score"] = df[score_parts].mean(axis=1).round(4)

    df["i2_durability_state"] = np.select(
        [
            df["i2_recursive_durability_score"] >= 0.80,
            df["i2_recursive_durability_score"] >= 0.65,
            df["i2_recursive_durability_score"] >= 0.50,
            df["i2_recursive_durability_score"] >= 0.35,
        ],
        [
            "anti_fragile_durability",
            "strong_durability",
            "watch_durability",
            "fragile_durability",
        ],
        default="structural_deterioration",
    )

    summary = {
        "component": "i2_recursive_durability_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": int(len(df)),
        "symbol_count": int(df["symbol"].nunique()),
        "score_parts": score_parts,
        "average_i2_score": round(float(df["i2_recursive_durability_score"].mean()), 4),
        "status": "i2_recursive_durability_engine_complete",
    }

    df.to_parquet(out / "i2_recursive_durability_engine_v1.parquet", index=False)
    df.to_json(out / "i2_recursive_durability_engine_v1.json", orient="records", indent=2)

    with open(out / "i2_recursive_durability_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("I2 Recursive Durability Engine complete")
    print("Average I2:", summary["average_i2_score"])


if __name__ == "__main__":
    build_i2_recursive_durability_engine_v1()

