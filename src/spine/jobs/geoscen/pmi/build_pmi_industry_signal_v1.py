import pandas as pd

from spine.jobs.geoscen.pmi.pmi_commentary_constants import (
    TAGS_OUTPUT,
    INDUSTRY_SIGNAL_OUTPUT,
)


def direction_score(value: str) -> int:
    if value == "+":
        return 1
    if value == "-":
        return -1
    return 0


def build_pmi_industry_signal_v1() -> pd.DataFrame:
    df = pd.read_parquet(TAGS_OUTPUT).copy()

    df["direction_score"] = df["direction"].apply(direction_score)
    df["pmi_gap_50"] = df["pmi"] - 50
    df["new_orders_gap_50"] = df["new_orders"] - 50

    group_cols = ["date", "source", "release_type", "sector", "industry"]

    out = (
        df.groupby(group_cols, dropna=False)
        .agg(
            pmi=("pmi", "mean"),
            new_orders=("new_orders", "mean"),
            commentary_count=("commentary_text", "count"),
            avg_direction_score=("direction_score", "mean"),
            avg_confidence=("confidence", "mean"),
            tag_labor=("tag_labor", "sum"),
            tag_inflation=("tag_inflation", "sum"),
            tag_credit=("tag_credit", "sum"),
            tag_demand=("tag_demand", "sum"),
            tag_supply_chain=("tag_supply_chain", "sum"),
            tag_geopolitics=("tag_geopolitics", "sum"),
        )
        .reset_index()
    )

    out["industry_zt_input"] = (
        (out["pmi"] - 50) * 0.40
        + (out["new_orders"] - 50) * 0.40
        + out["avg_direction_score"] * 10 * 0.20
    )

    INDUSTRY_SIGNAL_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(INDUSTRY_SIGNAL_OUTPUT, index=False)

    print(f"[OK] Wrote PMI industry signal rows ({len(out)})")
    print(f"[PATH] {INDUSTRY_SIGNAL_OUTPUT}")

    return out


if __name__ == "__main__":
    build_pmi_industry_signal_v1()

