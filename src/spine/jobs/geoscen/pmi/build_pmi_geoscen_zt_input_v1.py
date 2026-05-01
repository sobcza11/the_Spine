import pandas as pd

from spine.jobs.geoscen.pmi.pmi_commentary_constants import (
    COMMENTARY_NUMERIC_OVERLAY_OUTPUT,
    GEOSCEN_ZT_INPUT_OUTPUT,
)


def direction_score(value: str) -> int:
    value = str(value or "").strip()

    if value == "+":
        return 1

    if value == "-":
        return -1

    return 0


def build_pmi_geoscen_zt_input_v1() -> pd.DataFrame:
    df = pd.read_parquet(COMMENTARY_NUMERIC_OVERLAY_OUTPUT).copy()

    df["direction_score"] = df["direction"].apply(direction_score)

    df["pmi_gap_50"] = pd.to_numeric(df.get("pmi_Idx"), errors="coerce") - 50
    df["new_orders_gap_50"] = pd.to_numeric(df.get("no_Idx"), errors="coerce") - 50
    df["sig_score"] = pd.to_numeric(df.get("Sig"), errors="coerce")

    df["text_signal_score"] = df["direction_score"] * df["confidence"].fillna(0)

    df["numeric_signal_score"] = (
        df["pmi_gap_50"].fillna(0) * 0.35
        + df["new_orders_gap_50"].fillna(0) * 0.45
        + df["sig_score"].fillna(0) * 0.20
    )

    df["pmi_geoscen_zt_input"] = (
        df["numeric_signal_score"] * 0.70
        + df["text_signal_score"] * 10 * 0.30
    )

    out = (
        df.groupby(["date", "sector", "industry"], dropna=False)
        .agg(
            commentary_count=("commentary_text", "count"),
            avg_confidence=("confidence", "mean"),
            avg_text_signal_score=("text_signal_score", "mean"),
            avg_numeric_signal_score=("numeric_signal_score", "mean"),
            pmi_geoscen_zt_input=("pmi_geoscen_zt_input", "mean"),
            tag_labor=("tag_labor", "sum"),
            tag_inflation=("tag_inflation", "sum"),
            tag_credit=("tag_credit", "sum"),
            tag_demand=("tag_demand", "sum"),
            tag_supply_chain=("tag_supply_chain", "sum"),
            tag_geopolitics=("tag_geopolitics", "sum"),
        )
        .reset_index()
    )

    GEOSCEN_ZT_INPUT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(GEOSCEN_ZT_INPUT_OUTPUT, index=False)

    print(f"[OK] Wrote PMI GeoScen Zt input rows: {len(out)}")
    print(f"[PATH] {GEOSCEN_ZT_INPUT_OUTPUT}")

    return out


if __name__ == "__main__":
    build_pmi_geoscen_zt_input_v1()

