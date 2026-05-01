import os
import json
import pandas as pd

from spine.jobs.geoscen.pmi.pmi_commentary_constants import (
    TAGS_OUTPUT,
    NUMERIC_PANEL_INPUT,
    COMMENTARY_NUMERIC_OVERLAY_OUTPUT,
)


def clean_key(value) -> str:
    return " ".join(str(value or "").lower().strip().split())


def load_numeric_panel() -> pd.DataFrame:
    override = os.getenv("PMI_NUMERIC_PANEL_FILE")
    path = override if override else NUMERIC_PANEL_INPUT

    path = str(path)

    if path.endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        rows = payload.get("rows", payload) if isinstance(payload, dict) else payload
        df = pd.DataFrame(rows)

    elif path.endswith(".parquet"):
        df = pd.read_parquet(path)

    else:
        raise ValueError(f"[FAIL] Unsupported numeric panel file: {path}")

    required = ["date", "industry", "pmi_Idx", "no_Idx", "Sig", "Wt"]
    missing = set(required) - set(df.columns)

    if missing:
        raise ValueError(f"[FAIL] Numeric panel missing columns: {missing}. Found: {list(df.columns)}")

    return df.copy()


def build_pmi_commentary_numeric_overlay_v1() -> pd.DataFrame:
    df_tags = pd.read_parquet(TAGS_OUTPUT).copy()
    df_num = load_numeric_panel()

    df_tags["date"] = pd.to_datetime(df_tags["date"]).dt.strftime("%Y-%m-%d")
    df_num["date"] = pd.to_datetime(df_num["date"]).dt.strftime("%Y-%m-%d")

    df_tags["join_industry"] = df_tags["industry"].map(clean_key)
    df_num["join_industry"] = df_num["industry"].map(clean_key)

    numeric_cols = [
        "date",
        "join_industry",
        "pmi_type",
        "etf",
        "pmi_Idx",
        "no_Idx",
        "pmi_3M_Δ",
        "pmi_6M_Δ",
        "no_3M_Δ",
        "no_6M_Δ",
        "Sig",
        "Wt",
    ]

    keep_numeric_cols = [c for c in numeric_cols if c in df_num.columns]

    df = df_tags.merge(
        df_num[keep_numeric_cols],
        on=["date", "join_industry"],
        how="left",
        suffixes=("", "_numeric"),
    )

    df["numeric_relevance_flag"] = (
        (df["tag_demand"] == 1)
        | (df["tag_supply_chain"] == 1)
        | (df["tag_inflation"] == 1)
        | (df["tag_credit"] == 1)
    ).astype(int)

    conditional_numeric_cols = [
        "pmi_Idx",
        "no_Idx",
        "pmi_3M_Δ",
        "pmi_6M_Δ",
        "no_3M_Δ",
        "no_6M_Δ",
        "Sig",
        "Wt",
    ]

    for col in conditional_numeric_cols:
        if col in df.columns:
            df.loc[df["numeric_relevance_flag"] == 0, col] = pd.NA

    df = df.drop(columns=["join_industry"], errors="ignore")

    COMMENTARY_NUMERIC_OVERLAY_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(COMMENTARY_NUMERIC_OVERLAY_OUTPUT, index=False)

    print(f"[OK] Wrote PMI commentary numeric overlay rows: {len(df)}")
    print(f"[PATH] {COMMENTARY_NUMERIC_OVERLAY_OUTPUT}")

    return df


if __name__ == "__main__":
    build_pmi_commentary_numeric_overlay_v1()

    