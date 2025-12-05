# scripts/build_fed_block_for_spine.py

from pathlib import Path
import json

import pandas as pd


DATA_DIR = Path("data")


def _agg_by_date(df: pd.DataFrame, date_col: str, id_cols=None) -> pd.DataFrame:
    """
    Group a dataframe by date_col and aggregate:
    - numeric columns -> mean
    - id columns      -> first (or joined if list)
    """
    if df.empty:
        return df

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])

    df = df.sort_values(date_col)

    if id_cols is None:
        id_cols = []

    numeric_cols = df.select_dtypes("number").columns.tolist()

    agg_spec = {}
    for c in numeric_cols:
        agg_spec[c] = "mean"
    for c in id_cols:
        if c in df.columns:
            agg_spec[c] = "first"

    grouped = df.groupby(date_col, as_index=False).agg(agg_spec)
    return grouped


def load_minutes_df() -> pd.DataFrame:
    path = DATA_DIR / "fomc_minutes_features_for_spine.json"
    if not path.exists():
        print(f"! Minutes features file not found: {path}")
        return pd.DataFrame()

    rows = json.loads(path.read_text(encoding="utf-8"))
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = _agg_by_date(df, "meeting_date", id_cols=["meeting_id"])
    df = df.rename(columns={"meeting_date": "date"})
    return df


def load_statements_df() -> pd.DataFrame:
    path = DATA_DIR / "fomc_statements_features_for_spine.json"
    if not path.exists():
        print(f"! Statements features file not found: {path}")
        return pd.DataFrame()

    rows = json.loads(path.read_text(encoding="utf-8"))
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = _agg_by_date(df, "statement_date", id_cols=["statement_id"])
    df = df.rename(columns={"statement_date": "date"})
    return df


def load_beige_df() -> pd.DataFrame:
    path = DATA_DIR / "beige_book_features_for_spine.json"
    if not path.exists():
        print(f"! Beige features file not found: {path}")
        return pd.DataFrame()

    rows = json.loads(path.read_text(encoding="utf-8"))
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = _agg_by_date(df, "beige_date", id_cols=["beige_id"])
    df = df.rename(columns={"beige_date": "date"})
    return df


def main() -> None:
    minutes_df = load_minutes_df()
    stmts_df = load_statements_df()
    beige_df = load_beige_df()

    print("Minutes shape  :", minutes_df.shape)
    print("Statements shape:", stmts_df.shape)
    print("Beige shape    :", beige_df.shape)

    fed_block = None

    if not minutes_df.empty:
        fed_block = minutes_df.set_index("date")

    if not stmts_df.empty:
        stmts_block = stmts_df.set_index("date")
        fed_block = stmts_block if fed_block is None else fed_block.join(
            stmts_block, how="outer", rsuffix="_stmt"
        )

    if not beige_df.empty:
        beige_block = beige_df.set_index("date")
        fed_block = beige_block if fed_block is None else fed_block.join(
            beige_block, how="outer", rsuffix="_beige"
        )

    if fed_block is None or fed_block.empty:
        print("! No Fed features to merge; exiting.")
        return

    fed_block = fed_block.sort_index()

    # Reset index and clean up NaT
    fed_block_reset = fed_block.reset_index().rename(columns={"index": "date"})
    fed_block_reset = fed_block_reset.dropna(subset=["date"])

    out_csv = DATA_DIR / "fed_block_for_spine.csv"
    out_json = DATA_DIR / "fed_block_for_spine.json"

    fed_block_reset.to_csv(out_csv, index=False)
    fed_block_reset.to_json(
        out_json, orient="records", indent=2, date_format="iso"
    )

    print("\nSaved Fed block to:")
    print(f"  {out_csv}")
    print(f"  {out_json}")
    print("\nColumns:")
    print(list(fed_block_reset.columns))


if __name__ == "__main__":
    main()

