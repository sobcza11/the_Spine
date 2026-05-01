import os
from datetime import datetime, timezone

import pandas as pd

from spine.jobs.geoscen.pmi.pmi_commentary_constants import (
    SOURCE_NAME,
    LANGUAGE,
    QUAL_SHEET_NAME,
    RAW_OUTPUT,
    CANONICAL_OUTPUT,
)


def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lookup = {str(c).strip().lower(): c for c in df.columns}

    for candidate in candidates:
        key = candidate.strip().lower()
        if key in lookup:
            return lookup[key]

    return None


def clean_text(value) -> str:
    return " ".join(str(value or "").split())


def ingest_pmi_qual_t1() -> pd.DataFrame:
    source_path = os.getenv("PMI_QUAL_FILE")

    if not source_path:
        raise ValueError("[FAIL] Missing env var PMI_QUAL_FILE")

    df_raw = pd.read_excel(source_path, sheet_name=QUAL_SHEET_NAME)
    df_raw.columns = [str(c).strip() for c in df_raw.columns]

    RAW_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df_raw = df_raw.loc[:, ~df_raw.columns.str.contains("^Unnamed")]

    for col in df_raw.columns:
        try:
            df_raw[col] = df_raw[col].astype(str)
        except Exception:
            df_raw[col] = df_raw[col].apply(lambda x: str(x) if x is not None else "")

    RAW_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df_raw.to_parquet(RAW_OUTPUT, index=False)

    date_col = find_col(df_raw, ["date", "release_date", "month"])
    release_col = find_col(df_raw, ["group", "release_type", "survey", "report_type"])
    sector_col = find_col(df_raw, ["sector", "major_sector"])
    industry_col = find_col(df_raw, ["position", "industry", "subindustry", "category"])
    pmi_col = find_col(df_raw, ["pmi", "pmi_index", "PMI"])
    new_orders_col = find_col(df_raw, ["new_orders", "new orders", "new_orders_index"])
    commentary_col = find_col(
    df_raw,
        ["comments", "commentary", "commentary_text", "qual", "qual_text", "text"]
    )

    missing = {
        "date": date_col,
        "commentary_text": commentary_col,
    }

    missing = [k for k, v in missing.items() if v is None]
    if missing:
        raise ValueError(f"[FAIL] Missing required QUAL columns: {missing}. Found: {list(df_raw.columns)}")

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(df_raw[date_col], errors="coerce"),
            "source": SOURCE_NAME,
            "release_type": df_raw[release_col].map(clean_text) if release_col else "PMI",
            "sector": df_raw[sector_col].map(clean_text) if sector_col else "",
            "industry": df_raw[industry_col].map(clean_text) if industry_col else "",
            "pmi": None,
            "new_orders": None,
            "commentary_text": df_raw[commentary_col].map(clean_text),
            "language": LANGUAGE,
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    )

    df = df.dropna(subset=["date"])
    df = df[df["commentary_text"].str.len() > 0]
    df = df.reset_index(drop=True)

    CANONICAL_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(CANONICAL_OUTPUT, index=False)

    print(f"[OK] Wrote PMI raw rows: {len(df_raw)}")
    print(f"[OK] Wrote PMI canonical rows: {len(df)}")
    print(f"[PATH] {CANONICAL_OUTPUT}")

    return df


if __name__ == "__main__":
    ingest_pmi_qual_t1()

    