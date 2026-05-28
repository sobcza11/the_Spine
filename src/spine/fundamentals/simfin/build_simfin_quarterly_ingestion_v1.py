from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


TARGET_HINTS = {
    "income_quarterly": [
        "income",
        "quarter",
        "q",
    ],
    "balance_quarterly": [
        "balance",
        "quarter",
        "q",
    ],
    "cashflow_quarterly": [
        "cash",
        "quarter",
        "q",
    ],
}


def score_file(path: Path, hints):
    text = str(path).lower()
    score = 0

    for h in hints:
        if h in text:
            score += 1

    return score


def load_candidate(path: Path):
    suffix = path.suffix.lower()

    if suffix == ".parquet":
        return pd.read_parquet(path)

    if suffix == ".csv":
        return pd.read_csv(path)

    if suffix == ".zip":
        return pd.read_csv(path, compression="zip")

    return None


def has_quarterly_period(df: pd.DataFrame):
    cols = {c.lower(): c for c in df.columns}

    fiscal_period_col = (
        cols.get("fiscal period")
        or cols.get("fiscal_period")
        or cols.get("period")
    )

    if fiscal_period_col is None:
        return False

    values = (
        df[fiscal_period_col]
        .astype(str)
        .str.upper()
        .dropna()
        .unique()
        .tolist()
    )

    return any(v in ["Q1", "Q2", "Q3", "Q4"] for v in values)


def normalize_quarterly(df: pd.DataFrame):
    df = df.copy()

    cols = {c.lower(): c for c in df.columns}

    ticker_col = (
        cols.get("ticker")
        or cols.get("symbol")
    )

    date_col = (
        cols.get("report date")
        or cols.get("report_date")
        or cols.get("date")
        or cols.get("fiscal date")
        or cols.get("fiscal_date")
        or cols.get("publish date")
    )

    period_col = (
        cols.get("fiscal period")
        or cols.get("fiscal_period")
        or cols.get("period")
    )

    if ticker_col is None:
        raise KeyError(f"No ticker/symbol column found. Columns: {list(df.columns)}")

    if date_col is None:
        raise KeyError(f"No report/date column found. Columns: {list(df.columns)}")

    df["symbol"] = df[ticker_col].astype(str)
    df["statement_date"] = pd.to_datetime(df[date_col], errors="coerce")

    if period_col:
        df["fiscal_period"] = df[period_col].astype(str).str.upper()
    else:
        df["fiscal_period"] = ""

    df = df[df["fiscal_period"].isin(["Q1", "Q2", "Q3", "Q4"])].copy()

    df = df.dropna(subset=["symbol", "statement_date"])

    return df


def build_simfin_quarterly_ingestion_v1():
    root = Path.cwd()

    simfin_root = root / "data" / "fundamentals" / "simfin"
    out = simfin_root

    out.mkdir(parents=True, exist_ok=True)

    candidates = []

    for pattern in ["**/*.parquet", "**/*.csv", "**/*.zip"]:
        candidates.extend(simfin_root.glob(pattern))

    discovery_rows = []
    selected = {}

    for target, hints in TARGET_HINTS.items():

        scored = []

        for fp in candidates:
            s = score_file(fp, hints)

            if s <= 0:
                continue

            scored.append((s, fp))

        scored = sorted(scored, key=lambda x: x[0], reverse=True)

        selected_file = None
        selected_df = None
        selected_score = 0

        for s, fp in scored:
            try:
                df = load_candidate(fp)
            except Exception:
                continue

            if df is None or df.empty:
                continue

            if not has_quarterly_period(df):
                continue

            try:
                qdf = normalize_quarterly(df)
            except Exception:
                continue

            if qdf.empty:
                continue

            selected_file = fp
            selected_df = qdf
            selected_score = s
            break

        if selected_df is not None:
            selected[target] = selected_df

            output_path = out / f"{target}.parquet"
            selected_df.to_parquet(output_path, index=False)

            selected_df.head(500).to_json(
                out / f"{target}_sample.json",
                orient="records",
                indent=2,
                date_format="iso"
            )

            discovery_rows.append({
                "target": target,
                "status": "found_and_exported",
                "source_file": str(selected_file.relative_to(root)),
                "output_file": str(output_path.relative_to(root)),
                "score": selected_score,
                "rows": int(len(selected_df)),
                "symbols": int(selected_df["symbol"].nunique()),
                "min_date": str(selected_df["statement_date"].min()),
                "max_date": str(selected_df["statement_date"].max()),
            })
        else:
            discovery_rows.append({
                "target": target,
                "status": "not_found",
                "source_file": None,
                "output_file": None,
                "score": 0,
                "rows": 0,
                "symbols": 0,
                "min_date": None,
                "max_date": None,
            })

    discovery = pd.DataFrame(discovery_rows)

    discovery.to_parquet(
        out / "simfin_quarterly_ingestion_discovery_v1.parquet",
        index=False
    )

    discovery.to_json(
        out / "simfin_quarterly_ingestion_discovery_v1.json",
        orient="records",
        indent=2
    )

    summary = {
        "component": "simfin_quarterly_ingestion_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "target_count": int(len(discovery)),
        "found_count": int((discovery["status"] == "found_and_exported").sum()),
        "missing_count": int((discovery["status"] == "not_found").sum()),
        "status": (
            "simfin_quarterly_ingestion_complete"
            if int((discovery["status"] == "not_found").sum()) == 0
            else "simfin_quarterly_ingestion_partial"
        ),
    }

    with open(
        out / "simfin_quarterly_ingestion_summary_v1.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(summary, f, indent=2)

    print("SimFin Quarterly Ingestion complete")
    print("Found:", summary["found_count"])
    print("Missing:", summary["missing_count"])
    print("Status:", summary["status"])


if __name__ == "__main__":
    build_simfin_quarterly_ingestion_v1()
