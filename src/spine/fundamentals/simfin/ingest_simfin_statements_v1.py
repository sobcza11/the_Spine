from pathlib import Path
from datetime import datetime, UTC
import os
import json
import pandas as pd
import simfin as sf


def _flatten_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if isinstance(out.index, pd.MultiIndex):
        out = out.reset_index()
    elif out.index.name is not None:
        out = out.reset_index()

    return out


def ingest_simfin_statements_v1():
    repo_root = Path.cwd()

    api_key = os.getenv("SIMFIN_API_KEY")
    if not api_key:
        raise RuntimeError("Missing SIMFIN_API_KEY environment variable.")

    raw_dir = repo_root / "data" / "fundamentals" / "simfin" / "raw"
    cache_dir = repo_root / "data" / "fundamentals" / "simfin" / "_simfin_cache"
    processed_dir = repo_root / "data" / "fundamentals" / "simfin" / "processed"

    raw_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    sf.set_api_key(api_key)
    sf.set_data_dir(str(cache_dir))

    income = _flatten_index(
        sf.load_income(variant="annual", market="us")
    )

    balance = _flatten_index(
        sf.load_balance(variant="annual", market="us")
    )

    cashflow = _flatten_index(
        sf.load_cashflow(variant="annual", market="us")
    )

    outputs = {
        "income_statements": income,
        "balance_sheets": balance,
        "cash_flow_statements": cashflow,
    }

    rows = []

    for name, df in outputs.items():
        csv_path = raw_dir / f"{name}.csv"
        parquet_path = processed_dir / f"{name}_v1.parquet"
        json_sample_path = processed_dir / f"{name}_sample_v1.json"

        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path, index=False)
        df.head(50).to_json(json_sample_path, orient="records", indent=2)

        rows.append({
            "statement": name,
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
            "csv": str(csv_path),
            "parquet": str(parquet_path),
            "json_sample": str(json_sample_path),
        })

    registry = pd.DataFrame(rows)

    registry_path = processed_dir / "simfin_ingested_statement_registry_v1.parquet"
    registry_json = processed_dir / "simfin_ingested_statement_registry_v1.json"
    summary_path = processed_dir / "simfin_ingestion_summary_v1.json"

    registry.to_parquet(registry_path, index=False)
    registry.to_json(registry_json, orient="records", indent=2)

    summary = {
        "component": "simfin_statement_ingestion_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "market": "us",
        "variant": "annual",
        "statement_count": int(len(registry)),
        "total_rows": int(registry["rows"].sum()),
        "statements": registry.to_dict(orient="records"),
        "status": "simfin_statement_ingestion_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("SimFin statement ingestion complete")
    print("Statements:", summary["statement_count"])
    print("Total Rows:", summary["total_rows"])
    print("RAW:", raw_dir)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)


if __name__ == "__main__":
    ingest_simfin_statements_v1()
