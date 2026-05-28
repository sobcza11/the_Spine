from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

def build_simfin_statement_registry_v1():
    root = Path.cwd()

    raw_dir = root / "data" / "fundamentals" / "simfin" / "raw"
    out_dir = root / "data" / "fundamentals" / "simfin" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    expected_files = {
        "income_statement": raw_dir / "income_statements.csv",
        "balance_sheet": raw_dir / "balance_sheets.csv",
        "cash_flow_statement": raw_dir / "cash_flow_statements.csv",
    }

    rows = []

    for statement_type, path in expected_files.items():
        rows.append({
            "statement_type": statement_type,
            "path": str(path),
            "exists": path.exists(),
            "source": "simfin",
            "status": "available" if path.exists() else "missing",
        })

    df = pd.DataFrame(rows)

    summary = {
        "component": "simfin_statement_registry_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "statement_count": int(len(df)),
        "available_count": int(df["exists"].sum()),
        "missing_count": int((~df["exists"]).sum()),
        "status": "simfin_statement_registry_complete",
    }

    df.to_parquet(out_dir / "simfin_statement_registry_v1.parquet", index=False)
    df.to_json(out_dir / "simfin_statement_registry_v1.json", orient="records", indent=2)

    with open(out_dir / "simfin_statement_registry_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("SimFin statement registry complete")
    print("Available:", summary["available_count"])
    print("Missing:", summary["missing_count"])
    print("SUMMARY:", out_dir / "simfin_statement_registry_summary_v1.json")

if __name__ == "__main__":
    build_simfin_statement_registry_v1()
