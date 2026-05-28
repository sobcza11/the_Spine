from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_i2_statement_normalizer_v1():
    root = Path.cwd()
    src = root / "data" / "fundamentals" / "simfin"
    out = root / "data" / "i2"
    out.mkdir(parents=True, exist_ok=True)

    files = list(src.glob("**/*.parquet"))

    rows = []
    for fp in files:
        try:
            df = pd.read_parquet(fp)
        except Exception:
            continue

        cols = {c.lower(): c for c in df.columns}

        symbol_col = (
            cols.get("ticker")
            or cols.get("symbol")
        )

        date_col = (
            cols.get("date")
            or cols.get("report date")
            or cols.get("reportdate")
            or cols.get("fiscaldate")
            or cols.get("publish date")
        )

        if symbol_col is None:
            continue

        df = df.copy()
        df["source_file"] = str(fp.relative_to(root))
        df["statement_family"] = fp.stem.lower()

        if date_col:
            df["statement_date"] = pd.to_datetime(df[date_col], errors="coerce")
        else:
            df["statement_date"] = pd.NaT

        df["symbol"] = df[symbol_col].astype(str)

        rows.append(df)

    if not rows:
        raise FileNotFoundError(f"No SimFin parquet statement files found under: {src}")

    panel = pd.concat(rows, ignore_index=True, sort=False)

    summary = {
        "component": "i2_statement_normalizer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": int(len(panel)),
        "symbol_count": int(panel["symbol"].nunique()),
        "source_file_count": int(len(files)),
        "status": "i2_statement_normalizer_complete",
    }

    panel.to_parquet(out / "i2_statement_normalized_v1.parquet", index=False)
    panel.head(500).to_json(out / "i2_statement_normalized_sample_v1.json", orient="records", indent=2)

    with open(out / "i2_statement_normalizer_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("I2 Statement Normalizer complete")
    print("Rows:", summary["row_count"])
    print("Symbols:", summary["symbol_count"])


if __name__ == "__main__":
    build_i2_statement_normalizer_v1()

