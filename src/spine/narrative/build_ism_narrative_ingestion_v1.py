from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_ism_narrative_ingestion_v1():
    root = Path.cwd()
    out = root / "data" / "narrative"
    out.mkdir(parents=True, exist_ok=True)

    candidates = list(root.glob("data/**/*ism*.*")) + list(root.glob("data/**/*ISM*.*"))

    rows = []
    for fp in candidates:
        if fp.suffix.lower() not in [".txt", ".csv", ".json", ".md"]:
            continue

        try:
            text = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        rows.append({
            "source_file": str(fp.relative_to(root)),
            "text_length": len(text),
            "text_sample": text[:1000],
            "ingestion_status": "ingested",
        })

    df = pd.DataFrame(rows)

    if df.empty:
        df = pd.DataFrame([{
            "source_file": "placeholder_ism_commentary",
            "text_length": 0,
            "text_sample": "",
            "ingestion_status": "placeholder_no_live_ism_files_found",
        }])

    summary = {
        "component": "ism_narrative_ingestion_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_count": int(len(df)),
        "live_source_count": int((df["ingestion_status"] == "ingested").sum()),
        "status": "ism_narrative_ingestion_complete",
    }

    df.to_parquet(out / "ism_narrative_ingestion_v1.parquet", index=False)
    df.to_json(out / "ism_narrative_ingestion_v1.json", orient="records", indent=2)

    with open(out / "ism_narrative_ingestion_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("ISM Narrative Ingestion complete")
    print("Sources:", summary["source_count"])
    print("Live sources:", summary["live_source_count"])


if __name__ == "__main__":
    build_ism_narrative_ingestion_v1()
