from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_recursive_state_memory_v1():
    root = Path.cwd()
    out = root / "data" / "propagation"
    out.mkdir(parents=True, exist_ok=True)

    summary_files = list(root.glob("data/**/*summary_v1.json"))
    rows = []

    for fp in summary_files:
        try:
            s = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue

        component = s.get("component", fp.stem)
        status = s.get("status", "unknown")
        generated_at = s.get("generated_at_utc")

        pressure_keys = [k for k in s if k.endswith("_pressure")]
        state_keys = [k for k in s if k.endswith("_state")]

        rows.append({
            "component": component,
            "status": status,
            "generated_at_utc": generated_at,
            "pressure": s.get(pressure_keys[0]) if pressure_keys else None,
            "state": s.get(state_keys[0]) if state_keys else None,
            "source_file": str(fp.relative_to(root)),
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        for c in ["component", "status", "generated_at_utc", "state", "source_file"]:
            df[c] = df[c].fillna("").astype(str)
        df["pressure"] = pd.to_numeric(df["pressure"], errors="coerce")
        df = df.sort_values(["component", "generated_at_utc"]).reset_index(drop=True)

    summary = {
        "component": "recursive_state_memory_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "memory_rows": int(len(df)),
        "component_count": int(df["component"].nunique()) if not df.empty else 0,
        "status": "recursive_state_memory_complete",
    }

    df.to_parquet(out / "recursive_state_memory_v1.parquet", index=False)
    df.to_json(out / "recursive_state_memory_v1.json", orient="records", indent=2)

    with open(out / "recursive_state_memory_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive State Memory complete")
    print("Rows:", summary["memory_rows"])
    print("Components:", summary["component_count"])


if __name__ == "__main__":
    build_recursive_state_memory_v1()
