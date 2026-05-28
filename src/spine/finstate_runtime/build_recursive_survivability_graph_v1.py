from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


def build_recursive_survivability_graph_v1():

    root = Path.cwd()

    src = (
        root
        / "data"
        / "i2_quarterly"
        / "i2_quarterly_survivability_intelligence_steps_6_10_v1.parquet"
    )

    if not src.exists():
        raise FileNotFoundError(src)

    df = pd.read_parquet(src)

    latest = (
        df.sort_values(["symbol", "statement_date"])
        .groupby("symbol", as_index=False)
        .tail(1)
    )

    pressure_col = (
        "q_dynamic_survivability_conditioning"
        if "q_dynamic_survivability_conditioning" in latest.columns
        else "q_quarterly_systemicity_overlay"
    )

    edges = []

    for _, r in latest.iterrows():

        symbol = str(r["symbol"])
        pressure = float(r.get(pressure_col, 0) or 0)

        edges.append({
            "source_node": symbol,
            "target_node": "FINSTATE",
            "edge_type": "survivability_conditioning",
            "edge_pressure": round(pressure, 4)
        })

        edges.append({
            "source_node": "FINSTATE",
            "target_node": "GeoScen",
            "edge_type": "macro_corporate_coupling",
            "edge_pressure": round(pressure, 4)
        })

    payload = {
        "component": "recursive_survivability_graph_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "symbols": int(latest["symbol"].nunique()),
        "edge_count": len(edges),
        "pressure_column": pressure_col,
        "sample_edges": edges[:500],
        "status": "recursive_survivability_graph_ready"
    }

    out = (
        root
        / "_offline_site"
        / "finstate_payloads"
        / "recursive_survivability_graph_v1.json"
    )

    out.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )

    print("Recursive Survivability Graph complete")
    print("Symbols:", payload["symbols"])
    print("Edges:", payload["edge_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_recursive_survivability_graph_v1()
