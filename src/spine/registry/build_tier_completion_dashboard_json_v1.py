from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def build_tier_completion_dashboard_json_v1():
    root = Path.cwd()
    registry_path = root / "data" / "registry" / "tier_status_registry_v1.parquet"
    out = root / "data" / "registry"
    out.mkdir(parents=True, exist_ok=True)

    if not registry_path.exists():
        raise FileNotFoundError(f"Missing registry: {registry_path}")

    df = pd.read_parquet(registry_path)

    tier_rows = []

    for tier, g in df.groupby("tier"):
        total = int(len(g))
        complete = int(g["complete"].sum())
        avg_pressure = round(float(g["pressure"].dropna().mean()), 4) if g["pressure"].notna().any() else None

        tier_rows.append({
            "tier": tier,
            "module_count": total,
            "complete_count": complete,
            "completion_rate": round(complete / total, 4) if total else 0,
            "average_pressure": avg_pressure,
            "dominant_state": g["state"].dropna().mode().iloc[0] if g["state"].dropna().shape[0] else None,
        })

    dashboard = {
        "component": "tier_completion_dashboard_json_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "overall_module_count": int(len(df)),
        "overall_complete_count": int(df["complete"].sum()),
        "overall_completion_rate": round(float(df["complete"].mean()), 4) if len(df) else 0,
        "tiers": tier_rows,
        "status": "tier_completion_dashboard_json_complete",
    }

    with open(out / "tier_completion_dashboard_v1.json", "w", encoding="utf-8") as f:
        json.dump(dashboard, f, indent=2)

    print("Tier Completion Dashboard JSON complete")
    print("Overall completion:", dashboard["overall_completion_rate"])
    print("OUTPUT:", out / "tier_completion_dashboard_v1.json")


if __name__ == "__main__":
    build_tier_completion_dashboard_json_v1()
