from pathlib import Path
import json
from datetime import datetime, timezone


REPO_ROOT = Path.cwd()

COMPARTMENTS = {
    "macro": "Macro Zₜ, RBL, structural macro, drift, contradiction.",
    "equities_index": "Market indexes, beta, broad risk appetite, index breadth.",
    "equities_sector": "Industry / sector cognition, PMI linkage, sector breadth.",
    "commflow": "C_FLOW, WTI pressure, defensive/risk-on flow context.",
    "fx": "FX pressure, USD, EUR/USD, USD/JPY, currency stress.",
    "rates": "Rates Zₜ, curve pressure, policy pressure, sovereign stress.",
}

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "compartments"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SRC_DIR = REPO_ROOT / "src" / "spine" / "jobs" / "geoscen" / "compartments"
SRC_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    rows = []

    for name, purpose in COMPARTMENTS.items():
        compartment_dir = SRC_DIR / name
        compartment_dir.mkdir(parents=True, exist_ok=True)

        init_file = compartment_dir / "__init__.py"
        init_file.touch(exist_ok=True)

        rows.append({
            "compartment": name,
            "purpose": purpose,
            "source_dir": str(compartment_dir),
            "status": "scaffolded",
        })

    payload = {
        "component": "GeoScen Compartment Scaffold",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "compartment_count": len(rows),
        "compartments": rows,
        "deployment_sequence": [
            "macro",
            "equities_index",
            "equities_sector",
            "commflow",
            "fx",
            "rates",
        ],
        "governance": {
            "offline_first": True,
            "compartmentalized": True,
            "deploy_all_together_later": True,
            "cross_domain_sync_required": True,
        },
    }

    out_json = OUT_DIR / "geoscen_compartment_scaffold_v1.json"
    out_txt = OUT_DIR / "geoscen_compartment_scaffold_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN COMPARTMENT SCAFFOLD V1\n")
        f.write("=" * 60 + "\n\n")

        for row in rows:
            f.write(
                f"- {row['compartment']} | "
                f"{row['status']} | "
                f"{row['purpose']}\n"
            )

    print("OK | GeoScen compartment scaffold v1 built")
    print(f"compartment_count: {payload['compartment_count']}")
    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()

    