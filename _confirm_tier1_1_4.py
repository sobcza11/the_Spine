from pathlib import Path
import json

root = Path.cwd()

checks = {
    "FinState Recursive Deepening": root / "data/finstate/recursive/finstate_recursive_deepening_summary_v1.json",
    "COT Recursive Completion": root / "data/cot/recursive/cot_recursive_completion_summary_v1.json",
    "Equities Recursive Expansion": root / "data/equities/recursive/equities_recursive_expansion_summary_v1.json",
    "Commodities Recursive Expansion": root / "data/commodities/recursive/commodities_recursive_expansion_summary_v1.json",
}

print("\nTIER 1 ITEMS 1-4 CONFIRMATION")
print("=" * 60)

all_ok = True

for name, path in checks.items():
    exists = path.exists()
    all_ok = all_ok and exists

    if exists:
        with open(path, "r", encoding="utf-8") as f:
            summary = json.load(f)

        status = summary.get("status")
        pressure_key = [k for k in summary.keys() if k.endswith("_pressure")][0]
        state_key = [k for k in summary.keys() if k.endswith("_state")][0]

        print(f"{name}: COMPLETE")
        print(f"  Status: {status}")
        print(f"  Pressure: {summary.get(pressure_key)}")
        print(f"  State: {summary.get(state_key)}")
    else:
        print(f"{name}: MISSING")
        print(f"  Missing: {path}")

print("=" * 60)
print("TIER 1 ITEMS 1-4 STATUS:", "COMPLETE" if all_ok else "INCOMPLETE")
