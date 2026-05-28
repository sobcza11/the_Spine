from pathlib import Path
import json

ROOT = Path.cwd()
TIER2 = ROOT / "data" / "serving" / "isovector" / "tier2"

required = [
    "executive_cognition_overlays_v1.json",
    "rbl_summary_layer_v1.json",
    "contradiction_topology_overlay_v1.json",
    "historical_analog_cognition_v1.json",
    "executive_routing_layer_v1.json",
]

failures = []

print("\n[IsoVector Tier 2 Validation]\n")

if not TIER2.exists():
    failures.append(f"Missing Tier 2 directory: {TIER2}")

for file_name in required:
    path = TIER2 / file_name

    if not path.exists():
        failures.append(f"Missing artifact: {file_name}")
        continue

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        failures.append(f"Invalid JSON: {file_name} -> {exc}")
        continue

    if "artifact" not in payload:
        failures.append(f"Missing artifact key: {file_name}")

    if "version" not in payload:
        failures.append(f"Missing version key: {file_name}")

    text = json.dumps(payload).lower()

    if "zt_mutation_allowed" in text and "false" not in text:
        failures.append(f"Governance risk: Zt mutation may not be blocked in {file_name}")

    print(f"[OK] Validated {file_name}")

print("\n[Validation Result]\n")

if failures:
    print("[FAIL] Tier 2 validation failed:\n")
    for failure in failures:
        print(f"- {failure}")
    raise SystemExit(1)

print("[PASS] Tier 2 cognition runtime artifacts validated.")

