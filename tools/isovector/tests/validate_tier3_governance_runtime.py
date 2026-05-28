from pathlib import Path
import json

ROOT = Path.cwd()
TIER3 = ROOT / "data" / "serving" / "isovector" / "tier3"

required = [
    TIER3 / "robustness" / "robustness_framework_v1.json",
    TIER3 / "drift" / "drift_monitoring_framework_v1.json",
    TIER3 / "calibration" / "calibration_overfitting_governance_v1.json",
    TIER3 / "governance" / "governed_validation_framework_v1.json",
]

failures = []

print("\n[IsoVector Tier 3 Governance Validation]\n")

for path in required:
    if not path.exists():
        failures.append(f"Missing artifact: {path}")
        continue

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        failures.append(f"Invalid JSON: {path.name} -> {exc}")
        continue

    if "artifact" not in payload:
        failures.append(f"Missing artifact key: {path.name}")

    if "version" not in payload:
        failures.append(f"Missing version key: {path.name}")

    text = json.dumps(payload).lower()

    if '"zt_mutation_allowed": true' in text:
        failures.append(f"Zt mutation allowed in: {path.name}")

    if "governance" not in payload:
        failures.append(f"Missing governance section: {path.name}")

    print(f"[OK] Validated {path.name}")

print("\n[Validation Result]\n")

if failures:
    print("[FAIL] Tier 3 validation failed:\n")
    for failure in failures:
        print(f"- {failure}")
    raise SystemExit(1)

print("[PASS] Tier 3 governance / robustness runtime validated.")

