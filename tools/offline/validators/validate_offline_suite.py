from pathlib import Path
import re

ROOT = Path.cwd()
OFFLINE_DIR = ROOT / "dist" / "offline"

required_files = [
    "index.html",
    "FX_OC.html",
    "RATES_OC.html",
    "C_FLOW_OC.html",
    "EQUITIES_SECTOR_OC.html",
    "EQUITIES_INDUSTRY_OC.html",
]

failures = []

print("\n[IsoVector Offline Suite Validation]\n")

if not OFFLINE_DIR.exists():
    failures.append(f"Missing offline directory: {OFFLINE_DIR}")

for file_name in required_files:
    path = OFFLINE_DIR / file_name

    if not path.exists():
        failures.append(f"Missing file: {file_name}")
        continue

    text = path.read_text(encoding="utf-8", errors="ignore")

    print(f"[OK] Found {file_name}")

    if "fetch(" in text:
        failures.append(f"{file_name} contains fetch() dependency")

    if "http://" in text or "https://" in text:
        failures.append(f"{file_name} contains external URL dependency")

    if file_name != "index.html":
        if "const " not in text or "PAYLOAD" not in text:
            failures.append(f"{file_name} may not contain embedded payload")

index_path = OFFLINE_DIR / "index.html"

if index_path.exists():
    index_text = index_path.read_text(encoding="utf-8", errors="ignore")

    links = re.findall(r'href="([^"]+)"', index_text)

    for link in links:
        linked_path = OFFLINE_DIR / link

        if not linked_path.exists():
            failures.append(f"Broken index link: {link}")
        else:
            print(f"[OK] Index link valid -> {link}")

print("\n[Validation Result]\n")

if failures:
    print("[FAIL] Offline suite validation failed:\n")

    for failure in failures:
        print(f"- {failure}")

    raise SystemExit(1)

print("[PASS] Offline suite is self-contained and validation-ready.")

