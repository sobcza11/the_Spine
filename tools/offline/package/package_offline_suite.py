from pathlib import Path
import shutil
import json
from datetime import datetime

ROOT = Path.cwd()

OFFLINE_DIR = ROOT / "dist" / "offline"
PACKAGE_ROOT = ROOT / "dist" / "packages"
PACKAGE_DIR = PACKAGE_ROOT / "IsoVector_Offline_Cognition_Suite"

required_files = [
    "index.html",
    "FX_OC.html",
    "RATES_OC.html",
    "C_FLOW_OC.html",
    "EQUITIES_SECTOR_OC.html",
    "EQUITIES_INDUSTRY_OC.html",
]

if PACKAGE_DIR.exists():
    shutil.rmtree(PACKAGE_DIR)

PACKAGE_DIR.mkdir(parents=True, exist_ok=True)

for file_name in required_files:
    src = OFFLINE_DIR / file_name
    dst = PACKAGE_DIR / file_name

    if not src.exists():
        raise FileNotFoundError(f"Missing required offline file: {src}")

    shutil.copy2(src, dst)

manifest = {
    "package_name": "IsoVector Offline Cognition Suite",
    "version": "v1",
    "created_at": datetime.now().isoformat(timespec="seconds"),
    "deployment_type": "standalone_offline_html",
    "server_required": False,
    "powershell_required": False,
    "python_required_at_runtime": False,
    "fetch_dependencies": False,
    "planes": [
        "FX",
        "RATES",
        "C_FLOW",
        "EQUITIES_SECTOR",
        "EQUITIES_INDUSTRY",
    ],
    "entrypoint": "index.html",
}

(PACKAGE_DIR / "manifest.json").write_text(
    json.dumps(manifest, indent=2),
    encoding="utf-8",
)

readme = """
IsoVector Offline Cognition Suite
=================================

Deployment Type:
Standalone offline HTML cognition suite.

Runtime Requirements:
- No Python required
- No PowerShell required
- No local server required
- No fetch() dependency required

How To Launch:
1. Open index.html directly in a browser.
2. Select a cognition plane.
3. Each plane runs as a standalone embedded-payload HTML file.

Included Planes:
- FX
- RATES
- C_FLOW
- EQUITIES_SECTOR
- EQUITIES_INDUSTRY

Governance Note:
This package preserves deterministic signal payloads inside static standalone HTML files.
AI-assisted interpretation layers must not mutate embedded runtime truth.
"""

(PACKAGE_DIR / "README.txt").write_text(
    readme.strip(),
    encoding="utf-8",
)

zip_path = PACKAGE_ROOT / "IsoVector_Offline_Cognition_Suite.zip"

if zip_path.exists():
    zip_path.unlink()

shutil.make_archive(
    str(zip_path.with_suffix("")),
    "zip",
    PACKAGE_DIR,
)

print(f"[OK] Package directory built -> {PACKAGE_DIR}")
print(f"[OK] Package archive built -> {zip_path}")

