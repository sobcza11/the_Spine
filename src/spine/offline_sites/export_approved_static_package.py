from pathlib import Path
from datetime import datetime, timezone
import json
import shutil


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

SRC = ROOT / "data" / "offline_sites"

OUT_DIR = ROOT / "data" / "deploy_static" / "isovector_offline_approved"

SITES = [
    "equities-industry",
    "equities-sector",
    "c-flow",
    "fx",
    "rates",
]


def copy_site(site):
    src_dir = SRC / site
    dst_dir = OUT_DIR / site

    if dst_dir.exists():
        shutil.rmtree(dst_dir)

    shutil.copytree(src_dir, dst_dir)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    shutil.copy2(
        SRC / "index.html",
        OUT_DIR / "index.html",
    )

    for site in SITES:
        copy_site(site)

    (OUT_DIR / "styles.css").write_text(
        "body { font-family: Arial, sans-serif; margin: 32px; }",
        encoding="utf-8",
    )

    (OUT_DIR / "app.js").write_text(
        "console.log('IsoVector offline approved package loaded');",
        encoding="utf-8",
    )

    manifest = {
        "system": "IsoVector",
        "module": "approved-static-export",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "deployment_target": "isovector.io",
        "export_package": "isovector_offline_approved",
        "site_count": len(SITES),
        "sites": SITES,
        "governance": {
            "offline_review_complete_required": True,
            "writeback_allowed": False,
            "human_review_required": True,
            "ready_for_static_handoff": True,
        },
    }

    (OUT_DIR / "deployment_manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_DIR}")


if __name__ == "__main__":
    main()
