from pathlib import Path
from datetime import datetime, UTC
import json
import shutil


BLOCKED_PATTERNS = [
    ".env",
    "secret",
    "token",
    "credential",
    "key.pem",
    "private"
]


def is_safe(path):

    text = str(path).lower()

    return not any(p in text for p in BLOCKED_PATTERNS)


def build_secure_runtime_export_v1():

    root = Path.cwd()
    site = root / "_offline_site"
    export = site / "cloudflare" / "secure_runtime_export"

    if export.exists():
        shutil.rmtree(export)

    export.mkdir(parents=True, exist_ok=True)

    include_dirs = [
        "config",
        "core",
        "css",
        "components",
        "geoscen_runtime",
        "finstate_runtime",
        "finstate_payloads",
        "executive_runtime",
        "langroid_runtime",
        "payloads",
        "deploy_manifest"
    ]

    copied = []

    for d in include_dirs:

        src = site / d

        if not src.exists():
            continue

        dst = export / d

        for p in src.rglob("*"):

            if p.is_file() and is_safe(p):

                rel = p.relative_to(src)
                target = dst / rel
                target.parent.mkdir(parents=True, exist_ok=True)

                shutil.copy2(p, target)

                copied.append(str((d / rel).as_posix()) if hasattr(d, "as_posix") else str(Path(d) / rel).replace("\\", "/"))

    shutil.copy2(site / "index.html", export / "index.html")

    for css_file in ["styles.css"]:
        if (site / css_file).exists():
            shutil.copy2(site / css_file, export / css_file)

    manifest = {
        "component": "secure_runtime_export_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "copied_file_count": len(copied),
        "blocked_patterns": BLOCKED_PATTERNS,
        "export_root": str(export),
        "status": "secure_runtime_export_ready"
    }

    out = site / "cloudflare" / "manifests" / "secure_runtime_export_v1.json"

    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Secure Runtime Export complete")
    print("Copied:", manifest["copied_file_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_secure_runtime_export_v1()
