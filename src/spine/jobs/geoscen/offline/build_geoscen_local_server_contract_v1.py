from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "offline"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OFFLINE_SITE_DIR = REPO_ROOT / "_offline_site"
RUNTIME_DIR = OFFLINE_SITE_DIR / "geoscen_runtime"

SERVER_HOST = "localhost"
SERVER_PORT = 8787
SERVER_URL = f"http://{SERVER_HOST}:{SERVER_PORT}/"


def main() -> None:
    site_exists = OFFLINE_SITE_DIR.exists()
    runtime_exists = RUNTIME_DIR.exists()

    index_html = OFFLINE_SITE_DIR / "index.html"
    app_js = OFFLINE_SITE_DIR / "app.js"
    styles_css = OFFLINE_SITE_DIR / "styles.css"

    runtime_files = sorted(RUNTIME_DIR.glob("*.json")) if runtime_exists else []

    payload = {
        "component": "GeoScen Local Server Launch Contract",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "server_url": SERVER_URL,
        "server_host": SERVER_HOST,
        "server_port": SERVER_PORT,
        "offline_mode": True,
        "site_exists": site_exists,
        "runtime_exists": runtime_exists,
        "runtime_file_count": len(runtime_files),
        "entrypoints": {
            "index_html": str(index_html),
            "app_js": str(app_js),
            "styles_css": str(styles_css),
        },
        "entrypoint_status": {
            "index_html": index_html.exists(),
            "app_js": app_js.exists(),
            "styles_css": styles_css.exists(),
        },
        "launch_commands": {
            "powershell": [
                "Set-Location .\\_offline_site",
                "python -m http.server 8787",
            ],
            "browser": SERVER_URL,
        },
        "health_rules": {
            "site_required": True,
            "runtime_required": True,
            "runtime_json_required": True,
            "index_required": True,
        },
        "launch_ready": (
            site_exists
            and runtime_exists
            and len(runtime_files) > 0
            and index_html.exists()
        ),
        "governance": {
            "offline_first": True,
            "localhost_only": True,
            "cloud_not_required": True,
            "public_network_exposure": False,
            "runtime_artifacts_synced": True,
        },
    }

    out_json = OUT_DIR / "geoscen_local_server_contract_v1.json"
    out_txt = OUT_DIR / "geoscen_local_server_contract_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN LOCAL SERVER LAUNCH CONTRACT V1\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"server_url: {payload['server_url']}\n")
        f.write(f"offline_mode: {payload['offline_mode']}\n")
        f.write(f"launch_ready: {payload['launch_ready']}\n")
        f.write(f"runtime_file_count: {payload['runtime_file_count']}\n\n")

        f.write("ENTRYPOINT STATUS\n")
        f.write("-" * 60 + "\n")
        for k, v in payload["entrypoint_status"].items():
            f.write(f"- {k}: {v}\n")

        f.write("\nLAUNCH COMMANDS\n")
        f.write("-" * 60 + "\n")
        f.write("Set-Location .\\_offline_site\n")
        f.write("python -m http.server 8787\n")
        f.write("Open browser: http://localhost:8787/\n")

    print("OK | GeoScen Local Server Launch Contract v1 built")
    print(f"server_url         : {payload['server_url']}")
    print(f"launch_ready       : {payload['launch_ready']}")
    print(f"runtime_file_count : {payload['runtime_file_count']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()

    