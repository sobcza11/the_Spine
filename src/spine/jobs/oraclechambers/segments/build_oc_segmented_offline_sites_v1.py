from pathlib import Path
import json
from datetime import datetime, timezone


REPO_ROOT = Path.cwd()

OFFLINE_ROOT = REPO_ROOT / "_offline_site"
SEGMENT_ROOT = OFFLINE_ROOT / "oc_segments"

SOURCE_PAYLOAD = (
    REPO_ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "oc_local_site_hydration_v1.json"
)

SEGMENTS = [
    {
        "segment_id": "core_runtime",
        "site_name": "oc-core-runtime-local",
        "title": "OC Core Runtime Local",
        "role": "Core hydration, runtime state, API health, refresh control.",
        "online_mirror": "oc-core-runtime-online",
    },
    {
        "segment_id": "governance",
        "site_name": "oc-governance-local",
        "title": "OC Governance Local",
        "role": "Governance, provenance, source rules, AI non-orchestration.",
        "online_mirror": "oc-governance-online",
    },
    {
        "segment_id": "contradiction",
        "site_name": "oc-contradiction-local",
        "title": "OC Contradiction Local",
        "role": "Cross-asset disagreement, fragmentation, regime conflict.",
        "online_mirror": "oc-contradiction-online",
    },
    {
        "segment_id": "executive_command",
        "site_name": "oc-executive-command-local",
        "title": "OC Executive Command Local",
        "role": "Executive aggregation view across approved offline segments.",
        "online_mirror": "oc-executive-command-online",
    },
]


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def load_payload() -> dict:
    if not SOURCE_PAYLOAD.exists():
        return {
            "deployment_ready": False,
            "error": f"Missing source payload: {SOURCE_PAYLOAD}",
        }

    return json.loads(SOURCE_PAYLOAD.read_text(encoding="utf-8"))


def build_manifest(segment: dict, payload: dict) -> dict:
    return {
        "artifact": "oc_segment_offline_site_manifest_v1",
        "segment_id": segment["segment_id"],
        "site_name": segment["site_name"],
        "title": segment["title"],
        "role": segment["role"],
        "online_mirror": segment["online_mirror"],
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "offline_site_ready": True,
        "online_transition_allowed": False,
        "source_payload": str(SOURCE_PAYLOAD),
        "payload_deployment_ready": bool(payload.get("deployment_ready")),
    }


def build_index_html(segment: dict) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{segment["title"]}</title>
  <link rel="stylesheet" href="./styles.css" />
</head>
<body>
  <main class="shell">
    <header class="hero">
      <div class="eyebrow">ORACLECHAMBERS | OFFLINE SEGMENT</div>
      <h1>{segment["title"]}</h1>
      <p>{segment["role"]}</p>
    </header>

    <section class="grid">
      <article class="card">
        <h2>Segment Status</h2>
        <p id="segment-status">Loading...</p>
      </article>

      <article class="card">
        <h2>Online Mirror</h2>
        <p id="online-mirror">Loading...</p>
      </article>

      <article class="card">
        <h2>Deployment Gate</h2>
        <p id="deployment-gate">Loading...</p>
      </article>
    </section>

    <section class="card wide">
      <h2>Cognition Payload Summary</h2>
      <pre id="payload-summary">Loading payload...</pre>
    </section>
  </main>

  <script src="./app.js"></script>
</body>
</html>
"""


def build_styles_css() -> str:
    return """body {
  margin: 0;
  background: #020617;
  color: #e5e7eb;
  font-family: Arial, sans-serif;
}

.shell {
  padding: 32px;
}

.hero {
  border: 1px solid #334155;
  border-radius: 18px;
  padding: 24px;
  background: #0f172a;
  margin-bottom: 24px;
}

.eyebrow {
  color: #94a3b8;
  font-size: 12px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 16px;
}

.card {
  border: 1px solid #334155;
  border-radius: 18px;
  padding: 20px;
  background: #111827;
}

.wide {
  margin-top: 16px;
}

pre {
  white-space: pre-wrap;
  color: #cbd5e1;
}
"""


def build_app_js() -> str:
    return """async function loadSegment() {
  const manifest = await fetch("./manifest.json").then((r) => r.json());
  const payload = await fetch("./payloads/oc_local_site_hydration_v1.json").then((r) => r.json());

  document.getElementById("segment-status").textContent =
    manifest.offline_site_ready ? "Offline segment ready" : "Offline segment not ready";

  document.getElementById("online-mirror").textContent =
    manifest.online_mirror;

  document.getElementById("deployment-gate").textContent =
    manifest.online_transition_allowed
      ? "Online transition allowed"
      : "Online transition blocked pending validation";

  const sitePayload = payload.site_payload || {};
  const headline = sitePayload.headline || {};
  const narrative = sitePayload.narrative || {};

  document.getElementById("payload-summary").textContent = JSON.stringify({
    deployment_ready: payload.deployment_ready,
    site_mode: sitePayload.site_mode,
    runtime_mode: sitePayload.runtime_mode,
    regime: headline.regime,
    confidence: headline.confidence,
    conviction: headline.conviction,
    macro_temperature: headline.macro_temperature,
    executive_summary: narrative.executive_summary
  }, null, 2);
}

loadSegment().catch((error) => {
  document.getElementById("payload-summary").textContent =
    "Segment load failed: " + error.message;
});
"""


def build_segment_site(segment: dict, payload: dict) -> dict:
    site_dir = SEGMENT_ROOT / segment["site_name"]
    payload_dir = site_dir / "payloads"

    manifest = build_manifest(segment, payload)

    write_text(site_dir / "index.html", build_index_html(segment))
    write_text(site_dir / "styles.css", build_styles_css())
    write_text(site_dir / "app.js", build_app_js())
    write_text(site_dir / "manifest.json", json.dumps(manifest, indent=2))

    write_text(
        payload_dir / "oc_local_site_hydration_v1.json",
        json.dumps(payload, indent=2),
    )

    return {
        "segment_id": segment["segment_id"],
        "site_name": segment["site_name"],
        "path": str(site_dir),
        "offline_site_ready": True,
    }


def main() -> None:
    payload = load_payload()

    results = [
        build_segment_site(segment, payload)
        for segment in SEGMENTS
    ]

    summary = {
        "artifact": "build_oc_segmented_offline_sites_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "segment_count": len(results),
        "offline_sites_ready": all(item["offline_site_ready"] for item in results),
        "results": results,
    }

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

    