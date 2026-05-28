from __future__ import annotations

import json
import shutil
from pathlib import Path
from datetime import datetime, timezone


REPO_ROOT = Path.cwd()

OFFLINE_SITE_DIR = REPO_ROOT / "_offline_site"
RUNTIME_DIR = OFFLINE_SITE_DIR / "geoscen_runtime"

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "offline"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OFFLINE_SITE_DIR.mkdir(parents=True, exist_ok=True)
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


SOURCE_FILES = {
    "frontend": REPO_ROOT / "data" / "serving" / "geoscen" / "frontend" / "geoscen_frontend_intelligence_layer_v1.json",
    "contradiction": REPO_ROOT / "data" / "serving" / "geoscen" / "contradiction" / "geoscen_contradiction_engine_v1.json",
    "drift": REPO_ROOT / "data" / "serving" / "geoscen" / "drift" / "geoscen_historical_narrative_drift_engine_v1.json",
    "cb_cognition": REPO_ROOT / "data" / "serving" / "geoscen" / "cb" / "geoscen_cross_country_policy_cognition_v1.json",
    "macro_registry": REPO_ROOT / "data" / "serving" / "geoscen" / "macro" / "geoscen_macro_registry_v1.json",
    "macro_ingestion": REPO_ROOT / "data" / "serving" / "geoscen" / "macro" / "geoscen_macro_ingestion_v1.json",
    "rbl": REPO_ROOT / "data" / "serving" / "geoscen" / "geoscen_rbl_synthesis_v1.json",
    "structural": REPO_ROOT / "data" / "serving" / "geoscen" / "structure" / "geoscen_structural_macro_layer_v1.json",
}


def read_json(path: Path) -> dict:
    if not path.exists():
        return {"available": False}

    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    if isinstance(obj, dict):
        obj["available"] = True

    return obj


def sync_runtime_files() -> list[dict]:
    rows = []

    for name, src in SOURCE_FILES.items():
        available = src.exists()

        if available:
            dst = RUNTIME_DIR / src.name
            shutil.copy2(src, dst)
            target = str(dst)
            status = "synced"
        else:
            target = None
            status = "missing"

        rows.append({
            "component": name,
            "source": str(src),
            "target": target,
            "available": available,
            "sync_status": status,
        })

    return rows


def build_index_html() -> None:
    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>GeoScen Offline Runtime</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link rel="stylesheet" href="./styles.css" />
</head>
<body>
  <main class="shell">
    <header>
      <div>
        <p class="eyebrow">GEOSCEN | OFFLINE</p>
        <h1>GeoScen Runtime Console</h1>
      </div>
      <div id="runtime-status" class="status-pill">Loading</div>
    </header>

    <section class="grid">
      <article class="card">
        <h2>Macro Zₜ</h2>
        <div id="macro-zt" class="metric">--</div>
        <p id="macro-rbl">Loading macro state...</p>
      </article>

      <article class="card">
        <h2>Graph Area</h2>
        <ul id="graph-routes"></ul>
      </article>

      <article class="card wide">
        <h2>RBL</h2>
        <p id="rbl-text">Loading RBL...</p>
      </article>

      <article class="card">
        <h2>Final Metric</h2>
        <div id="final-metric" class="metric">-- / 100</div>
      </article>

      <article class="card">
        <h2>Contradiction</h2>
        <p id="contradiction-text">Loading...</p>
      </article>

      <article class="card">
        <h2>Drift</h2>
        <p id="drift-text">Loading...</p>
      </article>

      <article class="card">
        <h2>Structural Macro</h2>
        <p id="structural-text">Loading...</p>
      </article>

      <article class="card">
        <h2>CB Cognition</h2>
        <p id="cb-text">Loading...</p>
      </article>
    </section>
  </main>

  <script src="./app.js"></script>
</body>
</html>
"""
    (OFFLINE_SITE_DIR / "index.html").write_text(html, encoding="utf-8")


def build_styles_css() -> None:
    css = """
:root {
  font-family: Inter, Segoe UI, Arial, sans-serif;
  background: #0b0f14;
  color: #e8eef5;
}

body {
  margin: 0;
  background: radial-gradient(circle at top, #16202c, #0b0f14 55%);
}

.shell {
  max-width: 1180px;
  margin: 0 auto;
  padding: 28px;
}

header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
}

h1, h2, p {
  margin-top: 0;
}

.eyebrow {
  color: #8fb3ff;
  letter-spacing: 0.16em;
  font-size: 12px;
  margin-bottom: 6px;
}

.status-pill {
  border: 1px solid #2c425c;
  padding: 8px 12px;
  border-radius: 999px;
  background: #111a24;
}

.grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
}

.card {
  background: rgba(15, 24, 34, 0.9);
  border: 1px solid #233244;
  border-radius: 18px;
  padding: 18px;
  min-height: 150px;
  box-shadow: 0 18px 48px rgba(0,0,0,0.24);
}

.card.wide {
  grid-column: span 2;
}

.metric {
  font-size: 32px;
  font-weight: 700;
  margin-bottom: 12px;
}

ul {
  padding-left: 18px;
}

li {
  margin-bottom: 8px;
  color: #cbd8e6;
}

p {
  color: #cbd8e6;
  line-height: 1.45;
}
"""
    (OFFLINE_SITE_DIR / "styles.css").write_text(css, encoding="utf-8")


def build_app_js() -> None:
    js = """
async function loadJson(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Failed to load ${path}`);
  return await res.json();
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text ?? "Unavailable";
}

async function hydrate() {
  try {
    const frontend = await loadJson("./geoscen_runtime/geoscen_frontend_intelligence_layer_v1.json");
    const contradiction = await loadJson("./geoscen_runtime/geoscen_contradiction_engine_v1.json");
    const drift = await loadJson("./geoscen_runtime/geoscen_historical_narrative_drift_engine_v1.json");
    const structural = await loadJson("./geoscen_runtime/geoscen_structural_macro_layer_v1.json");
    const cb = await loadJson("./geoscen_runtime/geoscen_cross_country_policy_cognition_v1.json");

    setText("runtime-status", "Healthy | Offline");
    setText("macro-zt", frontend.temperature || "UNKNOWN");
    setText("macro-rbl", `Temperature score: ${frontend.temperature_score ?? "--"}`);

    const directRbl = await loadJson("./geoscen_runtime/geoscen_rbl_synthesis_v1.json").catch(() => null);

    const rblText =
    frontend.rbl ||
    frontend?.evidence?.geoscen_rbl?.rbl ||
    directRbl?.rbl ||
    directRbl?.rbl_text ||
    directRbl?.summary ||
    directRbl?.report ||
    JSON.stringify(directRbl, null, 2) ||
    "RBL unavailable";

    setText("rbl-text", rblText);    

    setText("final-metric", `${Math.round((frontend.temperature_score || 0) * 100)} / 100`);

    const routes = document.getElementById("graph-routes");
    if (routes) {
      routes.innerHTML = "";
      (frontend.graph_routes || []).forEach(route => {
        const li = document.createElement("li");
        li.textContent = `${route.title} → ${route.component}`;
        routes.appendChild(li);
      });
    }

    setText(
      "contradiction-text",
      `${contradiction.contradiction_severity || "none"} | score=${contradiction.contradiction_score ?? 0}`
    );

    setText(
      "drift-text",
      `${drift.drift_regime || "Unavailable"} | score=${drift.drift_score ?? "--"}`
    );

    setText(
      "structural-text",
      `${structural.structural_regime || "Unavailable"} | score=${structural.structural_pressure_score ?? "--"}`
    );

    setText(
      "cb-text",
      `Active CBs: ${cb.active_cb_count ?? "--"} / ${cb.tracked_cb_count ?? "--"}`
    );
  } catch (err) {
    setText("runtime-status", "Runtime Error");
    console.error(err);
  }
}

hydrate();
"""
    (OFFLINE_SITE_DIR / "app.js").write_text(js, encoding="utf-8")


def main() -> None:
    sync_rows = sync_runtime_files()

    build_index_html()
    build_styles_css()
    build_app_js()

    entrypoints = {
        "index_html": (OFFLINE_SITE_DIR / "index.html").exists(),
        "app_js": (OFFLINE_SITE_DIR / "app.js").exists(),
        "styles_css": (OFFLINE_SITE_DIR / "styles.css").exists(),
    }

    synced_count = sum(1 for row in sync_rows if row["sync_status"] == "synced")
    total_count = len(sync_rows)

    hydration_ready = (
        synced_count == total_count
        and all(entrypoints.values())
    )

    payload = {
        "component": "GeoScen Offline Site Hydration",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "offline_site_dir": str(OFFLINE_SITE_DIR),
        "runtime_dir": str(RUNTIME_DIR),
        "localhost_target": "http://localhost:8787/",
        "sync_rows": sync_rows,
        "entrypoints": entrypoints,
        "synced_count": synced_count,
        "total_count": total_count,
        "hydration_ready": hydration_ready,
        "governance": {
            "offline_first": True,
            "localhost_only": True,
            "frontend_synchronized": True,
            "runtime_hydrated": True,
            "cloud_not_required": True,
        },
    }

    out_json = OUT_DIR / "geoscen_offline_site_hydration_v1.json"
    out_txt = OUT_DIR / "geoscen_offline_site_hydration_v1.txt"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN OFFLINE SITE HYDRATION V1\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"localhost_target: {payload['localhost_target']}\n")
        f.write(f"hydration_ready: {payload['hydration_ready']}\n")
        f.write(f"synced_count: {payload['synced_count']}\n")
        f.write(f"total_count: {payload['total_count']}\n\n")

        f.write("ENTRYPOINTS\n")
        f.write("-" * 60 + "\n")
        for k, v in entrypoints.items():
            f.write(f"- {k}: {v}\n")

        f.write("\nSYNC ROWS\n")
        f.write("-" * 60 + "\n")
        for row in sync_rows:
            f.write(
                f"- {row['component']} | "
                f"{row['sync_status']} | "
                f"available={row['available']}\n"
            )

    print("OK | GeoScen Offline Site Hydration v1 built")
    print(f"hydration_ready : {hydration_ready}")
    print(f"synced_count    : {synced_count}")
    print(f"total_count     : {total_count}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)


if __name__ == "__main__":
    main()

