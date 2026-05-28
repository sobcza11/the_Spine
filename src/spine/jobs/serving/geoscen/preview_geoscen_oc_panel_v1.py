from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path.cwd()

PANEL_PATH = REPO_ROOT / "data" / "serving" / "fx" / "fx_panel_v1.json"
OUTPUT_PATH = REPO_ROOT / "data" / "serving" / "fx" / "fx_panel_v1.html"


def safe_json(value: Any) -> str:
    return json.dumps(value, indent=2, default=str)


def main() -> None:
    panel = json.loads(PANEL_PATH.read_text(encoding="utf-8"))
    layout = panel.get("layout", {})

    top_left = layout.get("top_left", {})
    top_right = layout.get("top_right", {})
    bottom_left = layout.get("bottom_left", {})
    bottom_right = layout.get("bottom_right", {})

    series = (
        top_right.get("series")
        or top_right.get("zt_timeseries")
        or []
    )

    chart_labels = [x.get("date") for x in series]
    chart_values = [
        x.get("value", x.get("zt"))
        for x in series
    ]

    top_left_value = (
        top_left.get("value")
        if top_left.get("value") is not None
        else top_left.get("zt_value", "n/a")
    )

    bottom_text = (
        bottom_left.get("text")
        or bottom_left.get("brief")
        or ""
    )

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{layout.get("title", "Panel Preview")}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body {{
      margin: 0;
      background: #0f172a;
      color: #e5e7eb;
      font-family: Segoe UI, Arial, sans-serif;
    }}
    .wrap {{ padding: 24px; }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      grid-template-rows: 260px 420px;
      gap: 16px;
    }}
    .card {{
      background: #111827;
      border: 1px solid #334155;
      border-radius: 16px;
      padding: 18px;
      overflow: auto;
    }}
    .score {{
      font-size: 52px;
      font-weight: 700;
      color: #f8fafc;
    }}
    .muted {{ color: #94a3b8; }}
    pre {{
      white-space: pre-wrap;
      font-size: 13px;
      line-height: 1.4;
    }}
    canvas {{ max-height: 180px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>{layout.get("title", "Panel Preview")}</h1>
    <p class="muted">Status: {panel.get("status", "unknown")}</p>

    <div class="grid">
      <div class="card">
        <h2>Top Left — {top_left.get("name", "n/a")}</h2>
        <div class="score">{top_left_value}</div>
        <p>Date: {top_left.get("date", "n/a")}</p>
        <pre>{safe_json(top_left.get("components", top_left))}</pre>
      </div>

      <div class="card">
        <h2>Top Right — {top_right.get("name", "n/a")}</h2>
        <p class="muted">Series points: {len(series)}</p>
        <canvas id="mainChart"></canvas>
      </div>

      <div class="card">
        <h2>Bottom Left — {bottom_left.get("name", "n/a")}</h2>
        <pre>{bottom_text}</pre>
      </div>

      <div class="card">
        <h2>Bottom Right — {bottom_right.get("name", "n/a")}</h2>
        <pre>{safe_json(bottom_right)}</pre>
      </div>
    </div>
  </div>

  <script>
    const labels = {json.dumps(chart_labels)};
    const values = {json.dumps(chart_values)};

    const ctx = document.getElementById("mainChart");

    new Chart(ctx, {{
      type: "line",
      data: {{
        labels: labels,
        datasets: [{{
          label: "{top_right.get("name", "Series")}",
          data: values,
          tension: 0.25,
          borderWidth: 2,
          pointRadius: 0
        }}]
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{
          legend: {{
            labels: {{ color: "#e5e7eb" }}
          }}
        }},
        scales: {{
          x: {{
            ticks: {{ color: "#94a3b8", maxTicksLimit: 8 }},
            grid: {{ color: "rgba(148,163,184,0.15)" }}
          }},
          y: {{
            min: -1,
            max: 1,
            ticks: {{ color: "#94a3b8" }},
            grid: {{ color: "rgba(148,163,184,0.15)" }}
          }}
        }}
      }}
    }});
  </script>
</body>
</html>
"""

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Preview written: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
