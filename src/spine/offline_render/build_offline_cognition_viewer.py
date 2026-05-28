from pathlib import Path
from datetime import datetime, timezone
import json
import html


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
INDEX_PATH = ROOT / "offline_render" / "offline_render_index.json"
OUT_PATH = ROOT / "offline_render" / "offline_cognition_viewer.html"


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def render_artifact_card(item: dict) -> str:
    file_name = html.escape(str(item.get("file", "")))
    system = html.escape(str(item.get("system", "")))
    module = html.escape(str(item.get("module") or item.get("plane") or ""))
    status = html.escape(str(item.get("status", "")))
    generated = html.escape(str(item.get("generated_utc", "")))
    path = html.escape(str(item.get("path", "")))

    return f"""
    <section class="card">
      <div class="card-top">
        <span class="system">{system}</span>
        <span class="status">{status}</span>
      </div>
      <h2>{file_name}</h2>
      <p class="module">{module}</p>
      <p class="generated">{generated}</p>
      <p class="path">{path}</p>
    </section>
    """


def main():
    data = load_json(INDEX_PATH)
    artifacts = data.get("artifacts", [])

    cards = "\n".join(render_artifact_card(x) for x in artifacts)

    rendered_at = datetime.now(timezone.utc).isoformat()

    doc = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>IsoVector Offline Cognition Viewer</title>
  <style>
    body {{
      margin: 0;
      font-family: Arial, sans-serif;
      background: #0b0f14;
      color: #e6edf3;
    }}
    header {{
      padding: 28px 36px;
      border-bottom: 1px solid #263241;
      background: #111821;
    }}
    h1 {{
      margin: 0 0 8px 0;
      font-size: 26px;
    }}
    .sub {{
      color: #9aa8b5;
      font-size: 14px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 16px;
      padding: 24px 36px 40px;
    }}
    .card {{
      background: #121a24;
      border: 1px solid #263241;
      border-radius: 14px;
      padding: 18px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.25);
    }}
    .card-top {{
      display: flex;
      justify-content: space-between;
      margin-bottom: 12px;
      font-size: 12px;
      color: #9aa8b5;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    h2 {{
      font-size: 18px;
      margin: 0 0 8px 0;
      color: #ffffff;
    }}
    .module {{
      color: #c8d3df;
      margin: 0 0 10px 0;
    }}
    .generated {{
      color: #8ea0b2;
      font-size: 12px;
      margin: 0 0 10px 0;
    }}
    .path {{
      color: #667789;
      font-size: 11px;
      word-break: break-all;
      margin: 0;
    }}
  </style>
</head>
<body>
  <header>
    <h1>IsoVector Offline Cognition Viewer</h1>
    <div class="sub">Artifacts: {len(artifacts)} | Rendered UTC: {html.escape(rendered_at)} | Offline only</div>
  </header>
  <main class="grid">
    {cards}
  </main>
</body>
</html>
"""

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(doc)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
