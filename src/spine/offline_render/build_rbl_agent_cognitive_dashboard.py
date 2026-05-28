from pathlib import Path
from datetime import datetime, timezone
import json
import html


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "offline_render"
OUT_PATH = OUT_DIR / "rbl_agent_cognitive_dashboard.html"

AGENT_OUTPUT = ROOT / "rbl_agent" / "langroid_rbl_agent_output.json"
CONTRACT = ROOT / "rbl_agent" / "rbl_agent_input_contract.json"
RECORD = ROOT / "rbl_agent" / "rbl_agent_saved_output_record.json"


def load(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def render_list(items):
    return "".join(f"<li>{html.escape(str(x))}</li>" for x in items)


def render_dict(d: dict):
    rows = []
    for k, v in d.items():
        rows.append(f"<li><strong>{html.escape(str(k))}</strong>: {html.escape(str(v))}</li>")
    return "".join(rows)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    agent = load(AGENT_OUTPUT)
    contract = load(CONTRACT)
    record = load(RECORD)

    rendered_at = datetime.now(timezone.utc).isoformat()

    doc = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>RBL Agent Cognitive Dashboard</title>
<style>
body {{
  margin: 0;
  background: #0b0f14;
  color: #e6edf3;
  font-family: Arial, sans-serif;
}}
header {{
  padding: 30px 42px;
  background: #111821;
  border-bottom: 1px solid #263241;
}}
h1 {{ margin: 0; font-size: 30px; }}
.sub {{ color: #91a3b5; margin-top: 8px; }}
main {{ padding: 30px 42px; }}
.card {{
  background: #121a24;
  border: 1px solid #263241;
  border-radius: 16px;
  padding: 22px;
  margin-bottom: 18px;
}}
h2 {{ margin-top: 0; }}
li {{ margin-bottom: 8px; line-height: 1.45; }}
.headline {{
  font-size: 22px;
  color: #ffffff;
  margin-bottom: 18px;
}}
.grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
  gap: 18px;
}}
.warning {{ color: #f0c36a; }}
</style>
</head>
<body>
<header>
  <h1>RBL Agent Cognitive Dashboard</h1>
  <div class="sub">Controlled RAG + Langroid-ready Read-only Synthesis | Rendered UTC: {html.escape(rendered_at)}</div>
</header>

<main>
  <section class="card">
    <div class="headline">{html.escape(str(agent.get("headline", "")))}</div>
    <h2>Agent RBL Synthesis</h2>
    <ul>{render_list(agent.get("synthesis", []))}</ul>
  </section>

  <section class="card">
    <h2>Executive Attention</h2>
    <ul>{render_list(agent.get("executive_attention", []))}</ul>
  </section>

  <div class="grid">
    <section class="card">
      <h2>Source Payloads</h2>
      <ul>{render_list(agent.get("source_payloads", []))}</ul>
    </section>

    <section class="card">
      <h2>Blocked Actions</h2>
      <ul>{render_list(contract.get("blocked_actions", []))}</ul>
    </section>
  </div>

  <section class="card">
    <h2>Agent Governance</h2>
    <ul>{render_dict(agent.get("governance", {}))}</ul>
  </section>

  <section class="card">
    <h2 class="warning">Saved Agent Record</h2>
    <ul>{render_dict(record.get("summary", {}))}</ul>
  </section>
</main>
</body>
</html>
"""

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(doc)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
