from pathlib import Path
from datetime import datetime, timezone
import json
import html


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "offline_render"
OUT_PATH = OUT_DIR / "offline_executive_dashboard.html"


FILES = [
    ROOT / "oraclechambers" / "oc_rbl_local.json",
    ROOT / "oraclechambers" / "oc_final_metric_local.json",
    ROOT / "oraclechambers" / "oc_contradiction_local.json",
    ROOT / "oraclechambers" / "oc_attention_routing_local.json",
    ROOT / "final_batch" / "executive_cognition_runtime.json",
]


def load_json(path: Path) -> dict:

    if not path.exists():
        return {
            "module": path.name,
            "status": "missing",
        }

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def render_card(data: dict) -> str:

    module = html.escape(str(data.get("module", "unknown")))
    system = html.escape(str(data.get("system", "unknown")))
    status = html.escape(str(data.get("status", "available")))
    generated = html.escape(str(data.get("generated_utc", "")))

    capability = html.escape(
        str(
            data.get("capability")
            or data.get("plane")
            or "institutional cognition"
        )
    )

    governance = data.get("governance", {})

    governance_lines = []

    for k, v in governance.items():
        governance_lines.append(
            f"<li><strong>{html.escape(str(k))}</strong>: {html.escape(str(v))}</li>"
        )

    governance_html = "".join(governance_lines)

    return f"""
    <section class="card">
        <div class="topbar">
            <span class="system">{system}</span>
            <span class="status">{status}</span>
        </div>

        <h2>{module}</h2>

        <p class="capability">{capability}</p>

        <p class="generated">
            Generated UTC: {generated}
        </p>

        <div class="gov">
            <h3>Governance</h3>
            <ul>
                {governance_html}
            </ul>
        </div>
    </section>
    """


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cards = []

    for path in FILES:
        data = load_json(path)
        cards.append(render_card(data))

    rendered_at = datetime.now(timezone.utc).isoformat()

    doc = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">

<title>IsoVector Executive Dashboard</title>

<style>

body {{
    margin: 0;
    background: #0b0f14;
    color: #e6edf3;
    font-family: Arial, sans-serif;
}}

header {{
    background: #111821;
    border-bottom: 1px solid #263241;
    padding: 28px 40px;
}}

h1 {{
    margin: 0;
    font-size: 30px;
}}

.sub {{
    margin-top: 8px;
    color: #91a3b5;
}}

.grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
    gap: 18px;
    padding: 30px 40px;
}}

.card {{
    background: #121a24;
    border: 1px solid #263241;
    border-radius: 16px;
    padding: 22px;
    box-shadow: 0 8px 30px rgba(0,0,0,0.25);
}}

.topbar {{
    display: flex;
    justify-content: space-between;
    margin-bottom: 16px;
    color: #9aa8b5;
    font-size: 12px;
    text-transform: uppercase;
}}

.status {{
    color: #67d38f;
}}

h2 {{
    margin: 0 0 10px 0;
    color: #ffffff;
}}

.capability {{
    color: #c8d3df;
    margin-bottom: 14px;
}}

.generated {{
    color: #7f91a4;
    font-size: 12px;
}}

.gov {{
    margin-top: 18px;
    background: #0f151d;
    border-radius: 10px;
    padding: 14px;
}}

.gov h3 {{
    margin-top: 0;
    color: #ffffff;
    font-size: 14px;
}}

.gov ul {{
    margin: 0;
    padding-left: 18px;
}}

.gov li {{
    margin-bottom: 6px;
    color: #9db0c3;
}}

footer {{
    padding: 20px 40px 40px;
    color: #728396;
    font-size: 12px;
}}

</style>
</head>

<body>

<header>
    <h1>IsoVector Executive Dashboard</h1>

    <div class="sub">
        Offline Institutional Cognition Rendering
        |
        Rendered UTC: {html.escape(rendered_at)}
    </div>
</header>

<main class="grid">
    {''.join(cards)}
</main>

<footer>
    Offline Only ? Governed Runtime ? Human Review Required
</footer>

</body>
</html>
"""

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(doc)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
