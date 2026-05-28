from pathlib import Path
from datetime import datetime, timezone
import json
import html


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_DIR = ROOT / "offline_render"
OUT_PATH = OUT_DIR / "rbl_cognitive_dashboard.html"


INPUTS = {
    "rbl": ROOT / "oraclechambers" / "oc_rbl_local.json",
    "final_metric": ROOT / "oraclechambers" / "oc_final_metric_local.json",
    "contradiction": ROOT / "oraclechambers" / "oc_contradiction_local.json",
    "attention": ROOT / "oraclechambers" / "oc_attention_routing_local.json",
    "equities": ROOT / "planes" / "equities_index_plane.json",
    "rates": ROOT / "planes" / "rates_plane.json",
    "fx": ROOT / "planes" / "fx_plane.json",
    "geoscen": ROOT / "geoscen" / "sovereign_vector_engine.json",
}


def load(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def signal_lines(payload: dict) -> list[str]:
    lines = []
    for item in payload.get("signals", []):
        signal = item.get("signal", "unknown")
        state = item.get("state", "unknown")
        score = item.get("score", "n/a")
        lines.append(f"{signal}: {state} ({score})")
    return lines


def build_rbl(data: dict) -> dict:
    equities = data["equities"]
    rates = data["rates"]
    fx = data["fx"]
    contradiction = data["contradiction"]
    attention = data["attention"]
    final_metric = data["final_metric"]
    geoscen = data["geoscen"]

    final_score = (
        final_metric.get("scores", {}).get("final_score")
        or final_metric.get("final_score")
        or "n/a"
    )

    max_severity = contradiction.get("max_severity", "n/a")
    top_priority = attention.get("top_priority", {})

    return {
        "headline": "System posture is operational but confirmation remains incomplete.",
        "read_between_the_lines": [
            "The platform is now surfacing cognition rather than only listing artifacts.",
            "Equities show constructive/risk-appetite behavior, but rates & FX still require confirmation.",
            "The contradiction layer is the key watch item: if risk assets strengthen while liquidity or policy pressure remains tight, regime confidence should not be overstated.",
            "GeoScen is present as a sovereign cognition layer, but it still needs real country scoring before it can support institutional-grade conclusions.",
            "The current RBL should be treated as governed synthesis: useful for interpretation, not yet autonomous inference.",
        ],
        "executive_attention": [
            f"Top priority: {top_priority.get('area', 'unknown')} ? {top_priority.get('reason', 'No reason available')}",
            f"Final deployability / confidence score: {final_score}",
            f"Max contradiction severity: {max_severity}",
        ],
        "domain_readout": {
            "equities": signal_lines(equities),
            "rates": signal_lines(rates),
            "fx": signal_lines(fx),
            "geoscen": [
                f"{x.get('country')}: pressure={x.get('vector', {}).get('pressure')}, fragility={x.get('vector', {}).get('fragility')}, policy_divergence={x.get('vector', {}).get('policy_divergence')}"
                for x in geoscen.get("vectors", [])
            ],
        },
        "limitations": [
            "This is not yet live LLM inference.",
            "This is not yet real RAG-grounded synthesis.",
            "Current cognition is rule/template-driven from governed payloads.",
            "Next upgrade should connect this RBL layer to controlled RAG + Langroid read-only agents.",
        ],
    }


def render_list(items):
    return "".join(f"<li>{html.escape(str(x))}</li>" for x in items)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    data = {k: load(v) for k, v in INPUTS.items()}
    rbl = build_rbl(data)

    rendered_at = datetime.now(timezone.utc).isoformat()

    domain_sections = ""

    for domain, lines in rbl["domain_readout"].items():
        domain_sections += f"""
        <section class="card">
          <h2>{html.escape(domain.upper())}</h2>
          <ul>{render_list(lines)}</ul>
        </section>
        """

    doc = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>OracleChambers RBL Cognitive Dashboard</title>
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
.warning {{
  color: #f0c36a;
}}
.grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
  gap: 18px;
}}
</style>
</head>
<body>
<header>
  <h1>OracleChambers RBL Cognitive Dashboard</h1>
  <div class="sub">Offline RBL Cognition | Rendered UTC: {html.escape(rendered_at)}</div>
</header>

<main>
  <section class="card">
    <div class="headline">{html.escape(rbl["headline"])}</div>
    <h2>Read Between the Lines</h2>
    <ul>{render_list(rbl["read_between_the_lines"])}</ul>
  </section>

  <section class="card">
    <h2>Executive Attention</h2>
    <ul>{render_list(rbl["executive_attention"])}</ul>
  </section>

  <div class="grid">
    {domain_sections}
  </div>

  <section class="card">
    <h2 class="warning">Limitations / Governance</h2>
    <ul>{render_list(rbl["limitations"])}</ul>
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
