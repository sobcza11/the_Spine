from pathlib import Path

ROOT = Path.cwd()

output_path = ROOT / "dist" / "offline" / "index.html"

planes = [
    ("FX", "FX_OC.html"),
    ("RATES", "RATES_OC.html"),
    ("C_FLOW", "C_FLOW_OC.html"),
    ("EQUITIES_SECTOR", "EQUITIES_SECTOR_OC.html"),
    ("EQUITIES_INDUSTRY", "EQUITIES_INDUSTRY_OC.html"),
]

cards_html = ""

for plane_name, file_name in planes:
    cards_html += f"""
    <a class="card" href="{file_name}">
        <h2>{plane_name}</h2>
        <p>Launch standalone cognition plane</p>
    </a>
    """

html = f"""
<!DOCTYPE html>
<html lang="en">

<head>
<meta charset="UTF-8">

<title>IsoVector | Offline Cognition Suite</title>

<style>

body {{
    background-color: #0b1020;
    color: #e5e7eb;
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 40px;
}}

h1 {{
    margin-bottom: 10px;
}}

.subtitle {{
    color: #9ca3af;
    margin-bottom: 40px;
}}

.grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: 20px;
}}

.card {{
    background: #111827;
    border-radius: 14px;
    padding: 24px;
    text-decoration: none;
    color: #e5e7eb;
    transition: 0.2s ease;
    border: 1px solid #1f2937;
}}

.card:hover {{
    transform: translateY(-3px);
    border-color: #60a5fa;
}}

.card h2 {{
    margin-top: 0;
}}

.footer {{
    margin-top: 50px;
    color: #6b7280;
    font-size: 14px;
}}

</style>

</head>

<body>

<h1>IsoVector Offline Cognition Suite</h1>

<div class="subtitle">
Governed institutional cognition runtime
</div>

<div class="grid">

{cards_html}

</div>

<div class="footer">
Standalone deployment • No server dependency • Offline institutional runtime
</div>

</body>
</html>
"""

output_path.parent.mkdir(parents=True, exist_ok=True)

output_path.write_text(
    html,
    encoding="utf-8"
)

print(f"[OK] Offline index built -> {output_path}")

