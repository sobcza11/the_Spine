from pathlib import Path
import json

ROOT = Path.cwd()

payload_path = ROOT / "data" / "serving" / "equities" / "industry_panel_serving.json"
output_path = ROOT / "dist" / "offline" / "EQUITIES_INDUSTRY_OC.html"

payload = json.loads(payload_path.read_text(encoding="utf-8"))

html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>IsoVector | EQUITIES INDUSTRY</title>
<style>
body {{
    background-color: #0b1020;
    color: #e5e7eb;
    font-family: Arial;
    margin: 40px;
}}
pre {{
    white-space: pre-wrap;
    background: #111827;
    padding: 20px;
    border-radius: 10px;
}}
</style>
</head>
<body>

<h1>IsoVector | EQUITIES INDUSTRY Cognition Plane</h1>

<script>
const EQUITIES_INDUSTRY_PAYLOAD = {json.dumps(payload)};
</script>

<pre id="payload"></pre>

<script>
document.getElementById("payload").textContent =
JSON.stringify(EQUITIES_INDUSTRY_PAYLOAD, null, 2);
</script>

</body>
</html>
"""

output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(html, encoding="utf-8")

print(f"[OK] EQUITIES_INDUSTRY standalone built -> {output_path}")
