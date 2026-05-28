from pathlib import Path
import json

ROOT = Path.cwd()

payload_path = ROOT / "data" / "serving" / "equities" / "us_sector_etf_data.json"
output_path = ROOT / "dist" / "offline" / "EQUITIES_SECTOR_OC.html"

payload = json.loads(payload_path.read_text(encoding="utf-8"))

html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>IsoVector | EQUITIES SECTOR</title>
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

<h1>IsoVector | EQUITIES SECTOR Cognition Plane</h1>

<script>
const EQUITIES_SECTOR_PAYLOAD = {json.dumps(payload)};
</script>

<pre id="payload"></pre>

<script>
document.getElementById("payload").textContent =
JSON.stringify(EQUITIES_SECTOR_PAYLOAD, null, 2);
</script>

</body>
</html>
"""

output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(html, encoding="utf-8")

print(f"[OK] EQUITIES_SECTOR standalone built -> {output_path}")

