from pathlib import Path
import json

ROOT = Path.cwd()

payload_path = ROOT / "data" / "serving" / "c_flow" / "c_flow_latest_v5.json"

output_path = ROOT / "dist" / "offline" / "C_FLOW_OC.html"

payload = json.loads(
    payload_path.read_text(encoding="utf-8")
)

html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>IsoVector | C_FLOW</title>

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

<h1>IsoVector | C_FLOW Cognition Plane</h1>

<script>
const CFLOW_PAYLOAD = {json.dumps(payload)};
</script>

<pre id="payload"></pre>

<script>
document.getElementById("payload").textContent =
JSON.stringify(CFLOW_PAYLOAD, null, 2);
</script>

</body>
</html>
"""

output_path.parent.mkdir(parents=True, exist_ok=True)

output_path.write_text(
    html,
    encoding="utf-8"
)

print(f"[OK] C_FLOW standalone built -> {output_path}")