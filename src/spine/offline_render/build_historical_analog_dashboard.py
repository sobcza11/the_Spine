from pathlib import Path
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "offline_render"

OUT_PATH = OUT_DIR / "historical_analog_dashboard.html"

FILE = ROOT / "visuals" / "historical_analog_overlays.json"


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if FILE.exists():

        with open(FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

    else:
        data = {}

    html = f"""
    <html>

    <head>

    <title>Historical Analog Dashboard</title>

    <style>

    body {{
        background: #0b0f14;
        color: white;
        font-family: Arial;
        padding: 40px;
    }}

    pre {{
        background: #121a24;
        padding: 20px;
        border-radius: 12px;
        overflow-x: auto;
    }}

    </style>

    </head>

    <body>

        <h1>Historical Analog Dashboard</h1>

        <pre>
{json.dumps(data, indent=2)}
        </pre>

    </body>

    </html>
    """

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
