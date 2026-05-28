from pathlib import Path
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "offline_render"

RUNTIME_HEALTH = ROOT / "macro" / "serving" / "runtime_health.json"

OUT_PATH = OUT_DIR / "runtime_health_dashboard.html"


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if RUNTIME_HEALTH.exists():

        with open(RUNTIME_HEALTH, "r", encoding="utf-8") as f:
            data = json.load(f)

    else:
        data = {}

    checks = data.get("checks", [])

    rows = []

    for item in checks:

        rows.append(f"""
        <tr>
            <td>{item.get("path")}</td>
            <td>{item.get("status")}</td>
            <td>{item.get("age_hours")}</td>
        </tr>
        """)

    html = f"""
    <html>

    <head>

    <title>Runtime Health Dashboard</title>

    <style>

    body {{
        background: #0b0f14;
        color: white;
        font-family: Arial;
        padding: 40px;
    }}

    table {{
        width: 100%;
        border-collapse: collapse;
    }}

    td, th {{
        border: 1px solid #263241;
        padding: 12px;
    }}

    </style>

    </head>

    <body>

        <h1>Runtime Health Dashboard</h1>

        <table>

            <tr>
                <th>Path</th>
                <th>Status</th>
                <th>Age Hours</th>
            </tr>

            {''.join(rows)}

        </table>

    </body>

    </html>
    """

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
