from pathlib import Path
import json
from datetime import datetime, timezone


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

VISUALS = ROOT / "visuals"
OUT_DIR = ROOT / "offline_render"

OUT_PATH = OUT_DIR / "contradiction_matrix_dashboard.html"


FILES = [
    VISUALS / "contradiction_heatmaps.json",
    ROOT / "planes" / "rates_plane.json",
    ROOT / "planes" / "fx_plane.json",
    ROOT / "planes" / "equities_index_plane.json",
]


def load_json(path: Path):
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []

    for path in FILES:

        data = load_json(path)

        rows.append(f"""
        <tr>
            <td>{path.name}</td>
            <td>{data.get("system", "unknown")}</td>
            <td>{data.get("module", data.get("plane", "unknown"))}</td>
            <td>{data.get("status", "available")}</td>
        </tr>
        """)

    html = f"""
    <html>
    <head>
        <title>Contradiction Matrix Dashboard</title>

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

        th {{
            background: #111821;
        }}

        tr:nth-child(even) {{
            background: #121a24;
        }}

        </style>

    </head>

    <body>

        <h1>Contradiction Matrix Dashboard</h1>

        <p>
            Rendered UTC:
            {datetime.now(timezone.utc).isoformat()}
        </p>

        <table>

            <tr>
                <th>Artifact</th>
                <th>System</th>
                <th>Module</th>
                <th>Status</th>
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
