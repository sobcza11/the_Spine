from pathlib import Path
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "offline_render"

FILES = [
    ROOT / "geoscen" / "sovereign_canonical_layer.json",
    ROOT / "geoscen" / "sovereign_vector_engine.json",
    ROOT / "geoscen" / "regional_transmission_systems.json",
]

OUT_PATH = OUT_DIR / "geoscen_sovereign_dashboard.html"


def load_json(path: Path):

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cards = []

    for path in FILES:

        data = load_json(path)

        cards.append(f"""
        <div class="card">

            <h2>{path.name}</h2>

            <p>
                <strong>System:</strong>
                {data.get("system")}
            </p>

            <p>
                <strong>Module:</strong>
                {data.get("module")}
            </p>

            <p>
                <strong>Status:</strong>
                {data.get("status", "available")}
            </p>

        </div>
        """)

    html = f"""
    <html>

    <head>

    <title>GeoScen Sovereign Dashboard</title>

    <style>

    body {{
        background: #0b0f14;
        color: white;
        font-family: Arial;
        padding: 40px;
    }}

    .grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
        gap: 18px;
    }}

    .card {{
        background: #121a24;
        border: 1px solid #263241;
        border-radius: 14px;
        padding: 20px;
    }}

    </style>

    </head>

    <body>

        <h1>GeoScen Sovereign Dashboard</h1>

        <div class="grid">
            {''.join(cards)}
        </div>

    </body>

    </html>
    """

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
