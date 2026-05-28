from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine"
)

REVIEW_ROOT = (
    ROOT
    / "data"
    / "review_runtime"
)


def load_payload(path):

    if not path.exists():
        return None

    return json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )


def render_block(title, lines):

    if not lines:
        return ""

    html = f"<h3>{title}</h3>"

    for line in lines:
        html += f"<p>{line}</p>"

    return html


def render_reserved_space(state):

    return f'''
    <h3>Reserved Space Decision</h3>
    <p>{state}</p>
    '''


def render_payload(name, payload):

    html = f'''
    <div class="panel">
        <h2>{name}</h2>
    '''

    html += render_block(
        "Zₜ Output",
        payload.get("zt", []),
    )

    html += render_block(
        "RBL Output",
        payload.get("rbl", []),
    )

    html += render_block(
        "RBL - Systemic",
        payload.get(
            "rbl_systemic",
            [],
        ),
    )

    html += render_block(
        "IV[t] State",
        payload.get(
            "iv_state",
            [],
        ),
    )

    html += render_reserved_space(
        payload.get(
            "reserved_space_decision",
            "-",
        )
    )

    html += "</div>"

    return html


def main():

    sections = [
        (
            "United States",
            REVIEW_ROOT
            / "rates"
            / "US"
            / "content.json",
        ),

        (
            "AUD/USD",
            REVIEW_ROOT
            / "fx"
            / "AUDUSD"
            / "content.json",
        ),
    ]

    body = ""

    for label, path in sections:

        payload = load_payload(path)

        if payload:
            body += render_payload(
                label,
                payload,
            )

    html = f'''
    <html>
    <head>
        <title>IsoVector Offline Runtime</title>

        <style>

        body {{
            font-family: Arial;
            background: #111;
            color: #eee;
            padding: 30px;
        }}

        .panel {{
            border: 1px solid #333;
            margin-bottom: 25px;
            padding: 20px;
            border-radius: 10px;
        }}

        h2 {{
            color: #7cc4ff;
        }}

        h3 {{
            margin-top: 20px;
            color: #ffd166;
        }}

        p {{
            line-height: 1.5;
        }}

        </style>

    </head>

    <body>

    <h1>IsoVector Offline Cognition Runtime</h1>

    {body}

    </body>
    </html>
    '''

    out = REVIEW_ROOT / "index.html"

    out.write_text(
        html,
        encoding="utf-8",
    )

    print(
        f"Rendered -> {out}"
    )


if __name__ == "__main__":
    main()
    