from pathlib import Path
import json


ROOTS = [
    Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\planes"),
    Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\geoscen"),
    Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\visuals"),
]

REQUIRED_KEYS = [
    "system",
    "generated_utc",
]


def test_geoscen_and_visual_layers():

    failures = []

    for root in ROOTS:

        for path in root.glob("*.json"):

            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            for key in REQUIRED_KEYS:

                if key not in data:
                    failures.append({
                        "file": str(path),
                        "missing_key": key,
                    })

    print(json.dumps(failures, indent=2))

    assert not failures, (
        f"GeoScen/Visual validation failures: {failures}"
    )
