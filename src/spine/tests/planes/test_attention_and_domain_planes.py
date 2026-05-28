from pathlib import Path
import json


OC_ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\oraclechambers")
PLANE_ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\planes")


FILES = [
    OC_ROOT / "oc_attention_routing_local.json",
    PLANE_ROOT / "equities_sector_plane.json",
    PLANE_ROOT / "equities_index_plane.json",
    PLANE_ROOT / "rates_plane.json",
    PLANE_ROOT / "fx_plane.json",
]


def test_attention_and_domain_planes():
    failures = []

    for path in FILES:
        if not path.exists():
            failures.append({"file": str(path), "error": "missing"})
            continue

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for key in ["system", "generated_utc", "governance"]:
            if key not in data:
                failures.append({"file": str(path), "missing_key": key})

    print(json.dumps(failures, indent=2))

    assert not failures, f"Tier 2 / 2.25 validation failures: {failures}"
