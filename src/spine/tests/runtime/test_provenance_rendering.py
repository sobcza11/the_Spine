from pathlib import Path
import json


PROVENANCE_FILE = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\macro\serving\runtime_health.json"
)


def test_provenance_rendering():
    assert PROVENANCE_FILE.exists(), (
        f"Missing provenance file: {PROVENANCE_FILE}"
    )

    with open(PROVENANCE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert "generated_utc" in data
    assert "checks" in data

    assert isinstance(data["checks"], list)

    print(json.dumps(data, indent=2))

    