from pathlib import Path
import json


RUNTIME_HEALTH = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\macro\serving\runtime_health.json"
)


def test_runtime_health_monitoring():
    assert RUNTIME_HEALTH.exists(), (
        f"Missing runtime health file: {RUNTIME_HEALTH}"
    )

    with open(RUNTIME_HEALTH, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["overall_status"] in [
        "ok",
        "attention_required",
    ]

    for check in data["checks"]:
        assert "path" in check
        assert "status" in check
        assert "modified_utc" in check

    print(json.dumps(data, indent=2))

    