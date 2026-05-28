from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier5\disaster_recovery_runtime.json"
)

def test_disaster_recovery_runtime():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "disaster-recovery-runtime"

    assert d["disaster_recovery_enabled"] is True

    assert d["recovery_point_count"] > 0

    assert len(d["recovery_points"]) > 0

    assert d["governance"]["rollback_supported"] is True
