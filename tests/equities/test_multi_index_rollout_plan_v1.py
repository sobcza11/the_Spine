import json
from pathlib import Path


ROOT = Path(__file__).parents[2]
PLAN = (
    ROOT
    / "governance/equities/multi_index_rollout/EQUITIES_MULTI_INDEX_ROLLOUT_PLAN_V1.md"
)
MATRIX = (
    ROOT
    / "governance/equities/multi_index_rollout/EQUITIES_MULTI_INDEX_DEPENDENCY_MATRIX_V1.md"
)
ORDER = ["SPY", "QQQ", "DIA", "IWM", "MDY", "ITOT"]


def _load(path: Path, marker: str):
    text = path.read_text(encoding="utf-8")
    section = text.split(marker, 1)[1]
    return json.loads(section.split("```json", 1)[1].split("```", 1)[0])


def test_deterministic_index_and_phase_ordering():
    plan = _load(PLAN, "<!-- MULTI_INDEX_ROLLOUT_JSON_V1 -->")
    matrix = _load(MATRIX, "<!-- MULTI_INDEX_MATRIX_JSON_V1 -->")
    assert plan["index_order"] == ORDER
    assert [item["instrument_id"] for item in plan["indexes"]] == ORDER
    assert [row["index"] for row in matrix["rows"]] == ORDER
    assert [phase["phase"] for phase in plan["phases"]] == list(range(1, 8))


def test_no_authorization_is_invented():
    plan = _load(PLAN, "<!-- MULTI_INDEX_ROLLOUT_JSON_V1 -->")
    by_index = {item["instrument_id"]: item for item in plan["indexes"]}
    assert by_index["QQQ"]["authorization_status"] == "AUTHORIZED"
    assert by_index["QQQ"]["authorization_reference"] == (
        "QQQ_TIINGO_DAILY_EOD_AUTHORIZATION_V1"
    )
    for instrument in {"SPY", "DIA", "IWM", "MDY", "ITOT"}:
        assert by_index[instrument]["authorization_status"] == "AUTHORIZATION_REQUIRED"
        assert by_index[instrument]["authorization_reference"] is None


def test_no_activation_is_authorized():
    plan = _load(PLAN, "<!-- MULTI_INDEX_ROLLOUT_JSON_V1 -->")
    matrix = _load(MATRIX, "<!-- MULTI_INDEX_MATRIX_JSON_V1 -->")
    assert plan["runtime_state"] == matrix["runtime_state"] == "VALIDATION_ONLY"
    assert plan["activation_state"] == matrix["activation_state"] == "PROHIBITED"


def test_no_sys_dependency():
    for path in (PLAN, MATRIX):
        text = path.read_text(encoding="utf-8")
        assert "spine.sys" not in text
        assert "src.spine.sys" not in text


def test_qqq_is_reference_and_other_rollouts_are_pending():
    plan = _load(PLAN, "<!-- MULTI_INDEX_ROLLOUT_JSON_V1 -->")
    assert plan["phases"][0] == {
        "phase": 1,
        "name": "QQQ_VALIDATION_REFERENCE",
        "status": "COMPLETE",
    }
    assert all(phase["status"] == "PENDING" for phase in plan["phases"][1:])
