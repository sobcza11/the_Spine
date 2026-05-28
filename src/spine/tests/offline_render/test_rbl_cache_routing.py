from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data"
)

OUT_PATH = (
    ROOT
    / "rbl_agent"
    / "langroid_rbl_agent_output.json"
)

CACHE_PATH = (
    ROOT
    / "rbl_agent"
    / "last_good_rbl_agent_output.json"
)


def test_rbl_cache_routing():

    assert OUT_PATH.exists()

    data = json.loads(
        OUT_PATH.read_text(
            encoding="utf-8"
        )
    )

    assert data["governance"]["read_only"] is True

    assert data["agent_mode"] in [
        "live_llm_read_only_synthesis",
        "cached_llm_synthesis",
        "fallback_rule_synthesis",
    ]

    if CACHE_PATH.exists():

        cache = json.loads(
            CACHE_PATH.read_text(
                encoding="utf-8"
            )
        )

        assert (
            cache["governance"]["read_only"]
            is True
        )
