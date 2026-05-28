from pathlib import Path


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\review_runtime"
)


DOMAINS = [
    "equities_industry",
    "equities_sector",
    "c_flow",
    "fx",
    "rates",
]


def test_review_runtime_structure():

    assert ROOT.exists()

    for d in DOMAINS:

        base = ROOT / d

        assert (base / "zt").exists()
        assert (base / "rbl").exists()
        assert (base / "systemic").exists()

        assert (base / "graphs").exists()
        assert (base / "overlays").exists()

        assert (base / "snapshots").exists()
        assert (base / "review").exists()


def test_review_governance_files():

    assert (
        ROOT / "review_objectives.json"
    ).exists()

    assert (
        ROOT / "review_snapshot_policy.json"
    ).exists()

    assert (
        ROOT / "rates" / "zt" / "review_seed.json"
    ).exists()
