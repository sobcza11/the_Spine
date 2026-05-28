from pathlib import Path


AUDIT_ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\audit"
)


def test_historical_audit_continuity():
    assert AUDIT_ROOT.exists(), (
        f"Missing audit root: {AUDIT_ROOT}"
    )

    files = sorted(AUDIT_ROOT.glob("**/*"))

    files = [x for x in files if x.is_file()]

    assert len(files) > 0, (
        "No historical audit files found."
    )

    latest = max(files, key=lambda x: x.stat().st_mtime)

    print(f"Audit files found: {len(files)}")
    print(f"Latest audit file: {latest}")

    