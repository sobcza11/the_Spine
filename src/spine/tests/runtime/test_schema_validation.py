from pathlib import Path
import json
import pandas as pd

from spine.runtime.validation.runtime_health import (
    validate_required_columns,
)

SERVING_ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\macro\serving"
)

FILES = [
    {
        "path": SERVING_ROOT / "ism_pmi_industry_panel.parquet",
        "required_cols": [
            "date",
        ],
    },
]


def test_schema_validation():
    failures = []

    for item in FILES:
        path = item["path"]

        if not path.exists():
            failures.append({
                "path": str(path),
                "error": "missing_file",
            })
            continue

        df = pd.read_parquet(path)

        result = validate_required_columns(
            df=df,
            required_cols=item["required_cols"],
        )

        if result["status"] != "ok":
            failures.append({
                "path": str(path),
                "missing_cols": result["missing_cols"],
            })

    out = {
        "status": "ok" if not failures else "schema_failures",
        "failures": failures,
    }

    print(json.dumps(out, indent=2))

    assert not failures, f"Schema failures detected: {failures}"

