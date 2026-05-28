from pathlib import Path
import json

from oc_equities_sector_zt_zeitgeist_v1 import (
    build_equities_sector_zt_zeitgeist_v1,
)
from oc_equities_sector_rbl_interpretation_v1 import (
    build_equities_sector_rbl_interpretation_v1,
)


EXPORT_DIR = (
    Path(__file__).resolve().parents[6]
    / "_offline_site"
    / "oc_ai_components"
    / "equities_sector"
    / "payloads"
)

EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    payload = {
        "zt": build_equities_sector_zt_zeitgeist_v1(),
        "rbl": build_equities_sector_rbl_interpretation_v1(),
    }

    outpath = EXPORT_DIR / "equities_sector_ai_components_v1.json"

    outpath.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(
        {
            "artifact": "export_equities_sector_ai_components_v1",
            "exported": True,
            "path": str(outpath),
        }
    )


if __name__ == "__main__":
    main()
    