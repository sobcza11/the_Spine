from pathlib import Path
import json

from oc_cflow_zt_zeitgeist_v1 import build_cflow_zt_zeitgeist_v1
from oc_cflow_rbl_interpretation_v1 import build_cflow_rbl_interpretation_v1


EXPORT_DIR = (
    Path(__file__).resolve().parents[6]
    / "_offline_site"
    / "oc_ai_components"
    / "c_flow"
    / "payloads"
)

EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    payload = {
        "zt": build_cflow_zt_zeitgeist_v1(),
        "rbl": build_cflow_rbl_interpretation_v1(),
    }

    outpath = EXPORT_DIR / "cflow_ai_components_v1.json"

    outpath.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(
        {
            "artifact": "export_cflow_ai_components_v1",
            "exported": True,
            "path": str(outpath),
        }
    )


if __name__ == "__main__":
    main()

    