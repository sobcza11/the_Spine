from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\offline_sites"
)


FILES = [
    "equities_industry_manifest.json",
    "equities_sector_manifest.json",
    "c_flow_manifest.json",
    "fx_manifest.json",
    "rates_manifest.json",
]


def test_offline_site_manifests():

    for f in FILES:

        p = ROOT / f

        assert p.exists()

        d = json.loads(
            p.read_text(encoding="utf-8")
        )

        assert d["offline_review_mode"] is True
        assert d["deployment_target"] == "isovector.io"

        assert d["governance"]["writeback_allowed"] is False
        assert d["governance"]["human_review_required"] is True

        assert d["payloads"]["zt_panel"] is not None
        assert d["payloads"]["rbl_panel"] is not None
