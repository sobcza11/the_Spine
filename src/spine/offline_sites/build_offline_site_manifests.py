from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

OUT_DIR = ROOT / "data" / "offline_sites"


SITE_MANIFESTS = {
    "equities_industry_manifest.json": {
        "site": "EQUITIES - INDUSTRY",
        "zt_panel": "Z? ? Equity Mrkt ? zeitgeist",
        "rbl_panel": "RBL ? Equity Mrkt ? OC",
        "systemic_panel": "RBL ? Equity Mrkt (Systemic) ? OC",
    },

    "equities_sector_manifest.json": {
        "site": "EQUITIES - SECTOR",
        "zt_panel": "Z? ? Equity - Sector ? zeitgeist",
        "rbl_panel": "RBL ? Equity Sectors ? OC",
        "systemic_panel": None,
    },

    "c_flow_manifest.json": {
        "site": "C_FLOW",
        "zt_panel": "Z? ? Commodity Flow ? zeitgeist",
        "rbl_panel": "RBL ? Commodities ? OC",
        "systemic_panel": "RBL ? Commodities (Systemic) ? OC",
    },

    "fx_manifest.json": {
        "site": "FX",
        "zt_panel": "Z? ? FX zeitgeist",
        "rbl_panel": "RBL ? FX (Systemic) ? OC",
        "systemic_panel": "RBL ? FX (Systemic) ? OC",
    },

    "rates_manifest.json": {
        "site": "RATES",
        "zt_panel": "Z? ? Bond Market ? zeitgeist",
        "rbl_panel": "RBL ? Bond Mrkt (Systemic) ? OC",
        "systemic_panel": "RBL ? Bond Mrkt (Systemic) ? OC",
    },
}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for fname, spec in SITE_MANIFESTS.items():

        payload = {
            "system": "IsoVector",
            "generated_utc": datetime.now(timezone.utc).isoformat(),

            "site": spec["site"],

            "offline_review_mode": True,

            "deployment_target": "isovector.io",

            "governance": {
                "writeback_allowed": False,
                "human_review_required": True,
                "source_payloads_required": True,
            },

            "payloads": {
                "zt_panel": spec["zt_panel"],
                "rbl_panel": spec["rbl_panel"],
                "systemic_panel": spec["systemic_panel"],
            },
        }

        out_path = OUT_DIR / fname

        out_path.write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )

        print(f"Wrote -> {out_path}")


if __name__ == "__main__":
    main()
