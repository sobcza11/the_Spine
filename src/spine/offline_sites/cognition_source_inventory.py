from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine")

OUT_DIR = ROOT / "data" / "review_runtime"
OUT_PATH = OUT_DIR / "cognition_source_inventory.json"


SOURCE_MAP = {
    "rates": {
        "source_files": [
            "data/spine_us/serving/rates/rates_selected_global_panel.json",
            "data/spine_us/serving/rates/rates_curve_data.json",
            "data/spine_us/serving/rates/rates_spread_data.json",
            "data/spine_us/serving/rates/rates_policy_pressure_data.json",
            "data/spine_us/serving/rates/rates_sigma_rank.json",
        ],
        "instruments": ["AU", "CA", "DE", "JP", "UK", "CH", "IT", "US", "CN"],
    },

    "fx": {
        "source_files": [
            "data/fx_price_data.json",
            "data/fx_spreads_data.json",
            "data/fx_sigma_data.json",
            "data/fx_universe.json",
        ],
        "instruments": ["AUDUSD", "EURUSD", "GBPUSD", "USDCAD", "USDCHF", "USDJPY"],
    },

    "equities_index": {
        "source_files": [
            "data/spine_us/serving/equities/us_equity_index_data.json",
            "data/spine_us/serving/equities/equities_sigma_rank.json",
        ],
        "instruments": ["SPY", "QQQ", "DIA", "ITOT", "MDY", "IWM"],
    },

    "equities_sector": {
        "source_files": [
            "data/spine_us/serving/equities/us_sector_etf_data.json",
            "data/spine_us/serving/equities/industry_panel_serving.json",
            "data/spine_us/serving/equities/etf_pmi_breadth_by_etf.json",
            "data/spine_us/serving/equities/equities_sigma_rank.json",
        ],
        "instruments": [
            "XLB", "XLC", "XLE", "XLF", "XLI",
            "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",
        ],
    },
}


def file_status(path_text):
    p = ROOT / path_text
    return {
        "path": path_text,
        "exists": p.exists(),
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    inventory = {}

    for domain, spec in SOURCE_MAP.items():
        inventory[domain] = {
            "instruments": spec["instruments"],
            "source_files": [
                file_status(x)
                for x in spec["source_files"]
            ],
        }

    payload = {
        "system": "IsoVector",
        "module": "cognition-source-inventory",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "inventory": inventory,
        "governance": {
            "source_visibility_required": True,
            "missing_sources_must_be_labeled": True,
            "manual_placeholders_must_be_labeled": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()
