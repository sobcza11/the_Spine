from pathlib import Path
import json

REPO_ROOT = Path.cwd()

FX_DEPTH_PATH = REPO_ROOT / "data" / "serving" / "fx" / "fx_depth_serving_v1.json"
WTI_DEPTH_PATH = REPO_ROOT / "data" / "serving" / "fx" / "usdcad_wti_inventory_depth.json"

def main():
    if FX_DEPTH_PATH.exists():
        fx_payload = json.loads(FX_DEPTH_PATH.read_text(encoding="utf-8"))
    else:
        fx_payload = {
            "source": "the_Spine | FX DEPTH",
            "pairs": {}
        }
    wti_payload = json.loads(WTI_DEPTH_PATH.read_text(encoding="utf-8"))

    fx_payload.setdefault("pairs", {})

    fx_payload["pairs"]["USD/CAD"] = {
        "metric": "WTI Inv.",
        "source": wti_payload["source"],
        "method": wti_payload["method"],
        "as_of_date": wti_payload["as_of_date"],
        "rows": [
            {
                "date": row["date"],
                "value": row["value"],
                "change": row["inventory_surplus_3yr"],
                "week": row["week"],
                "inventory_mmbbl": row["inventory_mmbbl"],
                "inventory_display": row.get("inventory_display"),
                "inventory_direction": row.get("inventory_direction", "→"),
                "std_3yr": row.get("std_3yr"),
                "std_from_3yr_avg": row.get("std_from_3yr_avg"),
                "avg_3yr": row["avg_3yr"],
                "hist_avg": row["hist_avg"],
                "hist_min": row["hist_min"],
                "hist_max": row["hist_max"]
            }
            for row in wti_payload["rows"]
        ]
    }


    FX_DEPTH_PATH.write_text(json.dumps(fx_payload, indent=2), encoding="utf-8")

    print(f"MERGED INTO: {FX_DEPTH_PATH}")
    print(f"USD/CAD WTI Inv. rows: {len(wti_payload['rows'])}")

if __name__ == "__main__":
    main()

    