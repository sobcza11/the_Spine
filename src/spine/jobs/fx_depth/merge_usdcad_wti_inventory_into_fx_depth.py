from pathlib import Path
import json

REPO_ROOT = Path.cwd()

FX_DEPTH_PATH = REPO_ROOT / "data" / "serving" / "fx" / "fx_depth_serving_v1.json"
WTI_DEPTH_PATH = REPO_ROOT / "data" / "serving" / "fx" / "usdcad_wti_inventory_depth.json"

def main():
    fx_payload = json.loads(FX_DEPTH_PATH.read_text(encoding="utf-8"))
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

    