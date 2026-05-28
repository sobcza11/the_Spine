from pathlib import Path
import urllib.request

ROOT = Path.cwd()
BASE = "https://pub-73703eeb21994303b8b98f8cbcf6dbca.r2.dev"

objects = [
    "spine_us/serving/equities/industry_panel_serving.json",
    "spine_us/serving/equities/etf_pmi_breadth_by_etf.json",
    "spine_us/serving/equities/etf_pmi_composite.json",
    "spine_us/serving/equities/us_sector_etf_data.json",
    "spine_us/serving/equities/us_equity_index_data.json",
    "spine_us/serving/equities/equities_sigma_rank.json",
    "spine_us/serving/ism_pmi_latest.json",
]

for obj in objects:
    url = f"{BASE}/{obj}"
    local_path = ROOT / "_r2_restore" / obj
    local_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nGET {url}")
    print(f" -> {local_path}")

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req) as response:
            local_path.write_bytes(response.read())

        print(f"[OK] {local_path.stat().st_size} bytes")
    except Exception as exc:
        print(f"[FAIL] {exc}")

        