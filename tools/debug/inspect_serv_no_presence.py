from pathlib import Path
import json
import pandas as pd

ROOT = Path.cwd()

targets = [
    ROOT / "data" / "serving" / "equities" / "industry_panel_serving.json",
    ROOT / "data" / "serving" / "equities" / "etf_no_composite.json",
    ROOT / "data" / "macro" / "serving" / "ism_pmi_latest.json",
]

for path in targets:

    print("\n====================================================")
    print(path)
    print("====================================================")

    if not path.exists():
        print("[MISSING]")
        continue

    if path.suffix == ".json":

        data = json.loads(path.read_text(encoding="utf-8"))

        if isinstance(data, list) and len(data) > 0:

            sample = data[0]

            print("\nKEYS:\n")
            print(sample.keys())

            keys = [k.lower() for k in sample.keys()]

            no_keys = [
                k for k in keys
                if "no" in k
            ]

            print("\nNO-LIKE FIELDS:\n")
            print(no_keys)

            print("\nSAMPLE:\n")
            print(sample)

            