from pathlib import Path
import json
import pandas as pd

ROOT = Path.cwd()

failures = []

print("\n[serv_no Inspection Test]\n")

# ------------------------------------------------------------
# Test 1 — Confirm target inspection scripts exist
# ------------------------------------------------------------

scripts = [
    ROOT / "tools" / "debug" / "inspect_nonmanu_no.py",
    ROOT / "tools" / "debug" / "inspect_serv_no_presence.py",
]

for script in scripts:
    if script.exists():
        print(f"[OK] Found script -> {script.relative_to(ROOT)}")
    else:
        failures.append(f"Missing script: {script.relative_to(ROOT)}")

# ------------------------------------------------------------
# Test 2 — Confirm likely local nonmanu/services NO candidates
# ------------------------------------------------------------

candidate_parquets = [
    ROOT / "data" / "ism" / "us_ism_nonmanu_no_by_industry_canonical.parquet",
    ROOT / "data" / "spine_us" / "us_ism_nonmanu_no_by_industry_canonical.parquet",
    ROOT / "data" / "macro" / "serving" / "us_ism_nonmanu_no_by_industry_canonical.parquet",
]

found_parquets = [p for p in candidate_parquets if p.exists()]

if found_parquets:
    for p in found_parquets:
        df = pd.read_parquet(p)
        print(f"[OK] Read parquet -> {p.relative_to(ROOT)} | shape={df.shape}")
        print(f"     columns={df.columns.tolist()}")

        cols = {c.lower() for c in df.columns}
        if not any("date" == c or "date" in c for c in cols):
            failures.append(f"No date-like column in {p.relative_to(ROOT)}")
        if not any("industry" in c for c in cols):
            failures.append(f"No industry-like column in {p.relative_to(ROOT)}")
else:
    failures.append("No local nonmanu/services NO parquet candidate found")

# ------------------------------------------------------------
# Test 3 — Confirm serving artifacts contain NO-like fields
# ------------------------------------------------------------

candidate_jsons = [
    ROOT / "data" / "serving" / "equities" / "industry_panel_serving.json",
    ROOT / "data" / "serving" / "equities" / "etf_no_composite.json",
    ROOT / "data" / "macro" / "serving" / "ism_pmi_latest.json",
]

for path in candidate_jsons:
    if not path.exists():
        print(f"[WARN] Missing JSON candidate -> {path.relative_to(ROOT)}")
        continue

    data = json.loads(path.read_text(encoding="utf-8"))

    sample = data[0] if isinstance(data, list) and data else data

    if isinstance(sample, dict):
        keys = list(sample.keys())
        no_like = [k for k in keys if "no" in k.lower() or "order" in k.lower()]

        print(f"[OK] Read JSON -> {path.relative_to(ROOT)}")
        print(f"     NO-like fields={no_like}")

        if not no_like:
            failures.append(f"No NO/order-like fields found in {path.relative_to(ROOT)}")
    else:
        print(f"[WARN] JSON shape not dict/list[dict] -> {path.relative_to(ROOT)}")

# ------------------------------------------------------------
# Final Result
# ------------------------------------------------------------

print("\n[Test Result]\n")

if failures:
    print("[FAIL] serv_no inspection test failed:\n")
    for failure in failures:
        print(f"- {failure}")
    raise SystemExit(1)

print("[PASS] serv_no inspection scripts & candidate data validated.")

