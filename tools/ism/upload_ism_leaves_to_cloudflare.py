from pathlib import Path
import subprocess

ROOT = Path.cwd()

files = [
    "us_ism_manu_pmi_by_industry_canonical.parquet",
    "us_ism_manu_no_by_industry_canonical.parquet",
    "us_ism_nonmanu_pmi_by_industry_canonical.parquet",
    "us_ism_nonmanu_no_by_industry_canonical.parquet",
]

bucket = "thespine-us-hub"
prefix = "spine_us/leaves/ism"

for file_name in files:
    local_path = ROOT / "data" / "ism" / file_name

    if not local_path.exists():
        print(f"[SKIP] missing {local_path}")
        continue

    object_path = f"{bucket}/{prefix}/{file_name}"

    cmd = [
        "wrangler",
        "r2",
        "object",
        "put",
        object_path,
        "--file",
        str(local_path),
        "--remote",
    ]

    print("\nUploading:", file_name)
    print(" ".join(cmd))

    subprocess.run(cmd, check=True)

print("\n[OK] ISM leaves uploaded to Cloudflare R2.")
