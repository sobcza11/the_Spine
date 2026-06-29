############################################################
# upload_policy_language_to_r2.py
############################################################

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]

SERVING = ROOT / "data" / "serving" / "rates"

files = sorted(
    SERVING.glob("*_policy_language_latest.json")
)

if not files:
    raise RuntimeError("No policy-language JSON files found.")

for file in files:

    key = f"spine_us/serving/rates/{file.name}"

    print("=" * 70)
    print(file.name)
    print("=" * 70)

    subprocess.run(
        [
            "aws",
            "s3",
            "cp",
            str(file),
            f"s3://{ROOT.joinpath('').name}/{key}",
            "--endpoint-url",
            "https://$R2_ENDPOINT_URL",
        ],
        check=True,
    )

print()
print("Upload Complete.")
