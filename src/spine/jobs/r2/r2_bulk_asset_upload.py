import os
from pathlib import Path

import boto3


ROOT = Path.cwd()
PREFIX = "the_spine_assets"

ASSET_ROOTS = [
    "data",
    "exports",
    "metadata",
    "reports",
    "spine_us",
    "the_OracleChambers",
    "vinv",
]

def load_ignore_patterns():
    ignore_file = ROOT / ".spineignore"
    patterns = []

    if ignore_file.exists():
        patterns = [
            line.strip()
            for line in ignore_file.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]

    return patterns


IGNORE_PATTERNS = load_ignore_patterns()


def should_skip(path: Path) -> bool:
    path_str = path.as_posix()

    for pattern in IGNORE_PATTERNS:
        if pattern.endswith("/"):
            if pattern[:-1] in path.parts:
                return True

        elif pattern.startswith("*"):
            if path.name.endswith(pattern[1:]):
                return True

        elif pattern == "~$":
            if path.name.startswith("~$"):
                return True

        elif pattern in path_str:
            return True

    return False


def main():
    required = [
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET_NAME",
        "R2_ENDPOINT",
        "R2_REGION",
    ]

    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise EnvironmentError(f"Missing R2 env vars: {missing}")

    bucket = os.getenv("R2_BUCKET_NAME")

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION"),
    )

    uploaded = 0
    skipped = 0
    failed = 0

    for root_name in ASSET_ROOTS:
        root = ROOT / root_name

        if not root.exists():
            print(f"SKIP_MISSING_ROOT {root_name}")
            skipped += 1
            continue

        for path in root.rglob("*"):
            if not path.is_file():
                continue

            rel = path.relative_to(ROOT)

            if should_skip(rel):
                skipped += 1
                print(f"SKIPPED {rel.as_posix()}")
                continue

            key = f"{PREFIX}/{rel.as_posix()}"

            try:
                client.upload_file(
                    Filename=str(path),
                    Bucket=bucket,
                    Key=key,
                )
                uploaded += 1
                print(f"UPLOADED s3://{bucket}/{key}")

            except PermissionError as exc:
                failed += 1
                print(f"FAILED_PERMISSION {rel.as_posix()} :: {exc}")

            except Exception as exc:
                failed += 1
                print(f"FAILED_UPLOAD {rel.as_posix()} :: {type(exc).__name__}: {exc}")

    print("R2_BULK_ASSET_UPLOAD_COMPLETE")
    print(f"bucket={bucket}")
    print(f"uploaded={uploaded}")
    print(f"skipped={skipped}")
    print(f"failed={failed}")

    if failed > 0:
        raise SystemExit(f"R2 bulk upload completed with failed={failed}")


if __name__ == "__main__":
    main()
