import os
import sys
from pathlib import Path

from utils.storage_r2 import upload_file_to_r2


def _require_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def main() -> int:
    """
    Upload a local WTI leaf artifact to R2 using the repo-standard helper.

    Required env (aligned to src/utils/storage_r2.py):
      - R2_ENDPOINT
      - R2_ACCESS_KEY
      - R2_SECRET_KEY
      - R2_BUCKET_US
      - WTI_LEAF_PATH
      - R2_OBJECT_KEY
    """
    try:
        # Hard-require config in CI. Locally you can still run with env set.
        for k in ["R2_ENDPOINT", "R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_BUCKET_US"]:
            _require_env(k)

        leaf_path = Path(_require_env("WTI_LEAF_PATH")).expanduser()
        r2_key = _require_env("R2_OBJECT_KEY")

        if not leaf_path.exists():
            raise RuntimeError(f"WTI_LEAF_PATH does not exist: {leaf_path}")

        upload_file_to_r2(str(leaf_path), r2_key)

        print("[WTI][R2] Upload OK")
        print(f"[WTI][R2] path={leaf_path}")
        print(f"[WTI][R2] key={r2_key}")
        return 0

    except Exception as e:
        print(f"[WTI][R2] Upload FAILED: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

