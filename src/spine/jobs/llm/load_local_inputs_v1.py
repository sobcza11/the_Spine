from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path.cwd()
MANIFEST_PATH = REPO_ROOT / "config" / "llm" / "approved_r2_inputs_v1.json"


def load_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_approved_inputs(manifest: dict[str, Any]) -> dict[str, pd.DataFrame]:
    loaded: dict[str, pd.DataFrame] = {}

    for item in manifest["inputs"]:
        input_id = item["input_id"]
        path = REPO_ROOT / item["path"]
        required = item.get("required", False)
        allowed_fields = item["allowed_fields"]

        if not path.exists():
            if required:
                raise FileNotFoundError(f"Required input missing: {input_id} | {path}")
            continue

        if item["format"] != "parquet":
            raise ValueError(f"Unsupported input format for {input_id}: {item['format']}")

        df = pd.read_parquet(path)

        missing_cols = set(allowed_fields) - set(df.columns)
        if missing_cols:
            raise KeyError(
                f"{input_id} missing required approved fields: {sorted(missing_cols)}. "
                f"Found: {list(df.columns)}"
            )

        df = df[allowed_fields].copy()

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        loaded[input_id] = df

    return loaded


def main() -> None:
    manifest = load_manifest()
    loaded = read_approved_inputs(manifest)

    print("Approved local inputs loaded:")
    for input_id, df in loaded.items():
        print(f"- {input_id}: {df.shape}")


if __name__ == "__main__":
    main()

    