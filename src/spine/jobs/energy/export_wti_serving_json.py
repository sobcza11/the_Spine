from __future__ import annotations

import io
import json
import os
from pathlib import Path

import boto3
import pandas as pd


PRICE_KEY = "spine_us/leaves/energy/wti_price_t1.parquet"
INV_KEY = "spine_us/leaves/energy/crude_stocks_ex_spr_t2.parquet"

LOCAL_OUT_DIR = Path("data/serving/wti")

OUT_PRICE = LOCAL_OUT_DIR / "wti_price_data.json"
OUT_INV = LOCAL_OUT_DIR / "wti_inventory.json"
OUT_PANEL = LOCAL_OUT_DIR / "wti_panel.json"

R2_PRICE_OUT = "spine_us/serving/wti/wti_price_data.json"
R2_INV_OUT = "spine_us/serving/wti/wti_inventory.json"
R2_PANEL_OUT = "spine_us/serving/wti/wti_panel.json"


def _env(name: str, required: bool = True) -> str:
    value = os.getenv(name, "").strip()
    if required and not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _bucket() -> str:
    return _env("R2_BUCKET", required=False) or _env("R2_BUCKET_NAME")


def _r2_client():
    return boto3.client(
        "s3",
        endpoint_url=_env("R2_ENDPOINT"),
        aws_access_key_id=_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=_env("R2_SECRET_ACCESS_KEY"),
        region_name=_env("R2_REGION", required=False) or "auto",
    )


def _read_parquet(key: str) -> pd.DataFrame:
    obj = _r2_client().get_object(Bucket=_bucket(), Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _write_local_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _upload_json(key: str, payload: dict | list) -> None:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    _r2_client().put_object(
        Bucket=_bucket(),
        Key=key,
        Body=body,
        ContentType="application/json",
        CacheControl="no-store",
    )


def main() -> None:
    price = _read_parquet(PRICE_KEY).copy()
    inv = _read_parquet(INV_KEY).copy()

    price["date"] = pd.to_datetime(price["date"], errors="coerce")
    price["close"] = pd.to_numeric(price["close"], errors="coerce")
    price = price.dropna(subset=["date", "close"]).sort_values("date")

    inv["date"] = pd.to_datetime(inv["date"], errors="coerce")
    inv["value"] = pd.to_numeric(inv["value"], errors="coerce")
    inv = inv.dropna(subset=["date", "value"]).sort_values("date")

    price_latest = price["date"].max().strftime("%Y-%m-%d")
    inv_latest = inv["date"].max().strftime("%Y-%m-%d")

    price_rows = [
        {
            "symbol": str(r["symbol"]),
            "date": r["date"].strftime("%Y-%m-%d"),
            "close": float(r["close"]),
        }
        for _, r in price.iterrows()
    ]

    inv_rows = [
        {
            "symbol": str(r["symbol"]),
            "date": r["date"].strftime("%Y-%m-%d"),
            "value": float(r["value"]),
        }
        for _, r in inv.iterrows()
    ]

    inv_payload = {
        "panel": "wti_inventory",
        "source": "EIA",
        "series_id": "WCESTUS1",
        "frequency": "weekly",
        "as_of_date": inv_latest,
        "series": inv_rows,
    }

    panel_payload = {
        "panel": "wti_panel",
        "source": "the_Spine",
        "as_of_date": max(price_latest, inv_latest),
        "summary": {
            "wti_price_as_of": price_latest,
            "inventory_as_of": inv_latest,
        },
        "price": price_rows,
        "inventory": inv_rows,
    }

    _write_local_json(OUT_PRICE, price_rows)
    _write_local_json(OUT_INV, inv_payload)
    _write_local_json(OUT_PANEL, panel_payload)

    _upload_json(R2_PRICE_OUT, price_rows)
    _upload_json(R2_INV_OUT, inv_payload)
    _upload_json(R2_PANEL_OUT, panel_payload)

    print("WTI serving export complete.")
    print(f"price_latest={price_latest}")
    print(f"inventory_latest={inv_latest}")
    print(f"panel_as_of={panel_payload['as_of_date']}")


if __name__ == "__main__":
    main()

