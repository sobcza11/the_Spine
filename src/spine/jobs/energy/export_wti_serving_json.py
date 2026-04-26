from __future__ import annotations

import io
import json
import os
from pathlib import Path

import boto3
import pandas as pd


PRICE_KEY = "spine_us/leaves/energy/wti_price_t1.parquet"
INV_KEY = "spine_us/leaves/energy/crude_stocks_ex_spr_t2.parquet"

OUT_DIR = Path("data/serving/wti")

R2_OUT = {
    "price": "spine_us/serving/wti/wti_price_data.json",
    "inventory": "spine_us/serving/wti/wti_inventory.json",
    "panel": "spine_us/serving/wti/wti_panel.json",
}


def env(name):
    value = os.getenv(name) or os.getenv(name.replace("R2_BUCKET", "R2_BUCKET_NAME"))
    if not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value


def s3():
    return boto3.client(
        "s3",
        endpoint_url=env("R2_ENDPOINT"),
        aws_access_key_id=env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=env("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION", "auto"),
    )


def bucket():
    return os.getenv("R2_BUCKET") or env("R2_BUCKET_NAME")


def read_parquet(key):
    obj = s3().get_object(Bucket=bucket(), Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def upload_json(key, payload):
    s3().put_object(
        Bucket=bucket(),
        Key=key,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-store",
    )


def main():
    price = read_parquet(PRICE_KEY).copy()
    inv = read_parquet(INV_KEY).copy()

    price["date"] = pd.to_datetime(price["date"], errors="coerce")
    price["close"] = pd.to_numeric(price["close"], errors="coerce")
    price = price.dropna(subset=["date", "close"]).sort_values("date")

    inv["date"] = pd.to_datetime(inv["date"], errors="coerce")
    inv["value"] = pd.to_numeric(inv["value"], errors="coerce")
    inv = inv.dropna(subset=["date", "value"]).sort_values("date")

    price_latest = price["date"].max().strftime("%Y-%m-%d")
    inv_latest = inv["date"].max().strftime("%Y-%m-%d")

    price_payload = {
        "panel": "wti_price",
        "source": "EIA",
        "as_of_date": price_latest,
        "series": [
            {
                "symbol": str(r["symbol"]),
                "date": r["date"].strftime("%Y-%m-%d"),
                "close": float(r["close"]),
            }
            for _, r in price.iterrows()
        ],
    }

    inv_payload = {
        "panel": "wti_inventory",
        "source": "EIA",
        "series_id": "WCESTUS1",
        "frequency": "weekly",
        "as_of_date": inv_latest,
        "series": [
            {
                "symbol": str(r.get("symbol", "WCESTUS1")),
                "date": r["date"].strftime("%Y-%m-%d"),
                "value": float(r["value"]),
            }
            for _, r in inv.iterrows()
        ],
    }

    panel_payload = {
        "panel": "wti_panel",
        "source": "the_Spine",
        "as_of_date": max(price_latest, inv_latest),
        "summary": {
            "wti_price_as_of": price_latest,
            "inventory_as_of": inv_latest,
            "status": "operational",
        },
        "price": price_payload["series"],
        "inventory": inv_payload["series"],
    }

    write_json(OUT_DIR / "wti_price_data.json", price_payload)
    write_json(OUT_DIR / "wti_inventory.json", inv_payload)
    write_json(OUT_DIR / "wti_panel.json", panel_payload)

    upload_json(R2_OUT["price"], price_payload)
    upload_json(R2_OUT["inventory"], inv_payload)
    upload_json(R2_OUT["panel"], panel_payload)

    print("WTI serving export complete.")
    print(f"price_latest={price_latest}")
    print(f"inventory_latest={inv_latest}")
    print(f"panel_as_of={panel_payload['as_of_date']}")


if __name__ == "__main__":
    main()
