from __future__ import annotations

import io
import json
import os
from pathlib import Path

import boto3
import pandas as pd

REPO_ROOT = Path.cwd()

SOURCE_R2_KEY = "spine_us/global_equity_region_t1.parquet"

OUT_DIR = REPO_ROOT / "data" / "serving" / "equities"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PANEL = OUT_DIR / "global_equity_region_panel.json"
OUT_LATEST = OUT_DIR / "global_equity_region_latest.json"


def _env(name: str, required: bool = True) -> str:
    v = os.getenv(name, "").strip()
    if required and not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def _bucket_name() -> str:
    return _env("R2_BUCKET_NAME", False) or _env("R2_BUCKET")


def _r2_endpoint() -> str:
    ep = _env("R2_ENDPOINT", False)
    if ep:
        return ep
    account_id = _env("R2_ACCOUNT_ID")
    return f"https://{account_id}.r2.cloudflarestorage.com"


def _r2_client():
    return boto3.client(
        "s3",
        endpoint_url=_r2_endpoint(),
        aws_access_key_id=_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def _read_parquet_from_r2(key: str) -> pd.DataFrame:
    obj = _r2_client().get_object(Bucket=_bucket_name(), Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _zscore(s: pd.Series, window: int = 252) -> pd.Series:
    mean = s.rolling(window, min_periods=60).mean()
    std = s.rolling(window, min_periods=60).std()
    return ((s - mean) / std).replace([float("inf"), float("-inf")], pd.NA)


def _state(score: float) -> str:
    if score >= 1.0:
        return "Risk-On / Strong"
    if score >= 0.25:
        return "Constructive"
    if score <= -1.0:
        return "Risk-Off / Weak"
    if score <= -0.25:
        return "Cautious"
    return "Balanced / Monitoring"


def main() -> int:
    df = _read_parquet_from_r2(SOURCE_R2_KEY)

    if df.empty:
        raise RuntimeError("Global equity region source is empty.")

    df["date"] = pd.to_datetime(df["date"]).dt.floor("D")
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["region"] = df["region"].astype(str)

    df = df.sort_values(["symbol", "date"])

    df["ret_1d"] = df.groupby("symbol")["close"].pct_change()
    df["ret_20d"] = df.groupby("symbol")["close"].pct_change(20)
    df["ret_60d"] = df.groupby("symbol")["close"].pct_change(60)
    df["vol_20d"] = (
        df.groupby("symbol")["ret_1d"]
        .rolling(20)
        .std()
        .reset_index(level=0, drop=True)
    )

    df["momentum_z"] = df.groupby("symbol")["ret_20d"].transform(_zscore)
    df["vol_z"] = df.groupby("symbol")["vol_20d"].transform(_zscore)

    df["equity_region_score"] = df["momentum_z"] - (0.35 * df["vol_z"])

    latest_rows = (
        df.sort_values("date")
        .groupby("symbol", as_index=False)
        .tail(1)
        .copy()
    )

    latest_rows["state"] = latest_rows["equity_region_score"].apply(
        lambda x: _state(float(x)) if pd.notna(x) else "Insufficient History"
    )

    panel_rows = []

    for _, r in latest_rows.iterrows():
        panel_rows.append({
            "symbol": r["symbol"],
            "region": r["region"],
            "date": str(pd.to_datetime(r["date"]).date()),
            "close": round(float(r["close"]), 4) if pd.notna(r["close"]) else None,
            "ret_20d": round(float(r["ret_20d"]), 4) if pd.notna(r["ret_20d"]) else None,
            "ret_60d": round(float(r["ret_60d"]), 4) if pd.notna(r["ret_60d"]) else None,
            "vol_20d": round(float(r["vol_20d"]), 4) if pd.notna(r["vol_20d"]) else None,
            "momentum_z": round(float(r["momentum_z"]), 4) if pd.notna(r["momentum_z"]) else None,
            "vol_z": round(float(r["vol_z"]), 4) if pd.notna(r["vol_z"]) else None,
            "equity_region_score": round(float(r["equity_region_score"]), 4) if pd.notna(r["equity_region_score"]) else None,
            "state": r["state"],
            "source": "Tiingo",
        })

    region_df = pd.DataFrame(panel_rows)

    region_summary = (
        region_df.groupby("region", as_index=False)
        .agg(
            score=("equity_region_score", "mean"),
            symbols=("symbol", "count"),
        )
        .sort_values("region")
    )

    latest_payload = {
        "meta": {
            "name": "global_equity_region_latest",
            "source": "Tiingo",
            "source_r2_key": SOURCE_R2_KEY,
            "method": "Momentum z-score minus volatility penalty by ETF.",
            "rows": len(panel_rows),
        },
        "regions": [
            {
                "region": r["region"],
                "score": round(float(r["score"]), 4) if pd.notna(r["score"]) else None,
                "symbols": int(r["symbols"]),
                "state": _state(float(r["score"])) if pd.notna(r["score"]) else "Insufficient History",
            }
            for _, r in region_summary.iterrows()
        ],
        "rows": panel_rows,
    }

    panel_payload = {
        "meta": latest_payload["meta"],
        "rows": panel_rows,
    }

    OUT_PANEL.write_text(json.dumps(panel_payload, indent=2), encoding="utf-8")
    OUT_LATEST.write_text(json.dumps(latest_payload, indent=2), encoding="utf-8")

    print("BUILT:", OUT_PANEL)
    print("BUILT:", OUT_LATEST)
    print("ROWS:", len(panel_rows))
    print(region_summary.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

