from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path.cwd()

# governed input from Spine / export prep
PRICE_PANEL_PATH = REPO_ROOT / "data" / "fx_price_panel.parquet"

# IsoVector static export target
OUTPUT_JS_PATH = REPO_ROOT / "IsoVector" / "fx_sigma_data.js"


PAIR_LABELS = {
    "EURUSD": "EUR/USD",
    "AUDUSD": "AUD/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
}


def _compute_sigma_rows(df: pd.DataFrame, trailing_window: int = 20) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["pair", "date"]).reset_index(drop=True)

    out_rows = []

    for symbol, g in df.groupby("pair", sort=True):
        closes = pd.to_numeric(g["close"], errors="coerce").dropna().to_numpy()

        if closes.size < trailing_window + 5:
            continue

        rets = pd.Series(closes).pct_change().dropna()
        trailing = rets.tail(trailing_window)

        if trailing.empty:
            continue

        vol = float(trailing.std(ddof=0))
        ease = float(1.0 / vol) if vol > 0 else 0.0

        out_rows.append(
            {
                "symbol": symbol,
                "pair": PAIR_LABELS.get(symbol, symbol),
                "trailing_window": trailing_window,
                "obs_count": int(trailing.size),
                "vol": vol,
                "ease": ease,
                "as_of_date": str(g["date"].max().date()),
            }
        )

    out = pd.DataFrame(out_rows)

    if out.empty:
        return out

    ease_mean = float(out["ease"].mean())
    ease_std = float(out["ease"].std(ddof=0))

    if ease_std == 0:
        ease_std = 1.0

    out["z"] = (out["ease"] - ease_mean) / ease_std
    out = out.sort_values(["ease", "pair"], ascending=[False, True]).reset_index(drop=True)
    out["rank"] = np.arange(1, len(out) + 1)

    return out[
        [
            "pair",
            "symbol",
            "rank",
            "z",
            "vol",
            "ease",
            "trailing_window",
            "obs_count",
            "as_of_date",
        ]
    ]


def main() -> None:
    df = pd.read_parquet(PRICE_PANEL_PATH).copy()

    required = {"pair", "date", "close"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"PRICE_PANEL_PATH missing required columns: {missing}")

    sigma = _compute_sigma_rows(df, trailing_window=20)

    payload = sigma.to_dict(orient="records")
    js_text = "window.IV_FX_SIGMA_DATA = " + json.dumps(payload, indent=2) + ";\n"

    OUTPUT_JS_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JS_PATH.write_text(js_text, encoding="utf-8")

    print(f"wrote {OUTPUT_JS_PATH}")
    print(f"rows: {len(payload)}")


if __name__ == "__main__":
    main()
    