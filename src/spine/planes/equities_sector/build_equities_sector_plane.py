from datetime import datetime, timezone
from pathlib import Path
import json
import pandas as pd


SECTOR_MAP = {
    "XLB": "Materials",
    "XLC": "Communication Services",
    "XLE": "Energy",
    "XLF": "Financials",
    "XLI": "Industrials",
    "XLK": "Technology",
    "XLP": "Consumer Staples",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
    "XLV": "Health Care",
    "XLY": "Consumer Discretionary",
}


def classify_sector_state(signal: float) -> str:
    if signal >= 7:
        return "leadership"
    if signal >= 3:
        return "improving"
    if signal <= -7:
        return "stress"
    if signal <= -3:
        return "weakening"
    return "neutral"


def build_sector_plane(df: pd.DataFrame) -> dict:
    required = {"symbol", "signal"}
    missing = required - set(df.columns)

    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    rows = []

    for _, r in df.iterrows():
        symbol = r["symbol"]
        signal = float(r["signal"])

        rows.append({
            "symbol": symbol,
            "sector": SECTOR_MAP.get(symbol, "Unknown"),
            "signal": round(signal, 2),
            "state": classify_sector_state(signal),
        })

    leaders = sorted(rows, key=lambda x: x["signal"], reverse=True)[:3]
    laggards = sorted(rows, key=lambda x: x["signal"])[:3]

    return {
        "system": "IsoVector",
        "plane": "equities_sector",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "Sector ETF USA",
        "rows": rows,
        "leaders": leaders,
        "laggards": laggards,
        "governance": {
            "ai_generated": False,
            "measurement_source": "the_Spine",
            "interpretation_layer": "IsoVector",
        },
    }


def write_sector_plane(payload: dict, out_path: str | Path) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

        