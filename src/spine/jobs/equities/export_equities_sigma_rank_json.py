import pandas as pd
import numpy as np
from pathlib import Path
import json

ROOT = Path.cwd()
DATA = ROOT / "data" / "serving" / "equities" / "us_equity_index_data.json"
OUT  = ROOT / "data" / "serving" / "equities" / "equities_sigma_rank.json"

WINDOW = 20

def main():
    if not DATA.exists():
        raise RuntimeError(f"Missing source file: {DATA}")

    with open(DATA, "r") as f:
        payload = json.load(f)

    rows = []

    for symbol, series in payload.items():
        df = pd.DataFrame(series)
        if df.empty:
            continue

        df["date"] = pd.to_datetime(df["date"])
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna().sort_values("date")

        df["ret"] = df["close"].pct_change()
        df["vol"] = df["ret"].rolling(WINDOW).std() * np.sqrt(252)

        latest = df.dropna().tail(1)
        if latest.empty:
            continue

        r = latest.iloc[0]

        rows.append({
            "symbol": symbol,
            "pair": symbol,
            "as_of_date": r["date"].strftime("%Y-%m-%d"),
            "realized_vol_20d": float(r["vol"])
        })

    if not rows:
        raise RuntimeError("No sigma rows generated.")

    vols = np.array([r["realized_vol_20d"] for r in rows])
    mean = vols.mean()
    std = vols.std() if vols.std() != 0 else 1

    for r in rows:
        r["z"] = float((r["realized_vol_20d"] - mean) / std)

    rows = sorted(rows, key=lambda x: x["z"], reverse=True)

    for i, r in enumerate(rows, start=1):
        r["rank"] = i
        r["state"] = "easy" if r["z"] > 0 else "tight"

    with open(OUT, "w") as f:
        json.dump(rows, f, indent=2)

    print("Equities sigma export complete.")
    print(f"as_of={rows[0]['as_of_date']}")

if __name__ == "__main__":
    main()
    