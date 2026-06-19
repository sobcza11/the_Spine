from pathlib import Path
from datetime import timedelta
import json
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

ROOT = Path(__file__).resolve().parents[3]

INPUT = ROOT / "data/cot/signals/cot_crowding_latest_v1.json"
OUTPUT = ROOT / "data/serving/cflow/cot_positioning_serving.json"


def next_business_day(date: pd.Timestamp) -> pd.Timestamp:
    holidays = USFederalHolidayCalendar().holidays(
        start=date - pd.Timedelta(days=7),
        end=date + pd.Timedelta(days=14),
    )

    while date.weekday() >= 5 or date.normalize() in holidays:
        date += pd.Timedelta(days=1)

    return date


def cot_release_date(as_of_date: pd.Timestamp) -> pd.Timestamp:
    """
    COT data is measured Tuesday.
    Standard public release is Friday.
    If Friday is a federal holiday, move to next business day.
    Handles Juneteenth, Christmas, New Year, etc.
    """
    release = as_of_date + pd.Timedelta(days=3)
    return next_business_day(release)


def main():
    df = pd.read_json(INPUT)
    df["date"] = pd.to_datetime(df["date"])

    latest_as_of = df["date"].max()
    latest_df = df[df["date"] == latest_as_of].copy()

    top = latest_df.sort_values(
        "crowding_stress_score",
        ascending=False
    ).iloc[0]

    release_date = cot_release_date(latest_as_of)

    out = {
        "metric": "COT Positioning",
        "category": "Financial Transmission",
        "sub_category": "Positioning",
        "source": "CFTC Commitment of Traders",
        "frequency": "Weekly",
        "latest": {
            "date": str(release_date.date()),
            "measurement_date": str(latest_as_of.date()),
            "release_date": str(release_date.date()),
            "value": round(float(top["crowding_stress_score"]), 4),
            "score": round(float(top["crowding_stress_score"]) * 100, 2),
            "state": str(top["crowding_direction"]),
            "data_vintage": str(latest_as_of.date()),
            "display_basis": "release_date",
        },
        "attribution": {
            "instrument": str(top["instrument"]),
            "market_name": str(top["market_name"]),
            "zscore": float(top["net_position_zscore"]),
            "extreme_flag": int(top["crowding_extreme_flag"]),
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2))

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()




