from pathlib import Path
import json
import pandas as pd


def normalize_0_1(series: pd.Series) -> pd.Series:
    min_v = series.min()
    max_v = series.max()

    if pd.isna(min_v) or pd.isna(max_v) or max_v == min_v:
        return pd.Series([0.5] * len(series), index=series.index)

    return (series - min_v) / (max_v - min_v)


def build_cot_raw_expansion_v1() -> pd.DataFrame:
    repo_root = Path.cwd()

    map_path = repo_root / "src" / "spine" / "cot" / "raw_expansion" / "cot_raw_instrument_map_v1.json"
    out_dir = repo_root / "data" / "cot" / "raw_expansion"
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(map_path, "r", encoding="utf-8") as f:
        instrument_map = json.load(f)

    rows = []

    for asset_class, instruments in instrument_map.items():
        for instrument in instruments:
            rows.append(
                {
                    "asset_class": asset_class,
                    "instrument": instrument,
                    "positioning_acceleration": 0.0,
                    "crowding_percentile": 0.5,
                    "unwind_probability": 0.0,
                    "cot_stress_score": 0.0,
                    "status": "scaffold_only",
                }
            )

    df = pd.DataFrame(rows)

    df["cot_stress_score"] = (
        0.35 * df["crowding_percentile"]
        + 0.35 * df["unwind_probability"]
        + 0.30 * normalize_0_1(df["positioning_acceleration"])
    ).round(4)

    parquet_path = out_dir / "cot_raw_expansion_v1.parquet"
    json_path = out_dir / "cot_raw_expansion_v1.json"

    df.to_parquet(parquet_path, index=False)
    df.to_json(json_path, orient="records", indent=2)

    print("Raw COT expansion scaffold complete")
    print("Rows:", len(df))
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)

    return df


if __name__ == "__main__":
    build_cot_raw_expansion_v1()
