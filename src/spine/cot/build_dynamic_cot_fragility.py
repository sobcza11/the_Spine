from pathlib import Path
import json
import pandas as pd
import numpy as np


REPO_ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine"
)

COT_SOURCE_CANDIDATES = [
    REPO_ROOT / "data" / "serving" / "cot" / "btc_futures_cot_serving_v1.parquet",
    REPO_ROOT / "data" / "cot" / "COT_VinV_weekly_1989_2025.parquet",
    REPO_ROOT / "data" / "cot" / "COT_VinV_indexes_1989_2025_monthly.parquet",
]

EXPORT_ROOT = (
    REPO_ROOT
    / "data"
    / "serving"
    / "cot"
)

GEOSCEN_EXPORT_ROOT = (
    REPO_ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "cot"
)

VALIDATION_ROOT = (
    REPO_ROOT
    / "data"
    / "finstate"
    / "validation"
)


def find_cot_source():

    for path in COT_SOURCE_CANDIDATES:
        if path.exists():
            return path

    raise FileNotFoundError(
        "No COT source file found in candidate paths."
    )


def normalize_cot_columns(df):

    df = df.copy()

    lower_map = {
        col: str(col).lower().strip()
        for col in df.columns
    }

    df = df.rename(
        columns=lower_map
    )

    date_candidates = [
        "date",
        "report_date",
        "as_of_date",
        "week",
        "timestamp",
    ]

    asset_candidates = [
        "asset",
        "symbol",
        "ticker",
        "market",
        "contract",
        "name",
    ]

    position_candidates = [
        "net_position",
        "net",
        "noncommercial_net",
        "managed_money_net",
        "large_spec_net",
        "spec_net",
        "position",
        "value",
    ]

    date_col = next(
        (c for c in date_candidates if c in df.columns),
        None
    )

    asset_col = next(
        (c for c in asset_candidates if c in df.columns),
        None
    )

    position_col = next(
        (c for c in position_candidates if c in df.columns),
        None
    )

    if date_col is None:
        raise KeyError(
            f"No date column found. Columns: {list(df.columns)}"
        )

    if position_col is None:
        numeric_cols = [
            c for c in df.columns
            if c != date_col
            and pd.api.types.is_numeric_dtype(df[c])
        ]

        if not numeric_cols:
            raise KeyError(
                f"No numeric position column found. Columns: {list(df.columns)}"
            )

        position_col = numeric_cols[0]

    if asset_col is None:
        df["asset"] = "COT_SOURCE"
        asset_col = "asset"

    out = df[
        [
            date_col,
            asset_col,
            position_col,
        ]
    ].copy()

    out.columns = [
        "date",
        "asset",
        "net_position",
    ]

    out["date"] = pd.to_datetime(
        out["date"],
        errors="coerce"
    )

    out["net_position"] = pd.to_numeric(
        out["net_position"],
        errors="coerce"
    )

    out = out.dropna(
        subset=[
            "date",
            "asset",
            "net_position",
        ]
    )

    return out


def load_cot_panel():

    source_path = find_cot_source()

    df = pd.read_parquet(
        source_path
    )

    panel = normalize_cot_columns(df)

    return panel, source_path


def percentile_rank(series):

    return (
        series.rank(pct=True)
        * 100
    )


def build_dynamic_cot_fragility():

    df, source_path = load_cot_panel()

    df = df.sort_values(
        [
            "asset",
            "date",
        ]
    ).reset_index(drop=True)

    df["position_change_1"] = (
        df.groupby("asset")["net_position"]
        .diff(1)
    )

    df["position_change_4"] = (
        df.groupby("asset")["net_position"]
        .diff(4)
    )

    df["position_acceleration"] = (
        df.groupby("asset")["position_change_1"]
        .diff(1)
    )

    df["crowding_percentile"] = (
        df.groupby("asset")["net_position"]
        .transform(percentile_rank)
    )

    df["positioning_fragility_score"] = (
        (
            df["crowding_percentile"].fillna(0) / 100
        )
        + (
            df["position_acceleration"]
            .abs()
            .rank(pct=True)
            .fillna(0)
        )
    ) / 2

    df["cot_fragility_state"] = np.select(
        [
            df["positioning_fragility_score"] >= 0.85,
            df["positioning_fragility_score"] >= 0.70,
            df["positioning_fragility_score"] >= 0.50,
        ],
        [
            "forced_unwind_risk",
            "crowded_positioning",
            "watch_positioning",
        ],
        default="contained_positioning"
    )

    df["geoscen_message_type"] = np.select(
        [
            df["cot_fragility_state"] == "forced_unwind_risk",
            df["cot_fragility_state"] == "crowded_positioning",
            df["cot_fragility_state"] == "watch_positioning",
        ],
        [
            "cot_forced_unwind_signal",
            "cot_crowding_signal",
            "cot_positioning_watch_signal",
        ],
        default="cot_contained_signal"
    )

    df["source_path"] = str(source_path)

    return df


def export_dynamic_cot_fragility():

    EXPORT_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )

    GEOSCEN_EXPORT_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )

    VALIDATION_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )

    panel = build_dynamic_cot_fragility()

    parquet_path = (
        EXPORT_ROOT
        / "dynamic_cot_fragility_panel_v1.parquet"
    )

    preview_path = (
        VALIDATION_ROOT
        / "dynamic_cot_fragility_panel_v1_preview.csv"
    )

    payload_path = (
        GEOSCEN_EXPORT_ROOT
        / "geoscen_cot_fragility_payload_v1.json"
    )

    panel.to_parquet(
        parquet_path,
        index=False
    )

    panel.head(250).to_csv(
        preview_path,
        index=False
    )

    latest = (
        panel.sort_values("date")
        .groupby("asset")
        .tail(1)
    )

    routes = []

    for _, row in latest.iterrows():

        routes.append(
            {
                "sender": "DynamicCOTFragility",
                "target": "GeoScenAgent",
                "message_type": row["geoscen_message_type"],
                "payload": {
                    "asset": row["asset"],
                    "date": str(row["date"]),
                    "net_position": float(row["net_position"]),
                    "position_change_1": (
                        None
                        if pd.isna(row["position_change_1"])
                        else float(row["position_change_1"])
                    ),
                    "position_change_4": (
                        None
                        if pd.isna(row["position_change_4"])
                        else float(row["position_change_4"])
                    ),
                    "position_acceleration": (
                        None
                        if pd.isna(row["position_acceleration"])
                        else float(row["position_acceleration"])
                    ),
                    "crowding_percentile": (
                        None
                        if pd.isna(row["crowding_percentile"])
                        else float(row["crowding_percentile"])
                    ),
                    "positioning_fragility_score": (
                        None
                        if pd.isna(row["positioning_fragility_score"])
                        else float(row["positioning_fragility_score"])
                    ),
                    "cot_fragility_state": row["cot_fragility_state"],
                },
            }
        )

    payload = {
        "component": "DynamicCOTFragility",
        "status": "operational",
        "assets": int(latest["asset"].nunique()),
        "rows": int(panel.shape[0]),
        "routes": routes,
        "states": sorted(
            latest["cot_fragility_state"]
            .dropna()
            .unique()
            .tolist()
        ),
    }

    with open(
        payload_path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            payload,
            f,
            indent=2,
            default=str
        )

    return {
        "status": "ok",
        "rows": int(panel.shape[0]),
        "assets": int(panel["asset"].nunique()),
        "states": payload["states"],
        "parquet_path": str(parquet_path),
        "preview_path": str(preview_path),
        "payload_path": str(payload_path),
    }


if __name__ == "__main__":

    result = export_dynamic_cot_fragility()

    print(result)
