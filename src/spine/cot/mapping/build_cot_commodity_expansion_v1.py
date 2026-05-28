from pathlib import Path
import json
import pandas as pd


# =========================================================
# COMMODITY / MACRO ROUTING MAP
# =========================================================

COMMODITY_VECTOR_MAP = {

    # ENERGY
    "CL": {
        "instrument_group": "energy",
        "macro_theme": "inflation_pressure",
        "iv_vectors": ["C", "X"],
        "geoscen_channel": "commodity_supply_shock",
    },

    "NG": {
        "instrument_group": "energy",
        "macro_theme": "energy_instability",
        "iv_vectors": ["X", "S"],
        "geoscen_channel": "energy_system_stress",
    },

    # METALS
    "GC": {
        "instrument_group": "precious_metals",
        "macro_theme": "debasement_flight",
        "iv_vectors": ["L", "S"],
        "geoscen_channel": "monetary_instability",
    },

    "SI": {
        "instrument_group": "precious_metals",
        "macro_theme": "inflation_volatility",
        "iv_vectors": ["S", "X"],
        "geoscen_channel": "inflation_escalation",
    },

    "HG": {
        "instrument_group": "industrial_metals",
        "macro_theme": "global_growth_stress",
        "iv_vectors": ["C", "X"],
        "geoscen_channel": "industrial_slowdown",
    },

    # FX
    "GBP": {
        "instrument_group": "fx",
        "macro_theme": "currency_instability",
        "iv_vectors": ["X", "S"],
        "geoscen_channel": "fx_transition",
    },

    "CAD": {
        "instrument_group": "fx",
        "macro_theme": "commodity_fx",
        "iv_vectors": ["C", "X"],
        "geoscen_channel": "commodity_currency_stress",
    },

    "AUD": {
        "instrument_group": "fx",
        "macro_theme": "china_sensitivity",
        "iv_vectors": ["C", "S"],
        "geoscen_channel": "asia_growth_transition",
    },

    "CHF": {
        "instrument_group": "fx",
        "macro_theme": "safe_haven_flow",
        "iv_vectors": ["L", "S"],
        "geoscen_channel": "risk_off_transition",
    },
}


def build_cot_commodity_expansion_v1():

    repo_root = Path.cwd()

    out_dir = (
        repo_root
        / "data"
        / "cot"
        / "mapping"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []

    for instrument, metadata in COMMODITY_VECTOR_MAP.items():

        row = {
            "instrument": instrument,
            **metadata,
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    parquet_path = (
        out_dir
        / "cot_commodity_expansion_v1.parquet"
    )

    json_path = (
        out_dir
        / "cot_commodity_expansion_v1.json"
    )

    summary_path = (
        out_dir
        / "cot_commodity_expansion_summary_v1.json"
    )

    df.to_parquet(
        parquet_path,
        index=False,
    )

    df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    summary = {
        "component": "cot_commodity_expansion_v1",
        "instrument_count": int(len(df)),
        "instrument_groups": sorted(
            df["instrument_group"].unique().tolist()
        ),
        "macro_themes": sorted(
            df["macro_theme"].unique().tolist()
        ),
        "iv_vectors_used": sorted(
            list(
                set(
                    v
                    for sublist in df["iv_vectors"]
                    for v in sublist
                )
            )
        ),
        "status": "commodity_expansion_mapping_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("COT commodity expansion complete")
    print("Rows:", len(df))
    print("Instruments:", sorted(df["instrument"].tolist()))
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df


if __name__ == "__main__":
    build_cot_commodity_expansion_v1()
