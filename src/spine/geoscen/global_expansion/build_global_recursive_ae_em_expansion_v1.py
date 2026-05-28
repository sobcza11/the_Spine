from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


REGION_PACKS = [
    {
        "region": "US",
        "block": "AE",
        "recursive_pressure": 0.3897,
        "policy_pressure": 0.3897,
        "fx_pressure": 0.4016,
        "rates_pressure": 0.3900,
        "growth_fragility": 0.3895,
        "liquidity_stress": 0.3961,
        "weight": 0.30,
    },
    {
        "region": "Europe",
        "block": "AE",
        "recursive_pressure": 0.36,
        "policy_pressure": 0.34,
        "fx_pressure": 0.37,
        "rates_pressure": 0.35,
        "growth_fragility": 0.38,
        "liquidity_stress": 0.33,
        "weight": 0.20,
    },
    {
        "region": "Japan",
        "block": "AE",
        "recursive_pressure": 0.33,
        "policy_pressure": 0.31,
        "fx_pressure": 0.40,
        "rates_pressure": 0.29,
        "growth_fragility": 0.32,
        "liquidity_stress": 0.30,
        "weight": 0.12,
    },
    {
        "region": "China",
        "block": "EM",
        "recursive_pressure": 0.37,
        "policy_pressure": 0.36,
        "fx_pressure": 0.35,
        "rates_pressure": 0.34,
        "growth_fragility": 0.42,
        "liquidity_stress": 0.39,
        "weight": 0.18,
    },
    {
        "region": "EM_ExChina",
        "block": "EM",
        "recursive_pressure": 0.34,
        "policy_pressure": 0.32,
        "fx_pressure": 0.39,
        "rates_pressure": 0.33,
        "growth_fragility": 0.35,
        "liquidity_stress": 0.36,
        "weight": 0.20,
    },
]


AE_EM_LINKS = [
    {
        "source": "US",
        "target": "Europe",
        "channel": "dollar_rates_policy_transmission",
        "weight": 0.30,
    },
    {
        "source": "US",
        "target": "EM_ExChina",
        "channel": "usd_liquidity_to_em_fragility",
        "weight": 0.35,
    },
    {
        "source": "China",
        "target": "Europe",
        "channel": "china_growth_to_europe_trade",
        "weight": 0.25,
    },
    {
        "source": "China",
        "target": "EM_ExChina",
        "channel": "china_liquidity_to_em_cycle",
        "weight": 0.30,
    },
    {
        "source": "Japan",
        "target": "US",
        "channel": "jpy_carry_to_us_duration",
        "weight": 0.25,
    },
    {
        "source": "Europe",
        "target": "US",
        "channel": "euro_area_fragility_to_us_fx",
        "weight": 0.20,
    },
]


def classify_region_state(score):
    if score >= 0.80:
        return "global_systemic_stress"

    if score >= 0.65:
        return "global_fragile_stress"

    if score >= 0.50:
        return "global_elevated_stress"

    if score >= 0.35:
        return "global_watch_stress"

    return "global_stable"


def build_global_recursive_ae_em_expansion_v1():
    repo_root = Path.cwd()

    out_dir = repo_root / "data" / "geoscen" / "global_expansion"
    out_dir.mkdir(parents=True, exist_ok=True)

    recursive_policy_path = (
        repo_root
        / "data"
        / "geoscen"
        / "policy"
        / "recursive_policy_response_layer_summary_v1.json"
    )

    if recursive_policy_path.exists():
        with open(recursive_policy_path, "r", encoding="utf-8") as f:
            policy = json.load(f)
    else:
        policy = {}

    us_policy_pressure = float(
        policy.get("policy_response_pressure", REGION_PACKS[0]["policy_pressure"])
        or REGION_PACKS[0]["policy_pressure"]
    )

    region_rows = []

    for item in REGION_PACKS:
        row = item.copy()

        if row["region"] == "US":
            row["policy_pressure"] = us_policy_pressure
            row["recursive_pressure"] = round(
                min(
                    1.0,
                    0.35 * row["policy_pressure"]
                    + 0.20 * row["fx_pressure"]
                    + 0.20 * row["rates_pressure"]
                    + 0.15 * row["growth_fragility"]
                    + 0.10 * row["liquidity_stress"],
                ),
                4,
            )

        row["regional_recursive_score"] = round(
            min(
                1.0,
                0.30 * row["recursive_pressure"]
                + 0.20 * row["policy_pressure"]
                + 0.18 * row["fx_pressure"]
                + 0.15 * row["rates_pressure"]
                + 0.10 * row["growth_fragility"]
                + 0.07 * row["liquidity_stress"],
            ),
            4,
        )

        row["weighted_global_pressure"] = round(
            row["regional_recursive_score"] * row["weight"],
            4,
        )

        row["regional_state"] = classify_region_state(
            row["regional_recursive_score"]
        )

        region_rows.append(row)

    region_df = pd.DataFrame(region_rows)

    score_map = dict(
        zip(
            region_df["region"],
            region_df["regional_recursive_score"],
        )
    )

    bridge_rows = []

    for link in AE_EM_LINKS:
        source = link["source"]
        target = link["target"]

        source_score = float(score_map.get(source, 0.0))
        target_score = float(score_map.get(target, 0.0))
        weight = float(link["weight"])

        bridge_pressure = round(
            min(
                1.0,
                source_score * target_score * weight,
            ),
            4,
        )

        adjusted_target_score = round(
            min(
                1.0,
                target_score + bridge_pressure,
            ),
            4,
        )

        bridge_rows.append(
            {
                "source_region": source,
                "target_region": target,
                "channel": link["channel"],
                "source_score": round(source_score, 4),
                "target_score": round(target_score, 4),
                "bridge_weight": weight,
                "ae_em_bridge_pressure": bridge_pressure,
                "adjusted_target_score": adjusted_target_score,
                "bridge_state": classify_region_state(adjusted_target_score),
            }
        )

    bridge_df = pd.DataFrame(bridge_rows)

    global_recursive_pressure = round(
        float(region_df["weighted_global_pressure"].sum()),
        4,
    )

    avg_bridge_pressure = round(
        float(bridge_df["ae_em_bridge_pressure"].mean()),
        4,
    )

    max_bridge_pressure = round(
        float(bridge_df["ae_em_bridge_pressure"].max()),
        4,
    )

    max_adjusted_region_score = round(
        float(bridge_df["adjusted_target_score"].max()),
        4,
    )

    global_recursive_state = classify_region_state(
        max(
            global_recursive_pressure,
            max_adjusted_region_score,
        )
    )

    summary = {
        "component": "global_recursive_ae_em_expansion_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "region_count": int(len(region_df)),
        "ae_count": int((region_df["block"] == "AE").sum()),
        "em_count": int((region_df["block"] == "EM").sum()),
        "bridge_count": int(len(bridge_df)),
        "global_recursive_pressure": global_recursive_pressure,
        "avg_ae_em_bridge_pressure": avg_bridge_pressure,
        "max_ae_em_bridge_pressure": max_bridge_pressure,
        "max_adjusted_region_score": max_adjusted_region_score,
        "global_recursive_state": global_recursive_state,
        "regional_states": dict(
            zip(
                region_df["region"],
                region_df["regional_state"],
            )
        ),
        "highest_bridge_links": bridge_df.sort_values(
            "ae_em_bridge_pressure",
            ascending=False,
        )
        .head(3)[
            [
                "source_region",
                "target_region",
                "ae_em_bridge_pressure",
            ]
        ]
        .to_dict(orient="records"),
        "status": "global_recursive_ae_em_expansion_complete",
    }

    region_parquet = out_dir / "global_recursive_region_packs_v1.parquet"
    bridge_parquet = out_dir / "global_recursive_ae_em_bridges_v1.parquet"
    region_json = out_dir / "global_recursive_region_packs_v1.json"
    bridge_json = out_dir / "global_recursive_ae_em_bridges_v1.json"
    summary_path = out_dir / "global_recursive_ae_em_expansion_summary_v1.json"

    region_df.to_parquet(region_parquet, index=False)
    bridge_df.to_parquet(bridge_parquet, index=False)

    region_df.to_json(
        region_json,
        orient="records",
        indent=2,
    )

    bridge_df.to_json(
        bridge_json,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Global recursive AE/EM expansion complete")
    print("Regions:", len(region_df))
    print("AE:", summary["ae_count"])
    print("EM:", summary["em_count"])
    print("Global Recursive Pressure:", global_recursive_pressure)
    print("Global Recursive State:", global_recursive_state)
    print("REGION PARQUET:", region_parquet)
    print("BRIDGE PARQUET:", bridge_parquet)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return region_df, bridge_df


if __name__ == "__main__":
    build_global_recursive_ae_em_expansion_v1()
