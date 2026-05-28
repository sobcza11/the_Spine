from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


REPO_ROOT = Path.cwd()

OUT_DIR = REPO_ROOT / "data" / "serving" / "geoscen" / "cb"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CB_PATH = REPO_ROOT / "data" / "geoscen" / "signals" / "macro_cb_oc_signals_v1.parquet"

CB_WEIGHTS = {
    "FED": 0.40,
    "ECB": 0.22,
    "BOJ": 0.14,
    "PBOC": 0.14,
    "BOE": 0.10,
}

def build_cb_coverage(latest_rows: pd.DataFrame) -> dict:
    active = set(latest_rows["bank_code"].dropna().astype(str).tolist())
    expected = set(CB_WEIGHTS.keys())

    missing = sorted(expected - active)
    present = sorted(expected & active)

    return {
        "expected_cb_count": len(expected),
        "active_cb_count": len(present),
        "missing_cb_count": len(missing),
        "present_cbs": present,
        "missing_cbs": missing,
        "coverage_ratio": round(len(present) / len(expected), 4),
        "coverage_status": "complete" if not missing else "partial",
    }


def classify_policy(row: pd.Series) -> str:
    tone = float(row.get("policy_tone", 0) or 0)
    uncertainty = float(row.get("uncertainty", 0) or 0)

    if tone >= 40 and uncertainty >= 30:
        return "Hawkish / Uncertain"
    if tone >= 40:
        return "Hawkish"
    if tone <= -20:
        return "Dovish"
    if uncertainty >= 30:
        return "Uncertain"
    return "Neutral / Monitoring"


def main() -> None:
    if not CB_PATH.exists():
        raise FileNotFoundError(f"Missing CB signal file: {CB_PATH}")

    df = pd.read_parquet(CB_PATH).copy()

    required = {"date", "bank_code"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"CB file missing required cols: {missing}")

    df["date"] = pd.to_datetime(df["date"])
    df = df[df["bank_code"].isin(CB_WEIGHTS.keys())].copy()

    latest_rows = (
        df.sort_values("date")
        .groupby("bank_code", as_index=False)
        .tail(1)
        .copy()
    )

    latest_rows["cb_weight"] = latest_rows["bank_code"].map(CB_WEIGHTS).fillna(0.0)
    latest_rows["policy_classification"] = latest_rows.apply(classify_policy, axis=1)
    latest_rows["weighted_policy_tone"] = latest_rows["policy_tone"].fillna(0) * latest_rows["cb_weight"]
    latest_rows["weighted_uncertainty"] = latest_rows["uncertainty"].fillna(0) * latest_rows["cb_weight"]

    global_policy_tone = float(latest_rows["weighted_policy_tone"].sum())
    global_uncertainty = float(latest_rows["weighted_uncertainty"].sum())

    hierarchy = latest_rows.sort_values("cb_weight", ascending=False).to_dict("records")

    coverage = build_cb_coverage(latest_rows)

    payload = {
        "component": "GeoScen Multi-CB Hierarchy",
        "version": "v1",
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "global_policy_tone": round(global_policy_tone, 4),
        "global_uncertainty": round(global_uncertainty, 4),
        "weighted_routing": CB_WEIGHTS,
        "active_cb_count": int(len(latest_rows)),
        "hierarchy": hierarchy,
        "routing_rules": {
            "primary_anchor": "FED",
            "secondary_anchor": "ECB",
            "asia_policy_block": ["BOJ", "PBOC"],
            "uk_policy_block": ["BOE"],
            "higher_weight_cb_moves_first": True,
            "missing_cb_allowed": True,
        },

        "coverage": coverage,

        "governance": {
            "rules_based": True,
            "ai_last": True,
            "explainable": True,
            "weighted_policy_routing": True,
            "source": str(CB_PATH),
        },
    }

    out_json = OUT_DIR / "geoscen_multi_cb_hierarchy_v1.json"
    out_txt = OUT_DIR / "geoscen_multi_cb_hierarchy_v1.txt"
    out_parquet = OUT_DIR / "geoscen_multi_cb_hierarchy_panel_v1.parquet"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    latest_rows.to_parquet(out_parquet, index=False)

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("GEOSCEN MULTI-CB HIERARCHY V1\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"global_policy_tone: {payload['global_policy_tone']}\n")
        f.write(f"global_uncertainty: {payload['global_uncertainty']}\n")
        f.write(f"active_cb_count: {payload['active_cb_count']}\n\n")

        f.write("WEIGHTED ROUTING\n")
        f.write("-" * 60 + "\n")
        for cb, weight in CB_WEIGHTS.items():
            f.write(f"- {cb}: {weight}\n")

        f.write("\nCB COVERAGE\n")
        f.write("-" * 60 + "\n")
        f.write(f"expected_cb_count: {coverage['expected_cb_count']}\n")
        f.write(f"active_cb_count: {coverage['active_cb_count']}\n")
        f.write(f"missing_cb_count: {coverage['missing_cb_count']}\n")
        f.write(f"coverage_ratio: {coverage['coverage_ratio']}\n")
        f.write(f"coverage_status: {coverage['coverage_status']}\n")
        f.write(f"present_cbs: {', '.join(coverage['present_cbs'])}\n")
        f.write(f"missing_cbs: {', '.join(coverage['missing_cbs']) if coverage['missing_cbs'] else 'None'}\n")

        f.write("\nLATEST CB STATES\n")
        f.write("-" * 60 + "\n")
        for row in hierarchy:
            f.write(
                f"- {row.get('bank_code')} | "
                f"{row.get('bank')} | "
                f"{row.get('policy_classification')} | "
                f"tone={row.get('policy_tone')} | "
                f"uncertainty={row.get('uncertainty')} | "
                f"weight={row.get('cb_weight')}\n"
            )

    print("OK | GeoScen Multi-CB Hierarchy v1 built")
    print(f"global_policy_tone : {payload['global_policy_tone']}")
    print(f"global_uncertainty : {payload['global_uncertainty']}")
    print(f"active_cb_count    : {payload['active_cb_count']}")

    print("\nArtifacts written:")
    print(out_json)
    print(out_txt)
    print(out_parquet)


if __name__ == "__main__":
    main()
