from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path.cwd()

READINESS_DIR = REPO_ROOT / "data" / "geoscen" / "readiness"
READINESS_DIR.mkdir(parents=True, exist_ok=True)


REQUIRED_FILES = [
    {
        "label": "GeoScen Overlay",
        "path": "data/serving/geoscen/geoscen_tier1_overlay_v1.parquet",
        "type": "parquet",
        "required_fields": [],
    },
    {
        "label": "FX Serving",
        "path": "data/serving/fx/fx_serving_v2.parquet",
        "type": "parquet",
        "required_fields": [],
    },
    {
        "label": "Rates Serving",
        "path": "data/serving/rates/rates_serving_v2.parquet",
        "type": "parquet",
        "required_fields": [],
    },
    {
        "label": "Breadth Factor Serving",
        "path": "data/serving/equities/breadth_factor_serving_v1.parquet",
        "type": "parquet",
        "required_fields": [],
    },
    {
        "label": "Equities Serving",
        "path": "data/serving/equities/equities_serving_v2.parquet",
        "type": "parquet",
        "required_fields": [],
    },
    {
        "label": "C_FLOW Serving",
        "path": "data/serving/c_flow/c_flow_serving_v5.parquet",
        "type": "parquet",
        "required_fields": [],
    },
    {
        "label": "Credit Serving",
        "path": "data/serving/credit/credit_serving_v1.parquet",
        "type": "parquet",
        "required_fields": [],
    },
    {
        "label": "BTC Futures COT Serving",
        "path": "data/serving/cot/btc_futures_cot_serving_v1.parquet",
        "type": "parquet",
        "required_fields": [],
    },
    {
        "label": "WTI Panel",
        "path": "data/serving/wti/wti_panel.json",
        "type": "json",
        "required_fields": [],
    },
    {
        "label": "OC C_FLOW Panel",
        "path": "data/serving/c_flow/c_flow_panel_v1.html",
        "type": "html",
        "required_fields": [],
    },
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_file_age_days(path: Path) -> float:
    modified_ts = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return (utc_now() - modified_ts).total_seconds() / 86400


def classify_freshness(age_days: float) -> str:
    if age_days <= 7:
        return "OK"
    if age_days <= 30:
        return "WARN"
    return "FAIL"


def load_artifact(path: Path, file_type: str):
    if file_type == "json":
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    if file_type == "parquet":
        return pd.read_parquet(path)

    if file_type == "html":
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    raise ValueError(f"Unsupported artifact type: {file_type}")


def extract_fields(obj) -> list[str]:
    if isinstance(obj, pd.DataFrame):
        return list(obj.columns)

    if isinstance(obj, dict):
        return list(obj.keys())

    if isinstance(obj, str):
        return ["html_text"]

    return []


def detect_placeholder_values(obj) -> list[str]:
    findings = []

    if isinstance(obj, pd.DataFrame):
        if obj.empty:
            findings.append("DATAFRAME_EMPTY")
            return findings

        numeric_cols = obj.select_dtypes(include=np.number).columns.tolist()

        for col in numeric_cols:
            series = obj[col].dropna()

            if len(series) == 0:
                findings.append(f"{col}: EMPTY_NUMERIC")
                continue

            if (series == 0).all():
                findings.append(f"{col}: ALL_ZERO")

            nan_ratio = obj[col].isna().mean()
            if nan_ratio > 0.50:
                findings.append(f"{col}: HIGH_NAN_RATIO")

        object_cols = obj.select_dtypes(include=["object"]).columns.tolist()

        for col in object_cols:
            sample = obj[col].dropna().astype(str)

            if len(sample) == 0:
                continue

            unknown_ratio = sample.str.upper().isin(
                ["UNKNOWN", "PLACEHOLDER", "TBD", "NONE", "NULL"]
            ).mean()

            if unknown_ratio > 0.50:
                findings.append(f"{col}: PLACEHOLDER_TEXT_RATIO_HIGH")

    elif isinstance(obj, dict):
        if not obj:
            findings.append("JSON_EMPTY")
            return findings

        for key, value in obj.items():
            if value in [None, "", "UNKNOWN", "PLACEHOLDER", "TBD"]:
                findings.append(f"{key}: EMPTY_OR_PLACEHOLDER")

            if isinstance(value, (int, float)) and value == 0:
                findings.append(f"{key}: ZERO_VALUE")

            if isinstance(value, list) and len(value) == 0:
                findings.append(f"{key}: EMPTY_LIST")

    elif isinstance(obj, str):
        if len(obj.strip()) == 0:
            findings.append("HTML_EMPTY")

        lowered = obj.lower()

        if "placeholder" in lowered or "tbd" in lowered:
            findings.append("HTML_PLACEHOLDER_TEXT")

    return findings


def validate_artifact(item: dict) -> dict:
    label = item["label"]
    rel_path = item["path"]
    artifact_path = REPO_ROOT / rel_path

    row = {
        "label": label,
        "path": rel_path,
        "exists": False,
        "freshness": "FAIL",
        "age_days": None,
        "missing_fields": [],
        "placeholder_flags": [],
        "status": "FAIL",
    }

    if not artifact_path.exists():
        print(f"[MISSING] {rel_path}")
        return row

    row["exists"] = True

    age_days = round(get_file_age_days(artifact_path), 2)
    freshness = classify_freshness(age_days)

    row["age_days"] = age_days
    row["freshness"] = freshness

    try:
        artifact = load_artifact(artifact_path, item["type"])
        available_fields = extract_fields(artifact)

        missing_fields = [
            field
            for field in item["required_fields"]
            if field not in available_fields
        ]

        placeholder_flags = detect_placeholder_values(artifact)

        row["missing_fields"] = missing_fields
        row["placeholder_flags"] = placeholder_flags

        if freshness == "FAIL":
            row["status"] = "FAIL"
        elif missing_fields:
            row["status"] = "FAIL"
        elif placeholder_flags:
            row["status"] = "WARN"
        else:
            row["status"] = "OK"

    except Exception as exc:
        row["status"] = "FAIL"
        row["placeholder_flags"].append(f"LOAD_ERROR: {exc}")

    print(
        f"[{row['status']}] {rel_path} | "
        f"freshness={row['freshness']} | "
        f"age_days={row['age_days']}"
    )

    return row


def classify_readiness(score: float) -> str:
    if score >= 90:
        return "DEPLOY READY"
    if score >= 75:
        return "MINOR HARDENING REQUIRED"
    if score >= 50:
        return "UNSTABLE"
    return "NOT DEPLOYABLE"


def main() -> None:
    results = [validate_artifact(item) for item in REQUIRED_FILES]

    df_results = pd.DataFrame(results)

    score_map = {
        "OK": 100,
        "WARN": 70,
        "FAIL": 0,
    }

    df_results["score"] = df_results["status"].map(score_map).fillna(0)

    readiness_score = round(float(df_results["score"].mean()), 2)
    readiness_state = classify_readiness(readiness_score)

    summary = {
        "timestamp_utc": utc_now().isoformat(),
        "readiness_score": readiness_score,
        "readiness_state": readiness_state,
        "total_artifacts": int(len(df_results)),
        "ok_count": int((df_results["status"] == "OK").sum()),
        "warn_count": int((df_results["status"] == "WARN").sum()),
        "fail_count": int((df_results["status"] == "FAIL").sum()),
    }

    report_path = READINESS_DIR / "geoscen_tier1_readiness_report.json"
    panel_path = READINESS_DIR / "geoscen_tier1_readiness_panel.parquet"
    summary_path = READINESS_DIR / "geoscen_tier1_readiness_summary.txt"

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "summary": summary,
                "results": results,
            },
            f,
            indent=4,
        )

    df_results.to_parquet(panel_path, index=False)

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("GEOSCEN TIER 1 READINESS SUMMARY\n")
        f.write("=" * 60 + "\n\n")

        for key, value in summary.items():
            f.write(f"{key}: {value}\n")

        f.write("\n")
        f.write(df_results.to_string(index=False))

    print("\n" + "=" * 60)
    print("FINAL READINESS SUMMARY")
    print("=" * 60)
    print(f"READINESS SCORE : {readiness_score}")
    print(f"READINESS STATE : {readiness_state}")
    print(f"OK COUNT        : {summary['ok_count']}")
    print(f"WARN COUNT      : {summary['warn_count']}")
    print(f"FAIL COUNT      : {summary['fail_count']}")

    print("\nArtifacts written:")
    print(report_path)
    print(panel_path)
    print(summary_path)


if __name__ == "__main__":
    main()

    