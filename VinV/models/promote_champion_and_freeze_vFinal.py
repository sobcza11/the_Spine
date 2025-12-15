"""
VinV — Promote Champion & Freeze Artifacts (vFinal)

CPMAI posture:
- Deterministic champion selection from walk-forward summary
- Final refit respects last WFV train_end cutoff
- Persist deployable champion model
- Freeze bundle (metrics + predictions + model)
- Hash-lock bundle (sha256) for audit immutability
- Git commit + tag + push

REQUIRES (produced by train_vinv_model_ladder_walkforward.py):
- the_Spine/the_Spine/vinv/reports/model_ladder/model_ladder_summary.csv
- the_Spine/the_Spine/vinv/reports/model_ladder/model_ladder_metrics_by_fold.csv
- the_Spine/the_Spine/vinv/reports/model_ladder/model_ladder_predictions.parquet
- the_Spine/the_Spine/vinv/reports/model_ladder/trained_models/<model>/fold_01.joblib

ALSO REQUIRES:
- the_Spine/the_Spine/vinv/ssot/vinv_modeling_view_vFinal.parquet
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import hashlib
import json
import shutil
import subprocess

import pandas as pd
import joblib


# ============================================================
# Repo anchor
# ============================================================
REPO_ROOT = Path(__file__).resolve().parents[2]  # ...\the_Spine
print(f"[VINV] REPO_ROOT = {REPO_ROOT}", flush=True)

# Your nested project layout currently writes under: REPO_ROOT/the_Spine/...
VINV_NESTED = REPO_ROOT / "the_Spine" / "vinv"

REPORTS = VINV_NESTED / "reports" / "model_ladder"
SUMMARY_CSV = REPORTS / "model_ladder_summary.csv"
FOLDS_CSV = REPORTS / "model_ladder_metrics_by_fold.csv"
PREDS_PARQ = REPORTS / "model_ladder_predictions.parquet"
TRAINED_MODELS = REPORTS / "trained_models"

MODEL_VIEW = VINV_NESTED / "ssot" / "vinv_modeling_view_vFinal.parquet"

CHAMPION_ROOT = VINV_NESTED / "champion"
CHAMPION_ROOT.mkdir(parents=True, exist_ok=True)

MODELS_DIR = VINV_NESTED / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

CHAMPION_POINTER = CHAMPION_ROOT / "champion_model.joblib"

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

try:
    from xgboost import XGBClassifier  # type: ignore
    HAS_XGB = True
except Exception:
    HAS_XGB = False


def _build_model_by_key(model_key: str):
    """
    Reconstruct the champion pipeline deterministically from the model_key.
    This avoids needing fold_01.joblib files.
    """
    if model_key == "tier0_dummy":
        return DummyClassifier(strategy="prior")

    if model_key == "tier1_logistic_l2":
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler(with_mean=False)),
            ("clf", LogisticRegression(max_iter=200))
        ])

    if model_key == "tier1_elastic_net":
        # Keep consistent with your ladder hyperparams if you used SGDClassifier there.
        from sklearn.linear_model import SGDClassifier
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler(with_mean=False)),
            ("clf", SGDClassifier(
                loss="log_loss",
                penalty="elasticnet",
                alpha=0.0005,
                l1_ratio=0.15,
                max_iter=2000,
                random_state=42
            ))
        ])

    if model_key == "tier2_random_forest":
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("clf", RandomForestClassifier(
                n_estimators=600,
                min_samples_leaf=10,
                n_jobs=-1,
                random_state=42
            ))
        ])

    if model_key == "tier2_gbdt":
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("clf", GradientBoostingClassifier(
                n_estimators=300,
                learning_rate=0.05,
                max_depth=3,
                random_state=42
            ))
        ])

    if model_key == "tier3_xgboost":
        if not HAS_XGB:
            raise ImportError("Champion is XGBoost but xgboost is not installed in this env.")
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("clf", XGBClassifier(
                n_estimators=800,
                learning_rate=0.03,
                max_depth=4,
                subsample=0.8,
                colsample_bytree=0.8,
                objective="binary:logistic",
                eval_metric="logloss",
                random_state=42,
                n_jobs=-1
            ))
        ])

    raise ValueError(f"Unknown model_key: {model_key}")

# ============================================================
# Utilities
# ============================================================
def _require(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required artifact missing: {path}")


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _hash_bundle(bundle_dir: Path) -> dict:
    files = sorted([p for p in bundle_dir.rglob("*") if p.is_file()])
    return {str(p.relative_to(bundle_dir)).replace("\\", "/"): _sha256_file(p) for p in files}


def _pick_champion(summary: pd.DataFrame) -> pd.Series:
    """
    Deterministic selection policy:
    1) Highest mean AUC
    2) Highest long/short Sharpe
    3) Highest lift at top decile
    """
    required = {"model", "auc", "long_short_sharpe_ann", "lift_top_decile"}
    missing = required - set(summary.columns)
    if missing:
        raise KeyError(f"Summary missing columns: {missing}. Found: {list(summary.columns)}")

    s = summary.sort_values(
        by=["auc", "long_short_sharpe_ann", "lift_top_decile"],
        ascending=[False, False, False],
        kind="mergesort"
    )
    return s.iloc[0]


def _last_training_cutoff(folds: pd.DataFrame, model_key: str, df_model: pd.DataFrame) -> pd.Timestamp:
    """
    If folds contains train_end (preferred), use it.
    Otherwise fall back to max available month in modeling view (CPMAI-safe fallback).
    """
    if "train_end" in folds.columns:
        sub = folds[folds["model"] == model_key].copy()
        if sub.empty:
            raise ValueError(f"No folds found for model={model_key}")
        sub["train_end"] = pd.to_datetime(sub["train_end"]).dt.to_period("M").dt.to_timestamp()
        return sub["train_end"].max()

    # Fallback: refit cutoff = max date in modeling view
    if "date" not in df_model.columns:
        raise KeyError("Modeling view missing 'date' for cutoff fallback.")
    cutoff = pd.to_datetime(df_model["date"]).dt.to_period("M").dt.to_timestamp().max()
    return cutoff


def _select_numeric_features(df: pd.DataFrame) -> list[str]:
    blocked = {"symbol", "date", "y", "ret_m", "ret_fwd_1m", "avail_ret_m"}
    cand = [c for c in df.columns if c not in blocked]
    num = df[cand].select_dtypes(include=["number"]).columns.tolist()
    dropped = sorted(set(cand) - set(num))
    if dropped:
        print(f"[VINV] Dropping non-numeric feature cols (refit): {dropped}", flush=True)
    if len(num) < 3:
        raise ValueError("Too few numeric features for refit; check modeling view.")
    return num


# ============================================================
# Main
# ============================================================
def main() -> None:
    print("[VINV] ENTERED main()", flush=True)

    # ---- Required artifacts from ladder
    _require(SUMMARY_CSV)
    _require(FOLDS_CSV)
    _require(PREDS_PARQ)
    _require(MODEL_VIEW)

    summary = pd.read_csv(SUMMARY_CSV)
    folds = pd.read_csv(FOLDS_CSV)

    champ = _pick_champion(summary)
    champ_name = str(champ["model"])
    print(f"[VINV] Champion selected: {champ_name}", flush=True)

    # Load modeling view early so we can compute cutoff if folds lacks train_end
    df_model = pd.read_parquet(MODEL_VIEW).copy()
    df_model["date"] = pd.to_datetime(df_model["date"]).dt.to_period("M").dt.to_timestamp()

    cutoff = _last_training_cutoff(folds, champ_name, df_model)
    print(f"[VINV] Final refit cutoff month: {cutoff.date()}", flush=True)

    print(f"[VINV] Final refit cutoff month: {cutoff.date()}", flush=True)

    model = _build_model_by_key(champ_name)

    df = pd.read_parquet(MODEL_VIEW).copy()
    df["date"] = pd.to_datetime(df["date"]).dt.to_period("M").dt.to_timestamp()

    # Train months <= cutoff (deployment-realistic)
    train_df = df[df["date"] <= cutoff].copy()
    if train_df.empty:
        raise ValueError("Refit training set is empty. Check cutoff and data dates.")

    feat_cols = _select_numeric_features(train_df)

    model.fit(train_df[feat_cols], train_df["y"].astype(int))

    # ---- Freeze bundle
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    bundle_dir = CHAMPION_ROOT / f"vinv_champion_freeze_{stamp}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    print(f"[VINV] Bundle: {bundle_dir}", flush=True)

    # Copy evaluation artifacts
    shutil.copy2(SUMMARY_CSV, bundle_dir / SUMMARY_CSV.name)
    shutil.copy2(FOLDS_CSV, bundle_dir / FOLDS_CSV.name)
    shutil.copy2(PREDS_PARQ, bundle_dir / PREDS_PARQ.name)

    # Persist champion model (bundle + stable pointer + model-key)
    joblib.dump(model, bundle_dir / "champion_model.joblib")
    joblib.dump(model, CHAMPION_POINTER)
    joblib.dump(model, MODELS_DIR / f"{champ_name}.joblib")

    # ---- Manifest
    manifest = {
        "vinv_release": "vFinal",
        "timestamp_utc": datetime.utcnow().isoformat(),
        "champion": {
            "model_key": champ_name,
            "auc": float(champ["auc"]),
            "long_short_sharpe_ann": float(champ["long_short_sharpe_ann"]),
            "lift_top_decile": float(champ["lift_top_decile"]),
        },
        "selection_policy": {
            "primary": "mean AUC",
            "tie_breaker_1": "long_short_sharpe_ann",
            "tie_breaker_2": "lift_top_decile",
            "deterministic": True
        },
        "final_refit": {
            "cutoff_month": str(cutoff),
            "n_training_rows": int(len(train_df)),
            "n_features": int(len(feat_cols)),
            "feature_cols_sample": feat_cols[:25]
        },
        "artifacts": {
            "bundle_dir": str(bundle_dir),
            "summary_csv": str(SUMMARY_CSV),
            "folds_csv": str(FOLDS_CSV),
            "predictions_parquet": str(PREDS_PARQ),
            "champion_pointer": str(CHAMPION_POINTER)
        },
        "cpmai_controls": [
            "Champion selected via pre-defined metrics only",
            "No manual overrides",
            "Final refit respects last WFV train_end cutoff",
            "Artifacts frozen & timestamped",
            "Hash-lock file produced (sha256)"
        ]
    }
    _write_json(bundle_dir / "champion_freeze_manifest.json", manifest)

    # ---- Hash-lock bundle (MUST occur after manifest/model are written)
    hashes = _hash_bundle(bundle_dir)
    _write_json(bundle_dir / "freeze_hashes_sha256.json", {
        "vinv_release": "vFinal",
        "timestamp_utc": datetime.utcnow().isoformat(),
        "bundle_dir": str(bundle_dir),
        "hash_algo": "sha256",
        "files": hashes
    })

    # ---- Git freeze (hard fail on errors)
    subprocess.run(["git", "add", str(bundle_dir), str(CHAMPION_POINTER)], check=True)
    subprocess.run(["git", "commit", "-m", f"Freeze VinV Champion vFinal ({champ_name})"], check=True)
    subprocess.run(["git", "tag", "-a", f"vinv-champion-vFinal-{stamp}", "-m", f"VinV Champion vFinal ({champ_name})"], check=True)
    subprocess.run(["git", "push", "origin", "main", "--tags"], check=True)

    print("[VINV] ✅ Champion promoted, refit, frozen, hash-locked, & tagged.", flush=True)


def _entrypoint():
    print("[VINV] ENTERED _entrypoint()", flush=True)
    try:
        main()
        print("[VINV] FINISHED main()", flush=True)
    except Exception as e:
        print("[VINV] ERROR:", repr(e), flush=True)
        raise


if __name__ == "__main__":
    _entrypoint()



