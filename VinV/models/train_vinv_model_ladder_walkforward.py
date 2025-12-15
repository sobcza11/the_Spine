"""
VinV Model Ladder (FinTech-grade)
Walk-forward validation + portfolio translation
CPMAI posture: baselines first, leakage-safe pipelines, audit-ready artifacts
"""

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Tuple
from datetime import datetime
import json
import math
import warnings
import joblib
import numpy as np
import pandas as pd

from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss

REPO_ROOT = Path(__file__).resolve().parents[2]  # ...\the_Spine (repo root)
print(f"[VINV] REPO_ROOT = {REPO_ROOT}", flush=True)

warnings.filterwarnings("ignore")

# Optional XGBoost
try:
    from xgboost import XGBClassifier  # type: ignore
    HAS_XGB = True
except Exception:
    HAS_XGB = False



# ============================================================
# Config
# ============================================================
@dataclass(frozen=True)
class Config:
    in_parquet: str = r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\the_Spine\vinv\ssot\vinv_modeling_view_vFinal.parquet"
    out_dir: str = "the_Spine/vinv/reports/model_ladder"
    date_col: str = "date"
    id_cols: Tuple[str, str] = ("symbol", "date")
    y_col: str = "y"
    ret_col: str = "ret_m"

    # With 7 symbols, shorten folds so we can complete today
    n_folds: int = 6
    min_train_months: int = 36
    test_months: int = 6
    step_months: int = 6

    top_q: float = 0.10
    bottom_q: float = 0.10
    long_only_q: float = 0.10


# ============================================================
# Utilities
# ============================================================
def _month_floor(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.to_period("M").dt.to_timestamp()


def _ensure_binary_y(y: pd.Series) -> pd.Series:
    vals = set(pd.unique(y.dropna()))
    if vals <= {0, 1}:
        return y.astype(int)
    if len(vals) == 2:
        hi = max(vals)
        return (y == hi).astype(int)
    raise ValueError(f"y must be binary; found {vals}")


def _select_feature_cols(df: pd.DataFrame, cfg: Config) -> List[str]:
    blocked = {cfg.y_col, cfg.ret_col, *cfg.id_cols}

    # candidate features
    cand = [c for c in df.columns if c not in blocked]

    # keep numeric only (prevents 'T1' style string columns)
    num = df[cand].select_dtypes(include=["number"]).columns.tolist()

    dropped = sorted(set(cand) - set(num))
    if dropped:
        print(f"[VINV] Dropping non-numeric feature cols: {dropped}", flush=True)

    return num


def _walk_forward_splits(months: List[pd.Timestamp], cfg: Config):
    months = sorted(months)
    splits = []
    tr_end = cfg.min_train_months - 1
    te_end = tr_end + cfg.test_months

    while te_end < len(months) and len(splits) < cfg.n_folds:
        splits.append(
            (months[: tr_end + 1],
             months[tr_end + 1 : te_end + 1])
        )
        tr_end += cfg.step_months
        te_end += cfg.step_months

    if not splits:
        raise ValueError("Insufficient history for walk-forward")
    return splits


def _predict_proba(model, X: pd.DataFrame) -> np.ndarray:
    p = model.predict_proba(X)[:, 1]
    return np.clip(p, 1e-6, 1 - 1e-6)


def _lift_at_top_decile(y: np.ndarray, p: np.ndarray, q: float) -> float:
    base = np.mean(y)
    if base <= 0:
        return np.nan
    k = max(1, int(len(p) * q))
    return np.mean(y[np.argsort(-p)[:k]]) / base


def _portfolio_metrics(df: pd.DataFrame, cfg: Config) -> dict:
    long, ls = [], []

    for _, sub in df.groupby(cfg.date_col):
        sub = sub.dropna(subset=[cfg.ret_col, "p_hat"]).sort_values("p_hat", ascending=False)
        if len(sub) < 10:
            continue

        n = len(sub)
        nL = max(1, int(n * cfg.long_only_q))
        nT = max(1, int(n * cfg.top_q))
        nB = max(1, int(n * cfg.bottom_q))

        long.append(sub.head(nL)[cfg.ret_col].mean())
        ls.append(sub.head(nT)[cfg.ret_col].mean() - sub.tail(nB)[cfg.ret_col].mean())

    def sharpe(x):
        if len(x) < 6:
            return np.nan
        return np.mean(x) / np.std(x, ddof=1) * np.sqrt(12)

    return {
        "long_only_mean_m": np.mean(long) if long else np.nan,
        "long_short_mean_m": np.mean(ls) if ls else np.nan,
        "long_only_sharpe_ann": sharpe(long),
        "long_short_sharpe_ann": sharpe(ls),
    }


def _df_to_md_table(df: pd.DataFrame) -> str:
    d = df.copy()
    for c in d.columns:
        if pd.api.types.is_float_dtype(d[c]):
            d[c] = d[c].map(lambda x: f"{x:.4f}" if pd.notna(x) else "")
        else:
            d[c] = d[c].astype(str)

    hdr = "| " + " | ".join(d.columns) + " |"
    sep = "| " + " | ".join(["---"] * len(d.columns)) + " |"
    rows = ["| " + " | ".join(r) + " |" for r in d.values]
    return "\n".join([hdr, sep] + rows)


# ============================================================
# Models
# ============================================================
def _make_models() -> Dict[str, object]:
    models = {
        "tier0_dummy": DummyClassifier(strategy="prior"),

        "tier1_logistic_l2": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler(with_mean=False)),
            ("clf", LogisticRegression(max_iter=200))
        ]),

        "tier1_elastic_net": Pipeline([
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
        ]),

        "tier2_random_forest": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("clf", RandomForestClassifier(
                n_estimators=600,
                min_samples_leaf=10,
                n_jobs=-1,
                random_state=42
            ))
        ]),

        "tier2_gbdt": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("clf", GradientBoostingClassifier(
                n_estimators=300,
                learning_rate=0.05,
                max_depth=3,
                random_state=42
            ))
        ])
    }

    if HAS_XGB:
        models["tier3_xgboost"] = Pipeline([
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

    return models


# ============================================================
# Main
# ============================================================
def main():
    print("[VINV] ENTERED main()", flush=True)
    cfg = Config()
    OUT = REPO_ROOT / "the_Spine" / "vinv" / "reports" / "model_ladder"
    OUT.mkdir(parents=True, exist_ok=True)
    print(f"[VINV] OUT = {OUT}", flush=True)

    df = pd.read_parquet(cfg.in_parquet)
    df[cfg.date_col] = _month_floor(df[cfg.date_col])
    df[cfg.y_col] = _ensure_binary_y(df[cfg.y_col])

    feats = _select_feature_cols(df, cfg)
    months = sorted(df[cfg.date_col].unique())
    splits = _walk_forward_splits(months, cfg)

    models = _make_models()
    rows, preds = [], []

    for name, model in models.items():
        for i, (tr_m, te_m) in enumerate(splits, 1):
            tr = df[df[cfg.date_col].isin(tr_m)]
            te = df[df[cfg.date_col].isin(te_m)]

            model.fit(tr[feats], tr[cfg.y_col])
            p = _predict_proba(model, te[feats])

            row = {
                "model": name,
                "fold": i,
                "auc": roc_auc_score(te[cfg.y_col], p),
                "logloss": log_loss(te[cfg.y_col], p),
                "brier": brier_score_loss(te[cfg.y_col], p),
                "lift_top_decile": _lift_at_top_decile(te[cfg.y_col].values, p, 0.10),
            }

            te2 = te[[cfg.date_col, cfg.ret_col]].copy()
            te2["p_hat"] = p
            row.update(_portfolio_metrics(te2, cfg))

            rows.append(row)

            p_out = te[[cfg.id_cols[0], cfg.id_cols[1], cfg.y_col, cfg.ret_col]].copy()
            p_out["model"] = name
            p_out["fold"] = i
            p_out["p_hat"] = p
            preds.append(p_out)

    mdf = pd.DataFrame(rows)
    pdf = pd.concat(preds)

    mdf.to_csv(OUT / "model_ladder_metrics_by_fold.csv", index=False)
    pdf.to_parquet(OUT / "model_ladder_predictions.parquet", index=False)

    summary = mdf.groupby("model", as_index=False).mean(numeric_only=True)
    summary.to_csv(OUT / "model_ladder_summary.csv", index=False)

    md = [
        "# VinV — Model Ladder Results (Walk-Forward)",
        "",
        f"Run UTC: {datetime.utcnow().isoformat()}",
        "",
        _df_to_md_table(summary)
    ]
    (OUT / "VinV_Model_Ladder_Results.md").write_text("\n".join(md))

    print("DONE — VinV model ladder complete.")



    # ADD inside main(), after OUT.mkdir(...)
    MODEL_OUT = OUT / "trained_models"
    MODEL_OUT.mkdir(parents=True, exist_ok=True)

    # ... inside your loops, RIGHT AFTER:
    # model.fit(tr[feats], tr[cfg.y_col])

    # PERSIST per-fold trained model (audit-ready)
    m_dir = MODEL_OUT / name
    m_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, m_dir / f"fold_{i:02d}.joblib")
    
    if __name__ == "__main__":
        main()

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
