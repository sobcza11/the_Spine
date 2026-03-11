from __future__ import annotations


def infer_regime_bucket(score: float) -> str:
    if score <= 30:
        return "Stable"
    if score <= 55:
        return "Tension"
    if score <= 75:
        return "Crisis Risk"
    return "Contagion"


def explain_regime(score: float) -> str:
    bucket = infer_regime_bucket(score)
    if bucket == "Stable":
        return "Stress remains contained."
    if bucket == "Tension":
        return "Stress is elevated but not yet acute."
    if bucket == "Crisis Risk":
        return "Stress is high & broadening across macro channels."
    return "Stress is acute with contagion risk elevated."

import pandas as pd
from sklearn.linear_model import LogisticRegression


def build_crisis_model(df):

    df = df.dropna()

    df["crisis_flag"] = (df["ft_gmi_score"].shift(-30) > 70).astype(int)

    features = [
        "ft_gmi_score",
        "ft_gmi_change_1d",
        "ft_gmi_change_5d",
        "stress_acceleration",
    ]

    X = df[features]
    y = df["crisis_flag"]

    model = LogisticRegression()

    model.fit(X, y)

    df["crisis_prob"] = model.predict_proba(X)[:, 1]

    return model, df

