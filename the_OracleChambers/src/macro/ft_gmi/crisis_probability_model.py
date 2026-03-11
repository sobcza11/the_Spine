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

