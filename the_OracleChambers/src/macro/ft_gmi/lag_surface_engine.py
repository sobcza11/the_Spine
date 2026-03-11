from .lag_optimizer import compute_lag_correlation


def build_lag_surface(df):

    results = {}

    target = df["ft_gmi_score"].shift(-30)

    for col in [
        "rates_stress",
        "fx_stress",
        "energy_stress",
        "equity_stress",
        "credit_stress",
    ]:

        lag_df, best = compute_lag_correlation(df[col], target)

        results[col] = {
            "best_lag": int(best["lag"]),
            "corr": float(best["correlation"]),
        }

    return results


