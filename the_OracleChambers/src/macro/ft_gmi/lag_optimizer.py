import numpy as np
import pandas as pd


LAG_GRID = [5,10,15,20,30,45,60,90,120]


def find_optimal_lag(signal: pd.Series, target: pd.Series):
    
    best_lag = None
    best_corr = -np.inf
    
    for lag in LAG_GRID:
        
        shifted = signal.shift(lag)
        corr = shifted.corr(target)
        
        if corr is None or np.isnan(corr):
            continue
        
        if abs(corr) > abs(best_corr):
            best_corr = corr
            best_lag = lag
    
    return best_lag, best_corr

def compute_lag_correlation(series, target, max_lag=90):

    results = []

    for lag in range(1, max_lag):

        shifted = series.shift(lag)

        corr = shifted.corr(target)

        results.append(
            {
                "lag": lag,
                "correlation": corr,
            }
        )

    df = pd.DataFrame(results)

    best = df.iloc[df["correlation"].abs().idxmax()]

    return df, best

