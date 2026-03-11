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

