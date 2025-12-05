from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from src.vinv.vinv_regimes import RegimeConfig


def load_regimes_from_csv(csv_path: str) -> List[RegimeConfig]:
    """
    Load macro regime definitions from a CSV file.

    Expected CSV columns:
        - name   (str, required)
        - start  (optional, ISO date string 'YYYY-MM-DD')
        - end    (optional, ISO date string 'YYYY-MM-DD')

    Example CSV:
        name,start,end
        COVID,2020-02-01,2021-03-31
        POST_GFC,2010-01-01,2019-12-31

    Returns:
        List[RegimeConfig]
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Regime CSV not found at: {path}")

    df = pd.read_csv(path)

    if "name" not in df.columns:
        raise ValueError("Regime CSV must contain a 'name' column.")

    # Optional columns: start, end
    regimes: List[RegimeConfig] = []
    for _, row in df.iterrows():
        name = str(row["name"]).strip()
        start = str(row["start"]).strip() if "start" in df.columns and not pd.isna(row["start"]) else None
        end = str(row["end"]).strip() if "end" in df.columns and not pd.isna(row["end"]) else None

        regimes.append(RegimeConfig(name=name, start=start, end=end))

    return regimes
