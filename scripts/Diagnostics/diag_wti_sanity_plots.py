"""
diag_wti_sanity_plots.py
------------------------
Diagnostics & sanity plots for WTI inventory canonical leaf.

Reads:
    data/spine_us/us_wti_inventory_canonical.parquet

Produces:
    exports/wti/wti_inventory_level.png
    exports/wti/wti_inventory_change.png

Checks:
    - Basic shape & head/tail
    - Missing dates / irregular cadence
"""

import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]

CANONICAL_PATH = BASE_DIR / "data" / "spine_us" / "us_wti_inventory_canonical.parquet"
EXPORT_DIR = BASE_DIR / "exports" / "wti"


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def load_canonical() -> pd.DataFrame:
    if not CANONICAL_PATH.exists():
        raise FileNotFoundError(f"WTI canonical leaf not found: {CANONICAL_PATH}")

    print(f"[WTI-DIAG] Loading canonical WTI inventory from {CANONICAL_PATH}")
    df = pd.read_parquet(CANONICAL_PATH)

    if "as_of_date" not in df.columns:
        raise KeyError("Canonical WTI leaf missing 'as_of_date' column.")

    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df = df.sort_values("as_of_date").reset_index(drop=True)
    return df


def check_cadence(df: pd.DataFrame):
    if df.shape[0] < 3:
        print("[WTI-DIAG] Too few rows to meaningfully check cadence.")
        return

    diffs = df["as_of_date"].diff().dt.days.dropna()
    mode_diff = diffs.mode().iloc[0] if not diffs.mode().empty else None

    print(f"[WTI-DIAG] Row count: {df.shape[0]}")
    print(f"[WTI-DIAG] Date range: {df['as_of_date'].min().date()} → {df['as_of_date'].max().date()}")
    print(f"[WTI-DIAG] Most common step size (days): {mode_diff}")

    irregular = diffs[diffs != mode_diff]
    if len(irregular) > 0:
        frac_irregular = len(irregular) / len(diffs)
        print(f"[WTI-DIAG] Irregular steps: {len(irregular)} ({frac_irregular:.1%} of intervals)")
        if frac_irregular > 0.25:
            print("[WTI-DIAG] Warning: more than 25% of date intervals are irregular.")
    else:
        print("[WTI-DIAG] All intervals match the modal step size.")


def plot_inventory_level(df: pd.DataFrame):
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_DIR / "wti_inventory_level.png"

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df["as_of_date"], df["inventory_level"])
    ax.set_title("WTI Inventory Level Over Time")
    ax.set_xlabel("Date")
    ax.set_ylabel("Inventory Level")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

    print(f"[WTI-DIAG] Saved inventory level plot → {out_path}")


def plot_inventory_change(df: pd.DataFrame):
    if "inventory_change" not in df.columns:
        print("[WTI-DIAG] No 'inventory_change' column found; skipping change plot.")
        return

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_DIR / "wti_inventory_change.png"

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(df["as_of_date"], df["inventory_change"])
    ax.set_title("WTI Inventory Weekly Change")
    ax.set_xlabel("Date")
    ax.set_ylabel("Change in Inventory Level")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

    print(f"[WTI-DIAG] Saved inventory change plot → {out_path}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
def main():
    print("\n=== WTI Inventory Diagnostics ===")
    try:
        df = load_canonical()
    except Exception as e:
        print(f"[WTI-DIAG] ERROR loading canonical WTI leaf: {e}")
        sys.exit(1)

    print("[WTI-DIAG] Head:")
    print(df.head())
    print("[WTI-DIAG] Tail:")
    print(df.tail())

    check_cadence(df)
    plot_inventory_level(df)
    plot_inventory_change(df)

    print("=== Done: WTI diagnostics ===\n")


if __name__ == "__main__":
    main()


