from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[4]

FACTOR_FILE = REPO_ROOT / "data/processed/cflow/cfl_factor_panel.parquet"
ZT_FILE = REPO_ROOT / "data/processed/cflow/cfl_zt_panel.parquet"

EXPECTED_FACTORS = {
    "ENERGY",
    "MONETARY_HEDGE",
    "GROWTH_METALS",
    "GRAINS",
}


def main():
    factor = pd.read_parquet(FACTOR_FILE).copy()
    zt = pd.read_parquet(ZT_FILE).copy()

    factor["date"] = pd.to_datetime(factor["date"])
    zt["date"] = pd.to_datetime(zt["date"])

    found = set(factor["factor"].dropna().unique())

    missing = EXPECTED_FACTORS - found
    extra = found - EXPECTED_FACTORS

    dupes = factor.duplicated(["date", "factor"]).sum()

    print("\n=== C-FL FACTOR VALIDATION ===")
    print("rows:", len(factor))
    print("date range:", factor["date"].min(), "to", factor["date"].max())
    print("factors:", sorted(found))
    print("missing:", sorted(missing))
    print("extra:", sorted(extra))
    print("duplicates:", dupes)

    print("\n=== COUNTS ===")
    print(factor.groupby("factor")["date"].count())

    print("\n=== ZT LATEST ===")
    print(zt.tail())

    if missing:
        raise ValueError(f"Missing expected factors: {missing}")
    if dupes > 0:
        raise ValueError("Duplicate rows detected")

    print("\nOK | C-FL factor panel valid")


if __name__ == "__main__":
    main()

    
