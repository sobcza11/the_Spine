from pathlib import Path
import pandas as pd

REPO_ROOT = Path.cwd()

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

    found_factors = set(factor["factor"].dropna().unique())

    missing = EXPECTED_FACTORS - found_factors
    extra = found_factors - EXPECTED_FACTORS

    dupes = factor.duplicated(["date", "factor"]).sum()

    print("\n=== C-FL FACTOR VALIDATION ===")
    print("rows:", len(factor))
    print("date min:", factor["date"].min())
    print("date max:", factor["date"].max())
    print("factors:", sorted(found_factors))
    print("missing factors:", sorted(missing))
    print("extra factors:", sorted(extra))
    print("duplicate date/factor rows:", dupes)

    print("\n=== FACTOR COUNTS ===")
    print(factor.groupby("factor")["date"].agg(["min", "max", "count"]))

    print("\n=== C-FL ZT LATEST ===")
    print(zt.tail())

    if missing:
        raise ValueError(f"Missing expected factors: {missing}")

    if dupes > 0:
        raise ValueError("Duplicate date/factor rows found.")

    print("\nOK | C-FL factor panel validated")


if __name__ == "__main__":
    main()

    