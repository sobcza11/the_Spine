from pathlib import Path
import pandas as pd


def main():
    repo_root = Path.cwd()
    cot_dir = repo_root / "data" / "cot"

    if not cot_dir.exists():
        raise FileNotFoundError(f"Missing COT directory: {cot_dir}")

    files = sorted(list(cot_dir.glob("*.parquet")) + list(cot_dir.glob("*.csv")))

    print("OK | COT file inspection v1")
    print(f"cot_dir: {cot_dir}")
    print("")

    for path in files:
        print("=" * 80)
        print(path.name)
        print("=" * 80)

        try:
            if path.suffix.lower() == ".parquet":
                df = pd.read_parquet(path)
            else:
                df = pd.read_csv(path, nrows=5000)

            print(f"shape: {df.shape}")
            print("columns:")
            print(list(df.columns))
            print("")
            print(df.head(3).to_string(index=False))
            print("")

            lower_cols = [c.lower() for c in df.columns]
            btc_like = [
                c for c in df.columns
                if "bitcoin" in c.lower()
                or "btc" in c.lower()
                or "cme" in c.lower()
            ]

            if btc_like:
                print("BTC-like columns found:")
                print(btc_like)
            else:
                print("BTC-like columns found: none")

        except Exception as e:
            print(f"ERROR reading {path.name}: {e}")

        print("")


if __name__ == "__main__":
    main()
    