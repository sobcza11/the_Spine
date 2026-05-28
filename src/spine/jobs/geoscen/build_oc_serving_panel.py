from pathlib import Path
import pandas as pd

REPO_ROOT = Path.cwd()

INPUTS = [
    REPO_ROOT / "data/geoscen/signals/macro_cb_oc_signals_v1.parquet",
    REPO_ROOT / "data/geoscen/signals/macro_cb_signals_v1.parquet",
    REPO_ROOT / "data/geoscen/signals/geoscen_beige_book_signals_v1.parquet",
    REPO_ROOT / "data/geoscen/pmi/signals/pmi_geoscen_zt_input_v1.parquet",
]

OUTFILE = REPO_ROOT / "data/serving/geoscen/oc_serving_panel.parquet"


def main():
    frames = []

    for path in INPUTS:
        if not path.exists():
            print(f"SKIP | missing: {path}")
            continue

        df = pd.read_parquet(path).copy()
        df["source_file"] = path.name
        frames.append(df)

    if not frames:
        raise FileNotFoundError("No OC input files found.")

    out = pd.concat(frames, ignore_index=True, sort=False)

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.sort_values("date")

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTFILE, index=False)

    print("OK | OC serving panel built")
    print(OUTFILE)
    print(out.shape)


if __name__ == "__main__":
    main()

    