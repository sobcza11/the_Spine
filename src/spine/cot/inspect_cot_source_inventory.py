from pathlib import Path
import pandas as pd


REPO_ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine"
)

COT_ROOTS = [
    REPO_ROOT / "data" / "serving" / "cot",
    REPO_ROOT / "data" / "cot",
    REPO_ROOT / "data" / "serving" / "c_flow",
    REPO_ROOT / "data" / "serving" / "cflow",
    REPO_ROOT / "data" / "serving" / "commflow",
    REPO_ROOT / "data" / "serving" / "flows",
]


def inspect_file(path):

    info = {
        "path": str(path),
        "suffix": path.suffix,
        "exists": path.exists(),
        "readable": False,
        "rows": None,
        "columns": None,
        "sample_columns": None,
        "possible_asset_columns": [],
        "possible_position_columns": [],
    }

    try:
        if path.suffix == ".parquet":
            df = pd.read_parquet(path)
        elif path.suffix == ".csv":
            df = pd.read_csv(path)
        elif path.suffix == ".json":
            df = pd.read_json(path)
        else:
            return info

        info["readable"] = True
        info["rows"] = int(df.shape[0])
        info["columns"] = int(df.shape[1])
        info["sample_columns"] = list(df.columns)[:30]

        lower_cols = {
            str(c).lower(): c
            for c in df.columns
        }

        asset_terms = [
            "asset",
            "symbol",
            "ticker",
            "market",
            "contract",
            "name",
            "commodity",
        ]

        position_terms = [
            "net",
            "position",
            "long",
            "short",
            "spread",
            "commercial",
            "noncommercial",
            "managed",
            "dealer",
        ]

        for lc, original in lower_cols.items():
            if any(term in lc for term in asset_terms):
                info["possible_asset_columns"].append(str(original))

            if any(term in lc for term in position_terms):
                info["possible_position_columns"].append(str(original))

    except Exception as e:
        info["error"] = str(e)

    return info


def run_inventory():

    rows = []

    for root in COT_ROOTS:

        if not root.exists():
            continue

        for path in root.rglob("*"):

            if path.suffix.lower() in [
                ".parquet",
                ".csv",
                ".json",
            ]:
                rows.append(
                    inspect_file(path)
                )

    out = pd.DataFrame(rows)

    export_dir = (
        REPO_ROOT
        / "data"
        / "serving"
        / "cot"
    )

    export_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    csv_path = (
        export_dir
        / "cot_source_inventory_v1.csv"
    )

    out.to_csv(
        csv_path,
        index=False
    )

    return {
        "status": "ok",
        "files": int(out.shape[0]),
        "readable": int(out["readable"].sum()) if not out.empty else 0,
        "output": str(csv_path),
    }


if __name__ == "__main__":

    print(run_inventory())
