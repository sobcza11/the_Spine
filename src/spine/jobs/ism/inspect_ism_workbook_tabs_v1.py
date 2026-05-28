from pathlib import Path

import pandas as pd


def main():
    repo_root = Path.cwd()

    in_path = (
        repo_root
        / "data"
        / "ism"
        / "ism_pmi_transp.xlsx"
    )

    xls = pd.ExcelFile(in_path)

    print("")
    print("AVAILABLE SHEETS:")
    print("=" * 80)

    for s in xls.sheet_names:
        print(s)


if __name__ == "__main__":
    main()
    