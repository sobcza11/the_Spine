from pathlib import Path
import pandas as pd

ROOT = Path.cwd()
path = ROOT / "data" / "ism" / "ism_pmi_transp.xlsx"

xls = pd.ExcelFile(path)

print("\nSHEETS:")
for s in xls.sheet_names:
    print("-", s)

for s in xls.sheet_names:
    print("\n" + "="*80)
    print("SHEET:", s)
    df = pd.read_excel(path, sheet_name=s)
    print("SHAPE:", df.shape)
    print("COLUMNS:", df.columns.tolist())
    print(df.head(10))

    