import pandas as pd
import sys

path = sys.argv[1]
df = pd.read_parquet(path)

print(df.head(20))
print("\nRows:", len(df))
print("Columns:", df.columns.tolist())
print(df.tail(5))
