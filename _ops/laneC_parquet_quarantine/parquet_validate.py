from pathlib import Path
import pandas as pd

try:
    import pyarrow.parquet as pq
except Exception as e:
    pq = None
    print("WARN: pyarrow not available:", e)

lst = Path("_laneC_parquet_list.txt")
paths = [Path(p.strip()) for p in lst.read_text().splitlines() if p.strip()]

out_lines = []
ok = fail = 0

def head_magic(p: Path) -> str:
    try:
        with p.open("rb") as f:
            b = f.read(4)
        return b.decode("ascii", errors="replace")
    except Exception:
        return "????"

for p in paths:
    tag = head_magic(p)
    if tag != "PAR1":
        fail += 1
        out_lines.append(f"FAIL_MAGIC|{tag}|{p}")
        continue

    try:
        if pq is not None:
            _pf = pq.ParquetFile(p)
        df = pd.read_parquet(p)
        ok += 1
        out_lines.append(f"OK|rows={len(df)}|cols={len(df.columns)}|{p}")
    except Exception as e:
        fail += 1
        out_lines.append(f"FAIL_READ|{type(e).__name__}:{e}|{p}")

Path("_laneC_parquet_report.txt").write_text("\n".join(out_lines), encoding="utf-8")
print(f"DONE. OK={ok} FAIL={fail}")
print("Wrote _laneC_parquet_report.txt")
