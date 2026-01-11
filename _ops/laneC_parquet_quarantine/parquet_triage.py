from pathlib import Path

paths = [Path(p.strip()) for p in Path("_laneC_parquet_list.txt").read_text().splitlines() if p.strip()]
out = []

def read_head(p, n=8):
    try:
        with p.open("rb") as f:
            return f.read(n)
    except Exception:
        return b""

def read_tail(p, n=8):
    try:
        sz = p.stat().st_size
        if sz < n:
            return b""
        with p.open("rb") as f:
            f.seek(-n, 2)
            return f.read(n)
    except Exception:
        return b""

for p in paths:
    try:
        sz = p.stat().st_size
    except Exception:
        sz = -1

    head = read_head(p, 4)
    tail = read_tail(p, 4)

    head_tag = head.decode("ascii", errors="replace")
    tail_tag = tail.decode("ascii", errors="replace")

    if sz == 0:
        status = "ZERO_BYTES"
    elif head_tag != "PAR1":
        status = f"NOT_PARQUET_HEAD({head_tag})"
    elif tail_tag != "PAR1":
        status = f"BAD_FOOTER({tail_tag})"
    else:
        status = "HEADER+FOOTER_OK"

    out.append(f"{status}|bytes={sz}|{p}")

Path("_laneC_parquet_triage.txt").write_text("\n".join(out), encoding="utf-8")
print("Wrote _laneC_parquet_triage.txt")
