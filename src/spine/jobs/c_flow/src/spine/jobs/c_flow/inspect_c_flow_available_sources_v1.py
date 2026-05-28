from pathlib import Path


SOURCE_CANDIDATES = {
    "FX": [
        "data/serving/fx",
        "data/fx",
        "data/processed/fx",
    ],
    "RATES": [
        "data/serving/rates",
        "data/rates",
        "data/processed/rates",
    ],
    "CB": [
        "data/serving/cb",
        "data/cb",
        "data/processed/cb",
        "data/geoscen/llm",
    ],
    "WTI / COMMODITIES": [
        "data/serving/wti",
        "data/wti",
        "data/processed/wti",
        "data/serving/commodities",
        "data/commodities",
        "data/processed/commodities",
    ],
    "COT": [
        "data/serving/cot",
        "data/cot",
        "data/processed/cot",
    ],
    "EQUITIES": [
        "data/serving/equities",
        "data/equities",
        "data/processed/equities",
    ],
    "CREDIT": [
        "data/serving/credit",
        "data/credit",
        "data/processed/credit",
    ],
    "C_FLOW": [
        "data/serving/c_flow",
        "data/c_flow",
        "data/processed/c_flow",
    ],
}


FILE_EXTENSIONS = {".parquet", ".csv", ".json", ".html"}


def list_files(path: Path):
    if not path.exists():
        return []

    files = []
    for p in path.rglob("*"):
        if p.is_file() and p.suffix.lower() in FILE_EXTENSIONS:
            files.append(p)

    return sorted(files)


def main():
    repo_root = Path.cwd()

    print("OK | C_FLOW source inventory v1")
    print(f"repo_root: {repo_root}")
    print("")

    total_found = 0

    for source_name, rel_paths in SOURCE_CANDIDATES.items():
        print("=" * 80)
        print(source_name)
        print("=" * 80)

        source_found = 0

        for rel in rel_paths:
            path = repo_root / rel
            files = list_files(path)

            if not path.exists():
                print(f"[MISSING DIR] {rel}")
                continue

            if not files:
                print(f"[EMPTY DIR]   {rel}")
                continue

            print(f"[FOUND DIR]   {rel}")
            for f in files:
                rel_file = f.relative_to(repo_root)
                print(f"  - {rel_file}")
                source_found += 1
                total_found += 1

        if source_found == 0:
            print(f"[NO FILES FOUND] {source_name}")

        print("")

    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"total candidate files found: {total_found}")

    if total_found == 0:
        print("No C_FLOW source candidates found. Check data directory naming.")
    else:
        print("Inventory complete. Use found files to define C_FLOW v1 source contract.")


if __name__ == "__main__":
    main()