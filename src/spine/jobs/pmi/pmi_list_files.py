from pathlib import Path

def main():
    root = Path.cwd()

    print("\n=== PMI / ISM FILES (LOCAL) ===\n")

    pmi_files = []

    for p in root.rglob("*.parquet"):
        name = str(p).lower()
        if "pmi" in name or "ism" in name:
            pmi_files.append(p)

    if not pmi_files:
        print("No PMI / ISM files found.")
        return

    for f in sorted(pmi_files):
        print(f)

    print(f"\nTotal PMI/ISM files: {len(pmi_files)}\n")


if __name__ == "__main__":
    main()

    