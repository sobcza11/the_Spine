"""
Cleanup utility for FedSpeak artifacts.

Removes derived parquet files like:
- beige_topics.parquet
- beige_topics_rbl.parquet
- sentiment_scores.parquet
- combined_policy_leaf.parquet
(and variants across BeigeBook / other Fed categories)

After running this, re-run the FedSpeak pipeline to regenerate.
"""

from pathlib import Path
from typing import List


# Filenames we explicitly want to target
EXACT_NAMES = {
    "beige_topics.parquet",
    "beige_topics_rbl.parquet",
    "sentiment_scores.parquet",
    "combined_policy_leaf.parquet",
}

# Optional patterns for "maybe more" style artifacts
# e.g., if you later have fomc_topics.parquet, sep_topics_rbl.parquet, etc.
SUFFIX_PATTERNS = (
    "_topics.parquet",
    "_topics_rbl.parquet",
)


def get_project_root() -> Path:
    """
    Infer the_OracleChambers root from this file location.

    This assumes the structure:
    the_OracleChambers/
      └─ src/
         └─ fed_speak/
            └─ admin/
               └─ cleanup_fedspeak_artifacts.py  (this file)
    """
    return Path(__file__).resolve().parents[3]


def find_candidate_files(processed_dir: Path) -> List[Path]:
    """
    Scan data/processed recursively for target FedSpeak parquet artifacts.
    """
    candidates: List[Path] = []

    for path in processed_dir.rglob("*.parquet"):
        name = path.name

        # Direct match
        if name in EXACT_NAMES:
            candidates.append(path)
            continue

        # Pattern-based match for *_topics*.parquet
        if any(name.endswith(suffix) for suffix in SUFFIX_PATTERNS):
            candidates.append(path)

    # Deduplicate, just in case
    return sorted(set(candidates))


def delete_files(files: List[Path], dry_run: bool = True) -> None:
    """
    Delete the provided files. If dry_run=True, only prints what would be removed.
    """
    if not files:
        print("[INFO] No matching FedSpeak artifacts found under data/processed.")
        return

    print(f"[INFO] Found {len(files)} FedSpeak artifact(s):")
    for p in files:
        print(f"  - {p}")

    if dry_run:
        print("\n[DRY RUN] No files have been deleted. "
              "Set dry_run=False in main() to actually remove them.")
        return

    print("\n[STEP] Deleting files...")
    for p in files:
        try:
            p.unlink()
            print(f"[OK] Deleted: {p}")
        except Exception as exc:
            print(f"[WARN] Failed to delete {p}: {exc}")


def main() -> None:
    """
    Entry point for cleanup.
    Defaults to a DRY RUN for safety.
    Change dry_run to False when you're ready.
    """
    project_root = get_project_root()
    processed_dir = project_root / "data" / "processed"

    print(f"[INFO] Project root inferred as: {project_root}")
    print(f"[INFO] Scanning under: {processed_dir}")

    files = find_candidate_files(processed_dir)

    # First run: dry run for safety
    delete_files(files, dry_run=True)

    # When you're happy with what it finds, you can:
    # delete_files(files, dry_run=False)


if __name__ == "__main__":
    main()

