"""
run_tranche1_pipeline.py

End-to-end runner for FedSpeak Tranche 1:
- Scrape (if needed)
- Canonical sentences
- VADER tone scoring
- BeigeBook leaf
- Combined policy leaf
- Governance checks
- Print latest Fed narrative
"""

from pathlib import Path

# Import your existing modules
from fed_speak.inputs.scrape_fed_docs import scrape_beige_book
from fed_speak.preprocess.canonical_sentences import build_canonical_from_raw
from fed_speak.tone.vader_tone import build_tone_table
from fed_speak.leaves.fed_leaves import build_beige_leaf
from fed_speak.integration.spine_integration import build_combined_policy_leaf
from fed_speak.integration.governance_checks import run_tranche1_checks
from oraclechambers.narratives.fedspeak_story import generate_latest_fedspeak_story

from fed_speak.config import RAW_BEIGE_DIR, PROCESSED_DIR


def ensure_beige_raw(index_url: str) -> None:
    """
    Scrape Beige Book only if raw parquet does not exist.
    """
    raw_path = RAW_BEIGE_DIR / "beige_raw.parquet"
    if raw_path.exists():
        print(f"[INFO] Beige raw already exists: {raw_path}")
        return
    print("[INFO] Scraping Beige Book index…")
    scrape_beige_book(index_url)
    print("[OK] Beige raw scraped.")


def run_tranche1(index_url: str) -> None:
    """
    Full orchestration for Tranche 1.
    """
    # 1. Scrape BeigeBook (if needed)
    ensure_beige_raw(index_url=index_url)

    # 2. Canonical sentences
    print("[STEP] Building canonical sentences for BeigeBook…")
    from fed_speak.config import RAW_BEIGE_DIR
    canonical_df = build_canonical_from_raw(
        RAW_BEIGE_DIR / "beige_raw.parquet", "BeigeBook"
    )
    print(f"[OK] Canonical rows: {len(canonical_df)}")

    # 3. Tone & VADER
    print("[STEP] Building VADER tone table for BeigeBook…")
    tone_df = build_tone_table("BeigeBook")
    print(f"[OK] Tone rows: {len(tone_df)}")

    # 4. Leaf generation
    print("[STEP] Building BeigeBook leaf…")
    leaf_df = build_beige_leaf()
    print(f"[OK] Leaf rows: {len(leaf_df)}")

    # 5. Combined policy leaf (Spine-facing)
    print("[STEP] Building combined_policy_leaf…")
    combined_df = build_combined_policy_leaf()
    print(f"[OK] Combined policy rows: {len(combined_df)}")

    # 6. Governance checks
    print("[STEP] Running governance checks…")
    run_tranche1_checks()
    print("[OK] Governance checks passed.")

    # 7. Narrative
    print("[STEP] Generating FedSpeak narrative…")
    story = generate_latest_fedspeak_story()
    print("\n=== FedSpeak Narrative (Tranche 1) ===")
    print(story)
    print("======================================\n")


if __name__ == "__main__":
    # You can adjust this URL to the actual Beige Book index page.
    beige_index_url = "https://www.federalreserve.gov/monetarypolicy/beige-book-default.htm"
    run_tranche1(index_url=beige_index_url)

"""
run_tranche1_pipeline.py

End-to-end runner for FedSpeak Tranche 1 (BeigeBook):
- Scrape (if needed)
- Canonical sentences
- VADER tone scoring
- BeigeBook leaf
- Combined policy leaf
- Governance checks
- Print latest Fed narrative
"""

from pathlib import Path

from fed_speak.inputs.scrape_fed_docs import scrape_beige_book
from fed_speak.preprocess.canonical_sentences import build_canonical_from_raw
from fed_speak.tone.vader_tone import build_tone_table
from fed_speak.leaves.fed_leaves import build_beige_leaf
from fed_speak.integration.spine_integration import build_combined_policy_leaf
from fed_speak.integration.governance_checks import run_tranche1_checks
from oraclechambers.narratives.fedspeak_story import generate_latest_fedspeak_story
from fed_speak.config import RAW_BEIGE_DIR


def ensure_beige_raw(index_url: str) -> None:
    """
    Scrape Beige Book only if raw parquet does not exist.
    """
    raw_path = RAW_BEIGE_DIR / "beige_raw.parquet"
    if raw_path.exists():
        print(f"[INFO] Beige raw already exists: {raw_path}")
        return
    print("[INFO] Scraping Beige Book index…")
    scrape_beige_book(index_url)
    print("[OK] Beige raw scraped.")


def run_tranche1(index_url: str) -> None:
    """
    Full orchestration for BeigeBook Tranche 1.
    """
    # 1. Scrape BeigeBook (if needed)
    ensure_beige_raw(index_url=index_url)

    # 2. Canonical sentences
    print("[STEP] Building canonical sentences for BeigeBook…")
    canonical_df = build_canonical_from_raw(
        RAW_BEIGE_DIR / "beige_raw.parquet", "BeigeBook"
    )
    print(f"[OK] Canonical rows: {len(canonical_df)}")

    # 3. Tone & VADER
    print("[STEP] Building VADER tone table for BeigeBook…")
    tone_df = build_tone_table("BeigeBook")
    print(f"[OK] Tone rows: {len(tone_df)}")

    # 4. Leaf generation
    print("[STEP] Building BeigeBook leaf…")
    leaf_df = build_beige_leaf()
    print(f"[OK] Leaf rows: {len(leaf_df)}")

    # 5. Combined policy leaf (Spine-facing)
    print("[STEP] Building combined_policy_leaf…")
    combined_df = build_combined_policy_leaf()
    print(f"[OK] Combined policy rows: {len(combined_df)}")

    # 6. Governance checks
    print("[STEP] Running governance checks…")
    run_tranche1_checks()
    print("[OK] Governance checks passed.")

    # 7. Narrative
    print("[STEP] Generating FedSpeak narrative…")
    story = generate_latest_fedspeak_story()
    print("\n=== FedSpeak Narrative (Tranche 1) ===")
    print(story)
    print("======================================\n")


if __name__ == "__main__":
    # Beige Book index URL (can adjust later if Fed changes structure)
    beige_index_url = "https://www.federalreserve.gov/monetarypolicy/beige-book-default.htm"
    run_tranche1(index_url=beige_index_url)
