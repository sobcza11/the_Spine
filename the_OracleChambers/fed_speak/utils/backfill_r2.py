"""
Backfill existing FedSpeak parquet outputs from local disk to R2.

Run once after wiring R2 uploads, or whenever you want to resync
local processed artifacts into the thespine-us-hub bucket.

Usage (from the_OracleChambers root):
    python -m fed_speak.utils.backfill_r2
"""

from pathlib import Path

from fed_speak.utils.r2_upload import upload_to_r2


def main() -> None:
    bucket = "thespine-us-hub"
    base = Path("data/processed")

    # List of (local_path, r2_key) pairs to backfill
    targets = [
        # FedSpeak fusion leaf
        (base / "FedSpeak" / "combined_policy_leaf.parquet",
         "FedSpeak/combined_policy_leaf.parquet"),

        # BeigeBook artifacts
        (base / "BeigeBook" / "beige_topics.parquet",
         "BeigeBook/beige_topics.parquet"),
        (base / "BeigeBook" / "beige_topics_rbl.parquet",
         "BeigeBook/beige_topics_rbl.parquet"),
        (base / "BeigeBook" / "sentiment_scores.parquet",
         "BeigeBook/sentiment_scores.parquet"),

        # FOMC Minutes artifacts (will only upload if present)
        (base / "FOMC_Minutes" / "minutes_leaf.parquet",
         "FOMC_Minutes/minutes_leaf.parquet"),
        (base / "FOMC_Minutes" / "canonical_sentences.parquet",
         "FOMC_Minutes/canonical_sentences.parquet"),
        (base / "FOMC_Minutes" / "sentiment_scores.parquet",
         "FOMC_Minutes/sentiment_scores.parquet"),
    ]

    for local_path, r2_key in targets:
        if local_path.exists():
            print(f"[BACKFILL] Uploading {local_path} -> r2://{bucket}/{r2_key}")
            upload_to_r2(local_path, bucket, r2_key)
        else:
            print(f"[BACKFILL] Skipping {local_path} (file not found)")


if __name__ == "__main__":
    main()

