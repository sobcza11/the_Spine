import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _load_parquet(path: Path, label: str) -> Optional[pd.DataFrame]:
    if not path.exists():
        logger.warning(f"{label} parquet not found at {path}")
        return None
    df = pd.read_parquet(path)
    logger.info(f"Loaded {label} parquet from {path} with {len(df)} rows.")
    return df


# -------------------------------
# BeigeBook governance (existing)
# -------------------------------

def check_beige_canonical_and_tone(
    canonical_path: Path,
    tone_path: Path,
) -> None:
    canonical = _load_parquet(canonical_path, "canonical_sentences")
    tone = _load_parquet(tone_path, "sentiment_scores")
    if canonical is None or tone is None:
        logger.warning("Skipping BeigeBook canonical/tone checks (missing files).")
        return

    if len(canonical) != len(tone):
        logger.error(
            f"Row-count mismatch: canonical={len(canonical)} vs tone={len(tone)}"
        )
        raise AssertionError("BeigeBook canonical vs tone row-count mismatch.")

    key_cols = ["event_id", "sentence_id"]
    if not set(key_cols).issubset(canonical.columns) or not set(key_cols).issubset(
        tone.columns
    ):
        logger.error(f"Missing key columns {key_cols} in canonical or tone parquet.")
        raise AssertionError("Key columns missing for BeigeBook canonical/tone.")

    merged = canonical[key_cols].merge(tone[key_cols], on=key_cols, how="outer", indicator=True)
    if (merged["_merge"] != "both").any():
        logger.error("Mismatched keys between canonical and tone parquets.")
        raise AssertionError("Key mismatch between canonical and tone parquets.")

    logger.info("BeigeBook canonical/tone governance checks passed.")


def check_beige_topics(
    canonical_path: Path,
    topics_path: Path,
) -> None:
    canonical = _load_parquet(canonical_path, "canonical_sentences")
    topics = _load_parquet(topics_path, "beige_topics")
    if canonical is None or topics is None:
        logger.warning("Skipping BeigeBook topics checks (missing files).")
        return

    if "topic_id" not in topics.columns or "topic_prob" not in topics.columns:
        logger.error("Topics parquet missing topic_id or topic_prob column.")
        raise AssertionError("Invalid schema for beige_topics parquet.")

    # Basic NaN checks
    if topics["topic_id"].isna().mean() > 0.10:
        logger.warning(
            f"More than 10% of rows have NaN topic_id in beige_topics "
            f"({topics['topic_id'].isna().mean():.2%})."
        )

    if topics["topic_prob"].isna().any():
        logger.error("NaNs found in topic_prob column.")
        raise AssertionError("NaNs in topic_prob column for beige_topics parquet.")

    # Row-count sanity: allow a small mismatch but flag if large
    canonical_n = len(canonical)
    topics_n = len(topics)
    row_ratio = topics_n / max(canonical_n, 1)
    if row_ratio < 0.90 or row_ratio > 1.10:
        logger.warning(
            f"Row-count ratio between canonical and topics is unusual: "
            f"{row_ratio:.3f} (canonical={canonical_n}, topics={topics_n})"
        )

    logger.info("BeigeBook topics governance checks passed.")


def check_beige_rbl(
    rbl_path: Path,
) -> None:
    rbl = _load_parquet(rbl_path, "beige_topics_rbl")
    if rbl is None:
        logger.warning("Skipping RBL checks (missing file).")
        return

    required = {"topic_id", "event_id", "sentence_id", "sentence_text", "topic_prob"}
    missing = required - set(rbl.columns)
    if missing:
        logger.error(f"RBL parquet missing required columns: {missing}")
        raise AssertionError("Invalid schema for beige_topics_rbl parquet.")

    if rbl["topic_prob"].isna().any():
        logger.error("NaNs found in topic_prob column in RBL parquet.")
        raise AssertionError("NaNs in topic_prob column for RBL parquet.")

    logger.info("BeigeBook RBL governance checks passed.")


def run_all_beige_checks(base_processed: Path) -> None:
    """
    Convenience entry-point for BeigeBook governance:
    - canonical vs tone
    - topics parquet
    - RBL parquet
    """
    canonical_path = base_processed / "canonical_sentences.parquet"
    tone_path = base_processed / "sentiment_scores.parquet"
    topics_path = base_processed / "beige_topics.parquet"
    rbl_path = base_processed / "beige_topics_rbl.parquet"

    logger.info("Running BeigeBook governance checks...")
    check_beige_canonical_and_tone(canonical_path, tone_path)
    check_beige_topics(canonical_path, topics_path)
    check_beige_rbl(rbl_path)
    logger.info("All BeigeBook governance checks completed.")


# --------------------------------
# NEW: FOMC Minutes governance
# --------------------------------

def check_minutes_canonical_and_tone(
    canonical_path: Path,
    tone_path: Path,
) -> None:
    """
    Ensure minutes canonical sentences and sentence-level sentiment align.
    """
    canonical = _load_parquet(canonical_path, "minutes_canonical_sentences")
    tone = _load_parquet(tone_path, "minutes_sentiment_scores")
    if canonical is None or tone is None:
        logger.warning("Skipping FOMC Minutes canonical/tone checks (missing files).")
        return

    key_cols = ["event_id", "sentence_id"]

    if not set(key_cols).issubset(canonical.columns) or not set(key_cols).issubset(
        tone.columns
    ):
        logger.error(f"Missing key columns {key_cols} in minutes canonical or tone parquet.")
        raise AssertionError("Key columns missing for FOMC Minutes canonical/tone.")

    merged = canonical[key_cols].merge(
        tone[key_cols], on=key_cols, how="outer", indicator=True
    )
    if (merged["_merge"] != "both").any():
        logger.error("Mismatched keys between minutes canonical and tone parquets.")
        raise AssertionError("Key mismatch between minutes canonical and tone parquets.")

    logger.info("FOMC Minutes canonical/tone governance checks passed.")


def check_minutes_leaf(
    leaf_path: Path,
) -> None:
    """
    Validate the event-level FOMC Minutes leaf.

    Expected columns (from fomc_minutes_leaf.py):
        - event_id
        - n_sentences
        - share_hawkish
        - share_dovish
        - share_neutral
        - mean_compound
        - inflation_risk
        - growth_risk
        - policy_bias
    """
    leaf = _load_parquet(leaf_path, "minutes_leaf")
    if leaf is None:
        logger.warning("Skipping FOMC Minutes leaf checks (missing file).")
        return

    required = {
        "event_id",
        "n_sentences",
        "share_hawkish",
        "share_dovish",
        "share_neutral",
        "mean_compound",
        "inflation_risk",
        "growth_risk",
        "policy_bias",
    }
    missing = required - set(leaf.columns)
    if missing:
        logger.error(f"Minutes leaf parquet missing required columns: {missing}")
        raise AssertionError("Invalid schema for minutes_leaf parquet.")

    if leaf["n_sentences"].le(0).any():
        logger.warning("Some FOMC Minutes events have n_sentences <= 0.")

    for col in [
        "share_hawkish",
        "share_dovish",
        "share_neutral",
        "mean_compound",
        "inflation_risk",
        "growth_risk",
        "policy_bias",
    ]:
        if leaf[col].isna().any():
            logger.error(f"NaNs found in column {col} in minutes_leaf parquet.")
            raise AssertionError(f"NaNs in {col} column for minutes_leaf parquet.")

    logger.info("FOMC Minutes leaf governance checks passed.")


def run_all_minutes_checks(base_processed: Path) -> None:
    """
    Convenience entry-point for FOMC Minutes governance.
    """
    canonical_path = base_processed / "canonical_sentences.parquet"
    tone_path = base_processed / "sentiment_scores.parquet"
    leaf_path = base_processed / "minutes_leaf.parquet"

    logger.info("Running FOMC Minutes governance checks...")
    check_minutes_canonical_and_tone(canonical_path, tone_path)
    check_minutes_leaf(leaf_path)
    logger.info("All FOMC Minutes governance checks completed.")


# ---------------------------
# FedSpeak-wide entry point
# ---------------------------

def main() -> None:
    """
    Run all FedSpeak governance checks we currently support:
        - BeigeBook (tone + topics + RBL)
        - FOMC Minutes (tone + leaf)
    """
    beige_base = Path("data/processed/BeigeBook")
    minutes_base = Path("data/processed/FOMC_Minutes")

    run_all_beige_checks(beige_base)
    run_all_minutes_checks(minutes_base)


if __name__ == "__main__":
    main()

