import logging
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from gensim import corpora, models
from gensim.models.coherencemodel import CoherenceModel
from gensim.utils import simple_preprocess

from fed_speak.utils.r2_upload import upload_to_r2  # <-- NEW: R2 upload helper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# Minimal, self-contained English stopword list to avoid NLTK dependency.
# You can expand this later if you want.
EN_STOPWORDS = {
    "a", "about", "above", "after", "again", "against", "all", "almost", "also",
    "am", "an", "and", "any", "are", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both",
    "but", "by",
    "can",
    "did", "do", "does", "doing", "down", "during",
    "each",
    "few", "for", "from", "further",
    "had", "has", "have", "having", "he", "her", "here", "hers", "herself",
    "him", "himself", "his", "how",
    "i", "if", "in", "into", "is", "it", "its", "itself",
    "just",
    "more", "most", "my", "myself",
    "no", "nor", "not", "now",
    "of", "off", "on", "once", "only", "or", "other", "our", "ours",
    "ourselves", "out", "over", "own",
    "same", "she", "should", "so", "some", "such",
    "than", "that", "the", "their", "theirs", "them", "themselves", "then",
    "there", "these", "they", "this", "those", "through", "to", "too",
    "under", "until", "up",
    "very",
    "was", "we", "were", "what", "when", "where", "which", "while", "who",
    "whom", "why", "will", "with",
    "you", "your", "yours", "yourself", "yourselves",
}


def load_canonical_sentences(canonical_path: Path) -> pd.DataFrame:
    """
    Expected minimal columns:
        - event_id
        - sentence_id
        - sentence_text
    Additional columns are preserved but not required.
    """
    df = pd.read_parquet(canonical_path)
    required = {"event_id", "sentence_id", "sentence_text"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Canonical sentences parquet missing required columns: {missing}")
    return df


def simple_clean_tokens(text: str, stop_words: set) -> List[str]:
    """
    Tokenize and lowercase a sentence, filtering out stopwords and very short tokens.
    Uses gensim.simple_preprocess (no NLTK dependency).
    """
    tokens = simple_preprocess(text, deacc=True, min_len=2)
    return [t for t in tokens if t not in stop_words]


def build_corpus(
    sentences: List[str],
    stop_words: set,
) -> Tuple[corpora.Dictionary, List[List[tuple]], List[List[str]]]:
    """
    Returns dictionary, corpus (BoW), and cleaned_docs (list of token lists).
    """
    cleaned_docs = [simple_clean_tokens(s, stop_words) for s in sentences]

    # Filter out empty docs
    cleaned_docs = [doc for doc in cleaned_docs if len(doc) > 0]

    if not cleaned_docs:
        raise ValueError("No non-empty documents after cleaning; check input text or stopword list.")

    dictionary = corpora.Dictionary(cleaned_docs)
    # Basic filtering – tune as needed
    dictionary.filter_extremes(no_below=5, no_above=0.5)
    corpus = [dictionary.doc2bow(doc) for doc in cleaned_docs]

    return dictionary, corpus, cleaned_docs


def train_lda_for_k(
    corpus,
    dictionary,
    cleaned_docs: List[List[str]],
    k_values: List[int],
) -> Tuple[int, models.LdaModel, float]:
    """
    Train LDA for each k and pick the best by coherence.
    Returns (best_k, best_model, best_coherence).
    """
    best_k = None
    best_model = None
    best_coherence = float("-inf")

    logger.info(f"Training LDA models for k in {k_values}...")
    for k in k_values:
        logger.info(f"Training LDA(k={k})...")
        model = models.LdaModel(
            corpus=corpus,
            id2word=dictionary,
            num_topics=k,
            random_state=42,
            passes=10,
            alpha="auto",
            eta="auto",
        )
        coherence_model = CoherenceModel(
            model=model,
            texts=cleaned_docs,
            dictionary=dictionary,
            coherence="c_v",
        )
        coherence = coherence_model.get_coherence()
        logger.info(f"k={k} coherence={coherence:.3f}")

        if coherence > best_coherence:
            best_k = k
            best_model = model
            best_coherence = coherence

    if best_model is None:
        raise RuntimeError("Failed to train any valid LDA model.")

    logger.info(f"Best LDA(k={best_k}) coherence={best_coherence:.3f}")
    return best_k, best_model, best_coherence


def build_topics_parquet(
    df: pd.DataFrame,
    corpus,
    model: models.LdaModel,
    output_path: Path,
) -> pd.DataFrame:
    """
    For each sentence (row in df), attach topic_id and topic_prob of the dominant topic.

    NOTE: For simplicity, we recompute topic distribution for each sentence directly
    from its text, rather than trying to perfectly align df rows with the corpus.
    """
    topic_rows = []
    for idx, row in df.iterrows():
        text = str(row["sentence_text"])
        tokens = simple_clean_tokens(text, EN_STOPWORDS)
        bow = model.id2word.doc2bow(tokens)
        if not bow:
            topic_rows.append((idx, None, 0.0))
            continue

        topic_dist = model.get_document_topics(bow)
        if not topic_dist:
            topic_rows.append((idx, None, 0.0))
            continue

        # Dominant topic
        dominant_topic, prob = max(topic_dist, key=lambda x: x[1])
        topic_rows.append((idx, dominant_topic, float(prob)))

    topic_df = pd.DataFrame(
        topic_rows,
        columns=["_row_idx", "topic_id", "topic_prob"],
    )

    merged = (
        df.reset_index(drop=True)
        .reset_index(names="_row_idx")
        .merge(topic_df, on="_row_idx", how="left")
        .drop(columns=["_row_idx"])
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(output_path, index=False)
    logger.info(f"Saved topics parquet to {output_path}")
    return merged


def build_rbl_parquet(
    topics_df: pd.DataFrame,
    output_path: Path,
    top_n_per_topic: int = 10,
) -> pd.DataFrame:
    """
    RBL = "Reading Between the Lines" – top N sentences per topic.
    Expects topics_df to have: event_id, sentence_id, sentence_text, topic_id, topic_prob.
    """
    required = {"event_id", "sentence_id", "sentence_text", "topic_id", "topic_prob"}
    missing = required - set(topics_df.columns)
    if missing:
        raise ValueError(f"topics_df missing required columns: {missing}")

    # Drop rows without a valid topic assignment
    valid = topics_df.dropna(subset=["topic_id"]).copy()
    if valid.empty:
        raise ValueError("No valid topic assignments found; RBL parquet would be empty.")

    valid["topic_id"] = valid["topic_id"].astype(int)

    rbl_rows = []
    for topic_id, group in valid.groupby("topic_id"):
        top_group = group.sort_values("topic_prob", ascending=False).head(top_n_per_topic)
        for _, row in top_group.iterrows():
            rbl_rows.append(
                {
                    "topic_id": topic_id,
                    "event_id": row["event_id"],
                    "sentence_id": row["sentence_id"],
                    "sentence_text": row["sentence_text"],
                    "topic_prob": row["topic_prob"],
                }
            )

    rbl_df = pd.DataFrame(rbl_rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rbl_df.to_parquet(output_path, index=False)
    logger.info(f"Saved RBL parquet to {output_path}")
    return rbl_df


def main() -> None:
    """
    CLI entry point:
    - loads canonical sentences
    - trains LDA over multiple k
    - writes topics + RBL parquets
    - logs coherence for governance checks
    - uploads parquet outputs to R2 for Power BI consumption
    """
    base_processed = Path("data/processed/BeigeBook")
    canonical_path = base_processed / "canonical_sentences.parquet"
    topics_path = base_processed / "beige_topics.parquet"
    rbl_path = base_processed / "beige_topics_rbl.parquet"

    logger.info(f"Loading canonical sentences from {canonical_path}...")
    df = load_canonical_sentences(canonical_path)

    logger.info("Building corpus...")
    dictionary, corpus, cleaned_docs = build_corpus(
        sentences=df["sentence_text"].astype(str).tolist(),
        stop_words=EN_STOPWORDS,
    )

    logger.info("Training and selecting best LDA model...")
    k_values = [5, 8, 10, 12, 15]
    best_k, best_model, best_coherence = train_lda_for_k(
        corpus=corpus,
        dictionary=dictionary,
        cleaned_docs=cleaned_docs,
        k_values=k_values,
    )
    logger.info(f"Best LDA model has k={best_k} with coherence={best_coherence:.3f}")

    logger.info("Building topics parquet...")
    topics_df = build_topics_parquet(
        df=df,
        corpus=corpus,
        model=best_model,
        output_path=topics_path,
    )

    logger.info("Building RBL parquet...")
    build_rbl_parquet(
        topics_df=topics_df,
        output_path=rbl_path,
        top_n_per_topic=10,
    )

    # Upload Parquet outputs to R2 (parquet-only, space sensitive)
    bucket = "thespine-us-hub"
    upload_to_r2(topics_path, bucket, "BeigeBook/beige_topics.parquet")
    upload_to_r2(rbl_path, bucket, "BeigeBook/beige_topics_rbl.parquet")

    logger.info("BeigeBook LDA pipeline completed successfully.")


if __name__ == "__main__":
    main()

