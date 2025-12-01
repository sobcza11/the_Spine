"""
beige_lda.py

HKNSL-style topic engine for BeigeBook:

- Use BeigeBook canonical sentences + VADER scores
- Split into Positive / Negative buckets (optional)
- Build Bag of Words with bigrams/trigrams
- Train LDA with coherence scoring
- Export:
    - beige_topics.parquet        (topic per sentence)
    - beige_lda_model/            (gensim LDA model)
    - beige_dict.pkl              (gensim Dictionary)
    - beige_lda_vis.html          (pyLDAvis visualization)
    - beige_topics_rbl.parquet    (top sentences per topic for RBL work)
"""

from pathlib import Path
from typing import List, Tuple, Dict

import pandas as pd
import numpy as np

import gensim
from gensim import corpora
from gensim.models import CoherenceModel

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

from fed_speak.config import PROCESSED_DIR

# Make sure you have nltk data installed at least once:
# nltk.download('punkt')
# nltk.download('stopwords')

try:
    import pyLDAvis
    import pyLDAvis.gensim_models as gensimvis
    HAS_PYLDAVIS = True
except ImportError:
    HAS_PYLDAVIS = False


# -----------------------------
# 1. Loading & basic filtering
# -----------------------------

def load_beige_sentences() -> pd.DataFrame:
    """
    Load BeigeBook canonical sentences joined with tone (VADER) data.
    Expects:
        PROCESSED_DIR / 'BeigeBook' / 'canonical_sentences.parquet'
        PROCESSED_DIR / 'BeigeBook' / 'sentiment_scores.parquet'
    """
    canon_path = PROCESSED_DIR / "BeigeBook" / "canonical_sentences.parquet"
    tone_path = PROCESSED_DIR / "BeigeBook" / "sentiment_scores.parquet"

    df_canon = pd.read_parquet(canon_path)
    df_tone = pd.read_parquet(tone_path)

    # They should have same rows & ordering; if they don't, merge on event_id + sentence_id
    if "sentence_id" in df_canon.columns and "sentence_id" in df_tone.columns:
        df = pd.merge(
            df_canon,
            df_tone[
                [
                    "event_id",
                    "sentence_id",
                    "vader_compound",
                    "tone_leaf",
                ]
            ],
            on=["event_id", "sentence_id"],
            how="inner",
        )
    else:
        # fallback: assume identical index
        df = pd.concat([df_canon, df_tone[["vader_compound", "tone_leaf"]]], axis=1)

    return df


def filter_for_topic_modeling(
    df: pd.DataFrame,
    comp_pos_threshold: float = 0.2,
    comp_neg_threshold: float = -0.2,
    min_len: int = 5,
) -> pd.DataFrame:
    """
    Filter BeigeBook sentences for LDA:
    - Drop very short sentences
    - (Optionally) split into positive / negative buckets, or keep all

    For now, we keep all sentences but store a 'sent_bucket' label.
    """
    df = df.copy()
    df["sent_len"] = df["sentence_text"].str.split().str.len()
    df = df[df["sent_len"] >= min_len]

    def _bucket(comp):
        if comp >= comp_pos_threshold:
            return "positive"
        elif comp <= comp_neg_threshold:
            return "negative"
        return "neutral"

    df["sent_bucket"] = df["vader_compound"].apply(_bucket)
    return df.reset_index(drop=True)


# -----------------------------
# 2. Tokenization & n-grams
# -----------------------------

def tokenize_and_clean(text: str, stop_words: set) -> List[str]:
    """
    Simple tokenizer + lowercasing + stopword removal + alpha filtering.
    """
    tokens = word_tokenize(text.lower())
    tokens = [t for t in tokens if t.isalpha()]
    tokens = [t for t in tokens if t not in stop_words and len(t) > 2]
    return tokens


def build_bigrams_trigrams(
    texts: List[List[str]],
    min_count: int = 5,
    threshold: float = 10.0,
) -> Tuple[gensim.models.Phrases, gensim.models.Phrases]:
    """
    Build bigram and trigram gensim Phrases models.
    """
    bigram = gensim.models.Phrases(texts, min_count=min_count, threshold=threshold)
    trigram = gensim.models.Phrases(bigram[texts], threshold=threshold)

    bigram_mod = gensim.models.phrases.Phraser(bigram)
    trigram_mod = gensim.models.phrases.Phraser(trigram)

    return bigram_mod, trigram_mod


def apply_ngrams(
    texts: List[List[str]],
    bigram_mod: gensim.models.Phrases,
    trigram_mod: gensim.models.Phrases,
) -> List[List[str]]:
    """
    Apply learned bigram + trigram models.
    """
    return [trigram_mod[bigram_mod[doc]] for doc in texts]


# -----------------------------
# 3. LDA + Coherence
# -----------------------------

def build_dictionary_corpus(
    texts: List[List[str]],
) -> Tuple[corpora.Dictionary, List[List[Tuple[int, int]]]]:
    """
    Build gensim dictionary & BoW corpus.
    """
    dictionary = corpora.Dictionary(texts)
    dictionary.filter_extremes(no_below=5, no_above=0.5)
    corpus = [dictionary.doc2bow(text) for text in texts]
    return dictionary, corpus


def train_lda_with_coherence(
    texts: List[List[str]],
    dictionary: corpora.Dictionary,
    corpus: List[List[Tuple[int, int]]],
    k_values: List[int] = None,
) -> Tuple[gensim.models.LdaModel, int, List[Tuple[int, float]]]:
    """
    Train LDA across candidate topic numbers and pick best by coherence.
    """
    if k_values is None:
        k_values = [5, 8, 10, 12, 15]

    best_model = None
    best_k = None
    best_coh = -np.inf
    results: List[Tuple[int, float]] = []

    for k in k_values:
        lda = gensim.models.LdaMulticore(
            corpus=corpus,
            id2word=dictionary,
            num_topics=k,
            random_state=42,
            passes=10,
            workers=2,
            chunksize=2000,
        )
        coherence_model = CoherenceModel(
            model=lda,
            texts=texts,
            dictionary=dictionary,
            coherence="c_v",
        )
        coh = coherence_model.get_coherence()
        results.append((k, coh))
        print(f"[LDA] k={k}, coherence={coh:.4f}")

        if coh > best_coh:
            best_coh = coh
            best_model = lda
            best_k = k

    assert best_model is not None
    print(f"[LDA] Best k={best_k} with coherence={best_coh:.4f}")
    return best_model, best_k, results


# -----------------------------
# 4. Topic assignment & RBL
# -----------------------------

def assign_topics_to_sentences(
    df: pd.DataFrame,
    lda_model: gensim.models.LdaModel,
    corpus: List[List[Tuple[int, int]]],
) -> pd.DataFrame:
    """
    For each sentence (doc), assign dominant topic + probability.
    """
    dominant_topics = []
    topic_probs = []

    for doc_bow in corpus:
        if not doc_bow:
            dominant_topics.append(None)
            topic_probs.append(0.0)
            continue

        topic_dist = lda_model.get_document_topics(doc_bow, minimum_probability=0.0)
        topic_id, prob = max(topic_dist, key=lambda x: x[1])
        dominant_topics.append(topic_id)
        topic_probs.append(prob)

    df = df.copy()
    df["topic_id"] = dominant_topics
    df["topic_prob"] = topic_probs
    return df


def build_rbl_view(df_topics: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """
    'Reading Between the Lines' helper:
    select top N sentences per topic by topic_prob.
    """
    subset_rows = []
    for topic_id, group in df_topics.groupby("topic_id"):
        top_group = group.nlargest(top_n, "topic_prob")
        subset_rows.append(top_group.assign(rbl_rank=range(1, len(top_group) + 1)))
    rbl_df = pd.concat(subset_rows, ignore_index=True)
    return rbl_df


# -----------------------------
# 5. Optional pyLDAvis export
# -----------------------------

def export_pyldavis(
    lda_model: gensim.models.LdaModel,
    corpus: List[List[Tuple[int, int]]],
    dictionary: corpora.Dictionary,
    out_path: Path,
) -> None:
    if not HAS_PYLDAVIS:
        print("[WARN] pyLDAvis not installed; skipping visualization export.")
        return
    vis_data = gensimvis.prepare(lda_model, corpus, dictionary)
    pyLDAvis.save_html(vis_data, str(out_path))
    print(f"[OK] pyLDAvis saved to {out_path}")


# -----------------------------
# 6. Orchestrator
# -----------------------------

def run_beige_lda() -> None:
    """
    End-to-end BeigeBook LDA pipeline:

    1) Load canonical sentences + tone
    2) Filter for LDA
    3) Tokenize & build bigrams/trigrams
    4) Build dictionary + corpus
    5) Train LDA with coherence search
    6) Assign topics to sentences
    7) Save:
        - beige_topics.parquet
        - beige_topics_rbl.parquet
        - dictionary + model
        - pyLDAvis HTML
    """
    out_dir = PROCESSED_DIR / "BeigeBook"
    out_dir.mkdir(parents=True, exist_ok=True)

    print("[STEP] Loading BeigeBook sentences + tone…")
    df = load_beige_sentences()

    print("[STEP] Filtering for LDA…")
    df = filter_for_topic_modeling(df)

    # Tokenization
    print("[STEP] Tokenizing…")
    stop_words = set(stopwords.words("english"))
    texts = [tokenize_and_clean(t, stop_words) for t in df["sentence_text"].tolist()]

    print("[STEP] Building bigrams/trigrams…")
    bigram_mod, trigram_mod = build_bigrams_trigrams(texts)
    texts_ngrams = apply_ngrams(texts, bigram_mod, trigram_mod)

    print("[STEP] Building dictionary & corpus…")
    dictionary, corpus = build_dictionary_corpus(texts_ngrams)

    print("[STEP] Training LDA with coherence search…")
    lda_model, best_k, coh_results = train_lda_with_coherence(
        texts_ngrams, dictionary, corpus
    )

    # Save dictionary & model
    dict_path = out_dir / "beige_dict.pkl"
    model_path = out_dir / "beige_lda_model"
    dictionary.save(str(dict_path))
    lda_model.save(str(model_path))
    print(f"[OK] Dictionary saved to {dict_path}")
    print(f"[OK] LDA model (k={best_k}) saved to {model_path}")

    print("[STEP] Assigning topics to sentences…")
    df_topics = assign_topics_to_sentences(df, lda_model, corpus)
    topics_path = out_dir / "beige_topics.parquet"
    df_topics.to_parquet(topics_path, index=False)
    print(f"[OK] beige_topics.parquet written to {topics_path}")

    print("[STEP] Building RBL view…")
    rbl_df = build_rbl_view(df_topics, top_n=20)
    rbl_path = out_dir / "beige_topics_rbl.parquet"
    rbl_df.to_parquet(rbl_path, index=False)
    print(f"[OK] beige_topics_rbl.parquet written to {rbl_path}")

    # Optional pyLDAvis
    vis_path = out_dir / "beige_lda_vis.html"
    export_pyldavis(lda_model, corpus, dictionary, vis_path)

    print("[DONE] BeigeBook LDA pipeline complete.")


if __name__ == "__main__":
    run_beige_lda()

