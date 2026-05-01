import os
import pandas as pd

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer

INPUT_PATH = "data/geoscen/cb/macro_cb_canonical_v1.parquet"
OUTPUT_PATH = "data/geoscen/signals/macro_cb_terms_v1.parquet"

MAX_FEATURES = 500
TOP_N_PER_DOC = 25


def clean_text(s):
    return str(s).lower()


def run():
    df = pd.read_parquet(INPUT_PATH).copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["text"] = df["text"].fillna("").astype(str)

    df = df[df["text"].str.len() > 0].copy()
    df = df.reset_index(drop=True)

    vectorizer_count = CountVectorizer(
        stop_words="english",
        max_features=MAX_FEATURES,
        ngram_range=(1, 2),
    )

    vectorizer_tfidf = TfidfVectorizer(
        stop_words="english",
        max_features=MAX_FEATURES,
        ngram_range=(1, 2),
    )

    count_matrix = vectorizer_count.fit_transform(df["text"].map(clean_text))
    tfidf_matrix = vectorizer_tfidf.fit_transform(df["text"].map(clean_text))

    count_terms = vectorizer_count.get_feature_names_out()
    tfidf_terms = vectorizer_tfidf.get_feature_names_out()

    rows = []

    for i, row in df.iterrows():
        count_values = count_matrix[i].toarray().ravel()
        tfidf_values = tfidf_matrix[i].toarray().ravel()

        top_idx = tfidf_values.argsort()[-TOP_N_PER_DOC:][::-1]

        for idx in top_idx:
            term = tfidf_terms[idx]
            count_idx = list(count_terms).index(term) if term in count_terms else None

            rows.append({
                "bank": row["bank"],
                "bank_code": row["bank_code"],
                "currency": row["currency"],
                "date": row["date"],
                "document_type": row["document_type"],
                "title": row["title"],
                "url": row["url"],
                "term": term,
                "tfidf_score": float(tfidf_values[idx]),
                "term_count": int(count_values[count_idx]) if count_idx is not None else 0,
                "term_rank": len(rows),
                "source_layer": "macro_cb_terms_v1",
                "version": "v1",
            })

    out = pd.DataFrame(rows)

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype("string")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("macro_cb terms rows:", len(out))


if __name__ == "__main__":
    run()

