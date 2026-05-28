from pathlib import Path
import hashlib
import pandas as pd


REPO_ROOT = Path.cwd()

OUTPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_llm_input_v1.parquet"

SOURCES = {
    "pmi_overlay": REPO_ROOT / "data/geoscen/pmi/signals/pmi_commentary_numeric_overlay_v1.parquet",
    "pmi_tags": REPO_ROOT / "data/geoscen/pmi/signals/pmi_commentary_tags_v1.parquet",
    "pmi_zt_input": REPO_ROOT / "data/geoscen/pmi/signals/pmi_geoscen_zt_input_v1.parquet",
    "macro_cb_oc": REPO_ROOT / "data/geoscen/signals/macro_cb_oc_signals_v1.parquet",
    "macro_cb_signals": REPO_ROOT / "data/geoscen/signals/macro_cb_signals_v1.parquet",
    "macro_cb_terms": REPO_ROOT / "data/geoscen/signals/macro_cb_terms_v1.parquet",
    "macro_cb_similarity": REPO_ROOT / "data/geoscen/signals/macro_cb_similarity_v1.parquet",
    "beige_book": REPO_ROOT / "data/geoscen/beige_book/beige_book_combined_canonical_v1.parquet",
    "fomc_minutes": REPO_ROOT / "data/geoscen/fomc/fomc_minutes_clean_v3.parquet",
}


def sha256_text(x: str) -> str:
    return hashlib.sha256(str(x).encode("utf-8", errors="ignore")).hexdigest()


def clean_text(x) -> str:
    if pd.isna(x):
        return ""
    x = str(x)
    replacements = {
        "Ã¢Â€Â“": "–",
        "Ã¢Â€Â”": "—",
        "Ã¢Â€Â™": "'",
        "Ã¢Â€Âœ": '"',
        "Ã¢Â€Â": '"',
        "Ã¯Â»Â¿": "",
        "Ã‚": "",
        "Â": "",
    }
    for bad, good in replacements.items():
        x = x.replace(bad, good)
    return " ".join(x.split())


def safe_col(df, col, default=None):
    return df[col] if col in df.columns else default


def base_frame(df, source_family, source_name, source_path):
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(safe_col(df, "date", pd.NaT), errors="coerce")
    out["source_family"] = source_family
    out["source_name"] = source_name
    out["module"] = "GeoScen"
    out["layer"] = "Tier 2.5"
    out["reporting_role"] = "RBL / explanation only"
    out["zt_reference_date"] = out["date"]
    out["source_path"] = str(source_path)
    out["version"] = "geoscen_llm_input_v1"
    return out


def build_pmi_overlay(path):
    df = pd.read_parquet(path)
    out = base_frame(df, "PMI", "pmi_commentary_numeric_overlay_v1", path)

    out["zt_module"] = "PMI"
    out["sector"] = safe_col(df, "sector", "")
    out["bank"] = ""
    out["document_id"] = ""
    out["numeric_signal_name"] = "pmi_geoscen_commentary"
    out["numeric_signal_value"] = pd.to_numeric(safe_col(df, "Sig", None), errors="coerce")
    out["direction"] = safe_col(df, "direction", "")
    out["confidence"] = pd.to_numeric(safe_col(df, "confidence", None), errors="coerce")
    out["tag_count"] = pd.to_numeric(safe_col(df, "tag_count", None), errors="coerce")
    out["primary_tag"] = ""
    out["secondary_tags"] = ""
    out["text_excerpt"] = safe_col(df, "commentary_text", "").apply(clean_text)
    out["llm_context"] = (
        "PMI commentary for "
        + out["sector"].astype(str)
        + " direction="
        + out["direction"].astype(str)
        + " confidence="
        + out["confidence"].astype(str)
        + ". Comment: "
        + out["text_excerpt"].str[:700]
    )
    return out


def build_pmi_zt(path):
    df = pd.read_parquet(path)
    out = base_frame(df, "PMI", "pmi_geoscen_zt_input_v1", path)

    out["zt_module"] = "PMI"
    out["sector"] = safe_col(df, "sector", "")
    out["bank"] = ""
    out["document_id"] = ""
    out["numeric_signal_name"] = "pmi_geoscen_zt_input"
    out["numeric_signal_value"] = pd.to_numeric(safe_col(df, "pmi_geoscen_zt_input", None), errors="coerce")
    out["direction"] = out["numeric_signal_value"].apply(lambda x: "+" if x > 0 else "-" if x < 0 else "neutral")
    out["confidence"] = pd.to_numeric(safe_col(df, "avg_confidence", None), errors="coerce")
    out["tag_count"] = pd.to_numeric(safe_col(df, "commentary_count", None), errors="coerce")
    out["primary_tag"] = ""
    out["secondary_tags"] = ""
    out["text_excerpt"] = ""
    out["llm_context"] = (
        "PMI GeoScen input for "
        + out["sector"].astype(str)
        + ": score="
        + out["numeric_signal_value"].astype(str)
        + ", confidence="
        + out["confidence"].astype(str)
        + "."
    )
    return out


def build_macro_cb_oc(path):
    df = pd.read_parquet(path)
    out = base_frame(df, "MacroCB", "macro_cb_oc_signals_v1", path)

    out["zt_module"] = "RATES"
    out["sector"] = ""
    out["bank"] = safe_col(df, "bank", "")
    out["document_id"] = ""
    out["numeric_signal_name"] = "tone_diff"
    out["numeric_signal_value"] = pd.to_numeric(safe_col(df, "tone_diff", None), errors="coerce")
    out["direction"] = out["numeric_signal_value"].apply(lambda x: "+" if x > 0 else "-" if x < 0 else "neutral")
    out["confidence"] = ""
    out["tag_count"] = ""
    out["primary_tag"] = "policy_tone"
    out["secondary_tags"] = "uncertainty,policy_divergence"
    out["text_excerpt"] = ""
    out["llm_context"] = (
        out["bank"].astype(str)
        + " OC signal: tone_diff="
        + out["numeric_signal_value"].astype(str)
        + ", policy divergence flag="
        + safe_col(df, "policy_divergence_flag", "").astype(str)
        + "."
    )
    return out


def build_macro_cb_signals(path):
    df = pd.read_parquet(path)
    out = base_frame(df, "MacroCB", "macro_cb_signals_v1", path)

    out["zt_module"] = "RATES"
    out["sector"] = ""
    out["bank"] = safe_col(df, "bank", "")
    out["document_id"] = safe_col(df, "title", "")
    out["numeric_signal_name"] = "policy_tone_score"
    out["numeric_signal_value"] = pd.to_numeric(safe_col(df, "policy_tone_score", None), errors="coerce")
    out["direction"] = out["numeric_signal_value"].apply(lambda x: "+" if x > 0 else "-" if x < 0 else "neutral")
    out["confidence"] = ""
    out["tag_count"] = ""
    out["primary_tag"] = "central_bank_language"
    out["secondary_tags"] = "hawkish,dovish,uncertainty"
    out["text_excerpt"] = safe_col(df, "title", "").apply(clean_text)
    out["llm_context"] = (
        out["bank"].astype(str)
        + " signal: policy_tone_score="
        + out["numeric_signal_value"].astype(str)
        + ", uncertainty_per_1k_words="
        + safe_col(df, "uncertainty_per_1k_words", "").astype(str)
        + "."
    )
    return out


def build_terms(path):
    df = pd.read_parquet(path)
    out = base_frame(df, "MacroCB", "macro_cb_terms_v1", path)

    out["zt_module"] = "RATES"
    out["sector"] = ""
    out["bank"] = safe_col(df, "bank", "")
    out["document_id"] = safe_col(df, "title", "")
    out["numeric_signal_name"] = "tfidf_score"
    out["numeric_signal_value"] = pd.to_numeric(safe_col(df, "tfidf_score", None), errors="coerce")
    out["direction"] = "neutral"
    out["confidence"] = ""
    out["tag_count"] = ""
    out["primary_tag"] = safe_col(df, "term", "").astype(str)
    out["secondary_tags"] = ""
    out["text_excerpt"] = safe_col(df, "term", "").apply(clean_text)
    out["llm_context"] = (
        out["bank"].astype(str)
        + " language term: "
        + out["primary_tag"].astype(str)
        + ", tfidf="
        + out["numeric_signal_value"].astype(str)
        + "."
    )
    return out


def build_similarity(path):
    df = pd.read_parquet(path)
    out = base_frame(df, "MacroCB", "macro_cb_similarity_v1", path)

    out["zt_module"] = "RATES"
    out["sector"] = ""
    out["bank"] = safe_col(df, "bank_code", "")
    out["document_id"] = safe_col(df, "title", "")
    out["numeric_signal_name"] = "similarity_score"
    out["numeric_signal_value"] = pd.to_numeric(safe_col(df, "similarity_score", None), errors="coerce")
    out["direction"] = "neutral"
    out["confidence"] = out["numeric_signal_value"]
    out["tag_count"] = ""
    out["primary_tag"] = "narrative_similarity"
    out["secondary_tags"] = safe_col(df, "comparison_type", "")
    out["text_excerpt"] = safe_col(df, "title", "").apply(clean_text)
    out["llm_context"] = (
        "Central-bank narrative similarity: "
        + out["document_id"].astype(str)
        + " vs "
        + safe_col(df, "match_title", "").astype(str)
        + ", similarity="
        + out["numeric_signal_value"].astype(str)
        + "."
    )
    return out


def build_full_text(path, source_family, source_name, text_col):
    df = pd.read_parquet(path)
    out = base_frame(df, source_family, source_name, path)

    out["zt_module"] = "SYSTEM"
    out["sector"] = ""
    out["bank"] = "Federal Reserve"
    out["document_id"] = safe_col(df, "document_id", "")
    out["numeric_signal_name"] = "text_chars"
    out["numeric_signal_value"] = pd.to_numeric(
        safe_col(df, "text_chars_v3", safe_col(df, "text_chars", None)),
        errors="coerce",
    )
    out["direction"] = "neutral"
    out["confidence"] = ""
    out["tag_count"] = ""
    out["primary_tag"] = source_family.lower()
    out["secondary_tags"] = "full_text"
    out["text_excerpt"] = safe_col(df, text_col, "").apply(clean_text).str[:1500]
    out["llm_context"] = (
        source_family
        + " source document "
        + out["document_id"].astype(str)
        + ": "
        + out["text_excerpt"]
    )
    return out


def main():
    frames = []

    frames.append(build_pmi_overlay(SOURCES["pmi_overlay"]))
    frames.append(build_pmi_zt(SOURCES["pmi_zt_input"]))
    frames.append(build_macro_cb_oc(SOURCES["macro_cb_oc"]))
    frames.append(build_macro_cb_signals(SOURCES["macro_cb_signals"]))
    frames.append(build_terms(SOURCES["macro_cb_terms"]))
    frames.append(build_similarity(SOURCES["macro_cb_similarity"]))
    frames.append(build_full_text(SOURCES["beige_book"], "BeigeBook", "beige_book_combined_canonical_v1", "text"))
    frames.append(build_full_text(SOURCES["fomc_minutes"], "FOMC", "fomc_minutes_clean_v3", "clean_text_v3"))

    out = pd.concat(frames, ignore_index=True)
    
    out["text_excerpt"] = out["text_excerpt"].fillna("").apply(clean_text)
    out["llm_context"] = out["llm_context"].fillna("").apply(clean_text)

    # Normalize parquet-safe column types
    out["confidence"] = pd.to_numeric(out["confidence"], errors="coerce")
    out["tag_count"] = pd.to_numeric(out["tag_count"], errors="coerce")
    out["numeric_signal_value"] = pd.to_numeric(out["numeric_signal_value"], errors="coerce")

    text_cols = [
        "source_family",
        "source_name",
        "module",
        "layer",
        "reporting_role",
        "zt_module",
        "numeric_signal_name",
        "direction",
        "primary_tag",
        "secondary_tags",
        "sector",
        "bank",
        "document_id",
        "text_excerpt",
        "llm_context",
        "source_path",
        "version",
    ]

    for c in text_cols:
        out[c] = out[c].fillna("").astype(str)

    out["row_sha256"] = out["llm_context"].apply(sha256_text)

    cols = [
        "date",
        "source_family",
        "source_name",
        "module",
        "layer",
        "reporting_role",
        "zt_reference_date",
        "zt_module",
        "numeric_signal_name",
        "numeric_signal_value",
        "direction",
        "confidence",
        "tag_count",
        "primary_tag",
        "secondary_tags",
        "sector",
        "bank",
        "document_id",
        "text_excerpt",
        "llm_context",
        "source_path",
        "row_sha256",
        "version",
    ]

    out = out[cols].sort_values(["date", "source_family", "source_name"]).reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("OK | GeoScen LLM input v1")
    print("output:", OUTPUT_PATH)
    print("shape:", out.shape)
    print(out["source_family"].value_counts(dropna=False))
    print(out.tail(10)[["date", "source_family", "source_name", "zt_module", "numeric_signal_name", "numeric_signal_value"]])


if __name__ == "__main__":
    main()

