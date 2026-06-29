from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


COUNTRY_MAP = {
    "US": {
        "label": "United States",
        "bank": "Federal Reserve",
        "code": "FED",
        "currency": "USD",
        "document_type": "statement",
        "canonical": "data/geoscen/cb/fed",
        "fallback": "data/geoscen/fomc",
        "output": "us_policy_language_latest.json",
    },
    "EU": {
        "label": "Euro Area",
        "bank": "European Central Bank",
        "code": "ECB",
        "currency": "EUR",
        "document_type": "policy_decision",
        "canonical": "data/geoscen/cb/ecb",
        "fallback": None,
        "output": "eu_policy_language_latest.json",
    },
    "GB": {
        "label": "United Kingdom",
        "bank": "Bank of England",
        "code": "BOE",
        "currency": "GBP",
        "document_type": "mpc",
        "canonical": "data/geoscen/cb/boe",
        "fallback": None,
        "output": "gb_policy_language_latest.json",
    },
    "JP": {
        "label": "Japan",
        "bank": "Bank of Japan",
        "code": "BOJ",
        "currency": "JPY",
        "document_type": "policy_decision",
        "canonical": "data/geoscen/cb/boj",
        "fallback": None,
        "output": "jp_policy_language_latest.json",
    },
    "CA": {
        "label": "Canada",
        "bank": "Bank of Canada",
        "code": "BOC",
        "currency": "CAD",
        "document_type": "rate_decision",
        "canonical": "data/geoscen/cb/boc",
        "fallback": None,
        "output": "ca_policy_language_latest.json",
    },
    "AU": {
        "label": "Australia",
        "bank": "Reserve Bank of Australia",
        "code": "RBA",
        "currency": "AUD",
        "document_type": "statement",
        "canonical": "data/geoscen/cb/rba",
        "fallback": None,
        "output": "au_policy_language_latest.json",
    },
}

TEXT_REQUIRED = {
    "US": True,
    "EU": True,
    "GB": False,
    "JP": False,
    "CA": False,
    "AU": False,
}


DISPLAY_BUCKETS = ["SH", "MH", "N", "MD", "SD"]


DICTIONARY = {
    "SH": [
        "inflation remains elevated",
        "inflation is elevated",
        "upside risks to inflation",
        "price stability",
        "restrictive",
        "tightening",
        "raise interest rates",
        "increase interest rates",
        "higher for longer",
        "inflationary pressures",
        "strong wage growth",
        "persistent inflation",
    ],
    "MH": [
        "inflation has moderated",
        "inflation is moderating",
        "disinflation",
        "gradual easing",
        "less restrictive",
        "policy remains restrictive",
        "monitor inflation",
        "data dependent",
        "hold rates",
        "maintain the target range",
        "maintain rates",
        "unchanged",
    ],
    "N": [
        "maintain",
        "unchanged",
        "stable",
        "balanced",
        "solid pace",
        "changed little",
        "ample reserves",
        "reaffirmed",
        "kept pace",
        "continue",
    ],
    "MD": [
        "growth has slowed",
        "economic activity has slowed",
        "weaker demand",
        "downside risks",
        "softening",
        "labor market has cooled",
        "easing inflation",
        "lower rates",
        "cut rates",
        "reduce interest rates",
    ],
    "SD": [
        "recession",
        "severe",
        "financial stress",
        "crisis",
        "sharp slowdown",
        "material deterioration",
        "significant weakness",
        "emergency",
        "disorderly",
    ],
}


SCORES = {
    "SH": 5,
    "MH": 2,
    "N": 0,
    "MD": -2,
    "SD": -5,
}


def clean_text(text: str) -> str:
    text = str(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def split_sentences(text: str) -> list[str]:
    text = clean_text(text)
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p.strip() for p in parts if len(p.strip()) > 20]


def classify_sentence(sentence: str) -> tuple[str, int, list[str]]:
    s = sentence.lower()

    matches = {}
    for bucket, terms in DICTIONARY.items():
        hits = [term for term in terms if term.lower() in s]
        if hits:
            matches[bucket] = hits

    if not matches:
        return "N", 0, []

    priority = ["SH", "SD", "MH", "MD", "N"]

    for bucket in priority:
        if bucket in matches:
            return bucket, SCORES[bucket], matches[bucket]

    return "N", 0, []


def find_source_file(cfg: dict) -> Path | None:
    paths = []

    canonical = Path(cfg["canonical"])
    if canonical.exists():
        paths.extend(canonical.glob("*.parquet"))

    fallback = cfg.get("fallback")
    if fallback and Path(fallback).exists():
        paths.extend(Path(fallback).glob("*.parquet"))

    if not paths:
        return None

    preferred = [
        p for p in paths
        if cfg["code"].lower() in p.name.lower()
        or cfg["document_type"].lower() in p.name.lower()
        or "combined" in p.name.lower()
        or "canonical" in p.name.lower()
    ]

    return sorted(preferred or paths)[-1]


def load_latest_document(cfg: dict) -> dict:
    source_file = find_source_file(cfg)

    if source_file is None:
        raise FileNotFoundError(f"No parquet corpus found for {cfg['bank']}")

    df = pd.read_parquet(source_file)

    date_col = "date" if "date" in df.columns else None
    text_col = "text" if "text" in df.columns else None

    if date_col is None:
        raise ValueError(f"No date column found in {source_file}")

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])

    if "document_type" in df.columns:
        filtered = df[df["document_type"].astype(str).str.lower() == cfg["document_type"].lower()]
        if not filtered.empty:
            df = filtered

    latest = df.sort_values(date_col).iloc[-1]

    text_value = (
        clean_text(latest[text_col])
        if text_col is not None
        else clean_text(latest.get("title", cfg["document_type"]))
    )

    return {
        "source_file": source_file.name,
        "document_date": latest[date_col].strftime("%Y-%m-%d"),
        "title": str(latest.get("title", cfg["document_type"])),
        "url": str(latest.get("url", "")),
        "text": text_value,
    }


def build_latest(jurisdiction: str) -> None:
    jurisdiction = jurisdiction.upper()
    cfg = COUNTRY_MAP[jurisdiction]

    print("=" * 70)
    print(cfg["bank"])
    print("=" * 70)

    doc = load_latest_document(cfg)
    sentences = split_sentences(doc["text"])

    rows = []
    for i, sentence in enumerate(sentences, start=1):
        bucket, score, matched_terms = classify_sentence(sentence)

        rows.append({
            "jurisdiction": jurisdiction,
            "central_bank": cfg["bank"],
            "document_type": cfg["document_type"],
            "document_date": doc["document_date"],
            "sentence_id": i,
            "sentence": sentence,
            "bucket": bucket,
            "score": score,
            "matched_terms": matched_terms,
        })

    audit = pd.DataFrame(rows)

    counts = audit["bucket"].value_counts().reindex(DISPLAY_BUCKETS, fill_value=0)
    pct = ((counts / max(len(audit), 1)) * 100).round(1)

    coverage = {
        "all_sentences": int(len(audit)),
        "classified_sentences": int(len(audit)),
        "unclassified_sentences": 0,
        "classified_pct": 100.0 if len(audit) else 0.0,
        "unclassified_pct": 0.0,
    }

    display_block = (
        "•|\n"
        + "\n|\n".join(
            f"{pct[bucket]}% {bucket}" if pct[bucket] > 0 else f"-- {bucket}"
            for bucket in DISPLAY_BUCKETS
        )
        + "\n|•"
    )

    output = {
        "lab": "01_RATES_Policy_Language",
        "jurisdiction": jurisdiction,
        "label": cfg["label"],
        "central_bank": cfg["bank"],
        "currency": cfg["currency"],
        "fx_code": cfg["currency"],
        "document_type": cfg["document_type"],
        "document_name": doc["source_file"],
        "document_title": doc["title"],
        "document_url": doc["url"],
        "document_date": doc["document_date"],
        "as_of": doc["document_date"],
        "run_date": datetime.today().strftime("%Y-%m-%d"),
        "policy_language": {
            f"{bucket}_pct": float(pct[bucket])
            for bucket in DISPLAY_BUCKETS
        },
        "counts": {
            bucket: int(counts[bucket])
            for bucket in DISPLAY_BUCKETS
        },
        "coverage": coverage,
        "display_block": display_block,
        "governance": {
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "scope": "latest_central_bank_document_only",
            "method": "deterministic_dictionary_weighted_sentence_classification",
        },
    }

    export_dir = Path("data/serving/rates")
    export_dir.mkdir(parents=True, exist_ok=True)

    json_path = export_dir / cfg["output"]
    audit_path = export_dir / cfg["output"].replace(".json", "_sentences.parquet")

    json_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    audit.to_parquet(audit_path, index=False)

    print(f"Exported JSON: {json_path}")
    print(f"Exported audit: {audit_path}")