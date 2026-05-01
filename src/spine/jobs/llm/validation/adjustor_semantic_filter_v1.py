import re
from typing import Dict, List


NEGATION_PATTERNS = [
    r"\bdoes not\b",
    r"\bdo not\b",
    r"\bdid not\b",
    r"\bnot\b",
    r"\bno\b",
    r"\bwithout\b",
    r"\bunlikely to\b",
    r"\bdoesn’t\b",
    r"\bdon’t\b",
]

PREDICTIVE_PATTERNS = [
    r"\bwill\b",
    r"\bgoing to\b",
    r"\bexpected to\b",
    r"\bforecast\b",
    r"\bpredict\b",
    r"\bprojection\b",
    r"\bprice target\b",
]

SAFE_INDICATIVE_PATTERNS = [
    r"\bis rising\b",
    r"\bis falling\b",
    r"\bhas increased\b",
    r"\bhas decreased\b",
]


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def has_pattern(text: str, patterns: List[str]) -> bool:
    return any(re.search(p, text) for p in patterns)


def classify_sentence(sentence: str) -> Dict:
    s = normalize(sentence)

    has_negation = has_pattern(s, NEGATION_PATTERNS)
    has_predictive = has_pattern(s, PREDICTIVE_PATTERNS)
    has_indicative = has_pattern(s, SAFE_INDICATIVE_PATTERNS)

    if has_predictive and not has_negation:
        return {
            "classification": "violation",
            "reason": "predictive_without_negation"
        }

    if has_predictive and has_negation:
        return {
            "classification": "safe",
            "reason": "negated_prediction"
        }

    if has_indicative:
        return {
            "classification": "safe",
            "reason": "indicative_statement"
        }

    return {
        "classification": "safe",
        "reason": "no_predictive_language"
    }


def evaluate_text(text: str) -> Dict:
    sentences = re.split(r"[.!?]+", text)

    results = []
    violations = 0

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue

        result = classify_sentence(sentence)

        if result["classification"] == "violation":
            violations += 1

        results.append({
            "sentence": sentence,
            **result
        })

    return {
        "total_sentences": len(results),
        "violations": violations,
        "results": results
    }

    