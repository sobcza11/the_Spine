from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib
import math
import re


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "rag"
STORE_PATH = OUT_DIR / "local_vector_store.json"
RETRIEVAL_PATH = OUT_DIR / "rag_retrieval_results.json"


SOURCE_FILES = [
    ROOT / "rbl_agent" / "rbl_grounded_context_bundle.json",
    ROOT / "oraclechambers" / "oc_rbl_local.json",
    ROOT / "oraclechambers" / "oc_contradiction_local.json",
    ROOT / "oraclechambers" / "oc_attention_routing_local.json",
    ROOT / "planes" / "equities_index_plane.json",
    ROOT / "planes" / "rates_plane.json",
    ROOT / "planes" / "fx_plane.json",
    ROOT / "geoscen" / "sovereign_vector_engine.json",
    ROOT / "geoscen" / "sovereign_canonical_layer.json",
    ROOT / "geoscen" / "regional_transmission_systems.json",
]


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}

    return json.loads(
        path.read_text(encoding="utf-8")
    )


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def stable_embedding(text: str, dim: int = 64) -> list[float]:
    """
    Step 7 ? local deterministic embedding generation.
    Not production semantic embeddings yet, but stable & testable.
    """

    text = normalize_text(text)
    vector = [0.0] * dim

    tokens = re.findall(r"[a-zA-Z0-9_]+", text)

    for token in tokens:
        h = int(
            hashlib.sha256(token.encode("utf-8")).hexdigest(),
            16,
        )
        idx = h % dim
        vector[idx] += 1.0

    norm = math.sqrt(sum(x * x for x in vector))

    if norm == 0:
        return vector

    return [x / norm for x in vector]


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def chunk_text(
    text: str,
    chunk_size: int = 650,
    overlap: int = 100,
) -> list[str]:
    """
    Step 8 ? governed context chunking.
    """

    clean = normalize_text(text)

    if not clean:
        return []

    chunks = []
    start = 0

    while start < len(clean):
        end = start + chunk_size
        chunk = clean[start:end].strip()

        if chunk:
            chunks.append(chunk)

        if end >= len(clean):
            break

        start = max(0, end - overlap)

    return chunks


def source_to_text(path: Path, data: dict) -> str:
    return json.dumps(
        {
            "source_file": path.name,
            "content": data,
        },
        indent=2,
        sort_keys=True,
    )


def build_vector_store() -> dict:
    """
    Step 6 ? local vector store.
    """

    records = []

    for path in SOURCE_FILES:
        data = load_json(path)

        if not data:
            continue

        raw_text = source_to_text(path, data)
        chunks = chunk_text(raw_text)

        for i, chunk in enumerate(chunks):
            citation_id = f"{path.name}#chunk-{i}"

            records.append({
                "citation_id": citation_id,
                "source_file": path.name,
                "source_path": str(path),
                "chunk_id": i,
                "text": chunk,
                "embedding": stable_embedding(chunk),
                "governance": {
                    "citation_required": True,
                    "read_only": True,
                    "source_file_required": True,
                },
            })

    return {
        "system": "IsoVector",
        "module": "local-vector-store",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "embedding_model": "stable_hash_embedding_v1",
        "record_count": len(records),
        "records": records,
        "governance": {
            "local_only": True,
            "read_only": True,
            "citation_required": True,
            "llm_writeback_allowed": False,
        },
    }


def enforce_citations(results: list[dict]) -> list[dict]:
    """
    Step 9 ? citation enforcement.
    """

    enforced = []

    for item in results:
        if not item.get("citation_id"):
            continue

        if not item.get("source_file"):
            continue

        enforced.append(item)

    return enforced


def search_store(
    store: dict,
    query: str,
    top_k: int = 5,
    required_terms: list[str] | None = None,
) -> list[dict]:
    query_embedding = stable_embedding(query)

    results = []

    for record in store.get("records", []):
        text = record.get("text", "")
        score = cosine(
            query_embedding,
            record.get("embedding", []),
        )

        if required_terms:
            bonus = sum(
                0.05
                for term in required_terms
                if term.lower() in text
            )
            score += bonus

        results.append({
            "citation_id": record["citation_id"],
            "source_file": record["source_file"],
            "source_path": record["source_path"],
            "chunk_id": record["chunk_id"],
            "score": round(score, 6),
            "text": text[:500],
        })

    ranked = sorted(
        results,
        key=lambda x: x["score"],
        reverse=True,
    )

    return enforce_citations(ranked[:top_k])


def geoscen_sovereign_retrieval(store: dict) -> list[dict]:
    """
    Step 10 ? GeoScen sovereign retrieval.
    """

    return search_store(
        store=store,
        query=(
            "GeoScen sovereign vector country pressure "
            "fragility policy divergence regional transmission"
        ),
        required_terms=[
            "geoscen",
            "sovereign",
            "country",
            "pressure",
            "policy",
        ],
        top_k=6,
    )


def rbl_contextual_retrieval(store: dict) -> list[dict]:
    """
    Step 11 ? RBL contextual retrieval.
    """

    return search_store(
        store=store,
        query=(
            "read between the lines executive attention "
            "cross asset contradiction rates fx equities confidence"
        ),
        required_terms=[
            "rbl",
            "contradiction",
            "equities",
            "rates",
            "fx",
        ],
        top_k=6,
    )


def retrieval_ranking(results: list[dict]) -> list[dict]:
    """
    Step 12 ? retrieval ranking.
    """

    ranked = sorted(
        results,
        key=lambda x: (
            x.get("score", 0),
            x.get("source_file", ""),
        ),
        reverse=True,
    )

    for idx, item in enumerate(ranked):
        item["rank"] = idx + 1

    return ranked


def hybrid_retrieval(store: dict) -> dict:
    """
    Step 13 ? hybrid retrieval:
    deterministic source selection + semantic scoring.
    """

    geoscen = retrieval_ranking(
        geoscen_sovereign_retrieval(store)
    )

    rbl = retrieval_ranking(
        rbl_contextual_retrieval(store)
    )

    combined = retrieval_ranking(
        enforce_citations(
            geoscen[:3] + rbl[:3]
        )
    )

    return {
        "system": "OracleChambers",
        "module": "hybrid-rag-retrieval",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "retrieval_sets": {
            "geoscen_sovereign": geoscen,
            "rbl_contextual": rbl,
            "hybrid_combined": combined,
        },
        "governance": {
            "deterministic_source_filtering": True,
            "semantic_ranking": True,
            "citation_required": True,
            "read_only": True,
            "llm_writeback_allowed": False,
        },
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    store = build_vector_store()

    STORE_PATH.write_text(
        json.dumps(store, indent=2),
        encoding="utf-8",
    )

    retrieval = hybrid_retrieval(store)

    RETRIEVAL_PATH.write_text(
        json.dumps(retrieval, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {STORE_PATH}")
    print(f"Records -> {store['record_count']}")
    print(f"Wrote -> {RETRIEVAL_PATH}")


if __name__ == "__main__":
    main()
