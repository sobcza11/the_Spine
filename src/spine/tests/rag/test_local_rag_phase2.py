from pathlib import Path
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

STORE_PATH = ROOT / "rag" / "local_vector_store.json"
RETRIEVAL_PATH = ROOT / "rag" / "rag_retrieval_results.json"


def load(path: Path) -> dict:
    return json.loads(
        path.read_text(encoding="utf-8")
    )


def test_06_local_vector_store():
    assert STORE_PATH.exists()

    store = load(STORE_PATH)

    assert store["module"] == "local-vector-store"
    assert store["record_count"] > 0
    assert len(store["records"]) == store["record_count"]
    assert store["governance"]["local_only"] is True
    assert store["governance"]["read_only"] is True


def test_07_embedding_generation():
    store = load(STORE_PATH)

    record = store["records"][0]

    assert "embedding" in record
    assert isinstance(record["embedding"], list)
    assert len(record["embedding"]) == 64
    assert all(isinstance(x, float) for x in record["embedding"])


def test_08_document_chunking():
    store = load(STORE_PATH)

    record = store["records"][0]

    assert "chunk_id" in record
    assert "text" in record
    assert len(record["text"]) > 0
    assert len(record["text"]) <= 650


def test_09_citation_enforcement():
    store = load(STORE_PATH)

    for record in store["records"]:
        assert record["citation_id"]
        assert record["source_file"]
        assert record["governance"]["citation_required"] is True


def test_10_geoscen_sovereign_retrieval():
    retrieval = load(RETRIEVAL_PATH)

    geoscen = retrieval["retrieval_sets"]["geoscen_sovereign"]

    assert len(geoscen) > 0
    assert all("citation_id" in x for x in geoscen)
    assert all("source_file" in x for x in geoscen)


def test_11_rbl_contextual_retrieval():
    retrieval = load(RETRIEVAL_PATH)

    rbl = retrieval["retrieval_sets"]["rbl_contextual"]

    assert len(rbl) > 0
    assert all("citation_id" in x for x in rbl)
    assert all("source_file" in x for x in rbl)


def test_12_retrieval_ranking():
    retrieval = load(RETRIEVAL_PATH)

    combined = retrieval["retrieval_sets"]["hybrid_combined"]

    assert len(combined) > 0

    ranks = [x["rank"] for x in combined]

    assert ranks == sorted(ranks)
    assert ranks[0] == 1


def test_13_hybrid_retrieval():
    retrieval = load(RETRIEVAL_PATH)

    assert retrieval["module"] == "hybrid-rag-retrieval"
    assert "geoscen_sovereign" in retrieval["retrieval_sets"]
    assert "rbl_contextual" in retrieval["retrieval_sets"]
    assert "hybrid_combined" in retrieval["retrieval_sets"]

    governance = retrieval["governance"]

    assert governance["deterministic_source_filtering"] is True
    assert governance["semantic_ranking"] is True
    assert governance["citation_required"] is True
    assert governance["read_only"] is True
    assert governance["llm_writeback_allowed"] is False
