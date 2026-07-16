from __future__ import annotations

import json
from pathlib import Path


REQUIRED_SOURCES = {
    "ism": Path("data/geoscen/pmi"),
    "beige_book": Path("data/geoscen/beige_book"),
    "central_banks": Path("data/geoscen/cb"),
}


def audit_sources(repo_root: Path) -> dict[str, object]:
    sources: dict[str, object] = {}

    for source_id, relative_path in REQUIRED_SOURCES.items():
        path = repo_root / relative_path

        sources[source_id] = {
            "path": relative_path.as_posix(),
            "exists": path.exists(),
            "file_count": (
                sum(1 for item in path.rglob("*") if item.is_file())
                if path.exists()
                else 0
            ),
        }

    return {
        "component": "institutional_evidence",
        "sources": sources,
        "ready": all(
            source["exists"]
            for source in sources.values()
        ),
    }


def main() -> int:
    repo_root = Path(__file__).resolve().parents[7]
    result = audit_sources(repo_root)
    print(json.dumps(result, indent=2))
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
