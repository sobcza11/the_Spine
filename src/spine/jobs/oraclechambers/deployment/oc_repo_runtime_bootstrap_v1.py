from pathlib import Path
import sys


def bootstrap_repo_root():
    """
    Universal OracleChambers repo-root bootstrap.

    Guarantees:
    - repo-root-safe execution
    - deterministic imports
    - portable runtime execution
    - scheduler-safe execution
    """

    current = Path(__file__).resolve()

    repo_root = None

    for parent in current.parents:
        if (parent / "src").exists() and (parent / "data").exists():
            repo_root = parent
            break

    if repo_root is None:
        raise RuntimeError("Unable to locate repository root.")

    repo_root_str = str(repo_root)

    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)

    return repo_root


if __name__ == "__main__":
    root = bootstrap_repo_root()

    print({
        "artifact": "oc_repo_runtime_bootstrap_v1",
        "repo_root": str(root),
        "bootstrap_ready": True
    })
    