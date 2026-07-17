from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

from spine.jobs.geoscen.eis.contracts import RawArtifactReference, utc_now_iso
from spine.jobs.geoscen.eis.exceptions import RawStorageError
from spine.jobs.geoscen.eis.lineage import sha256_bytes

SAFE_PART_RE = re.compile(r"[^a-zA-Z0-9_.-]+")
MAX_FILENAME_LEN = 120


def preserve_raw_response(
    *,
    provider: str,
    operation: str,
    content: bytes,
    destination: str | os.PathLike[str] | None,
    preserve_raw: bool,
    content_type: str | None = None,
    overwrite: bool = False,
) -> RawArtifactReference | None:
    if not preserve_raw or not destination:
        return None
    try:
        dest = Path(destination)
        dest.mkdir(parents=True, exist_ok=True)
        dest_resolved = dest.resolve()
        sha = sha256_bytes(content)
        filename = _safe_filename(provider, operation, sha or "")
        target = (dest_resolved / filename).resolve()
        if dest_resolved not in target.parents:
            raise RawStorageError("Raw storage path rejected.")
        if target.exists() and not overwrite:
            raise RawStorageError("Raw artifact already exists.")
        fd, tmp_name = tempfile.mkstemp(prefix=f".{filename}.", suffix=".tmp", dir=str(dest_resolved))
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
            os.replace(tmp_name, target)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        return RawArtifactReference(
            path=str(target),
            sha256=sha or "",
            size_bytes=len(content),
            content_type=content_type,
            stored_at=utc_now_iso(),
        )
    except RawStorageError:
        raise
    except Exception as exc:
        raise RawStorageError("Raw response storage failed.", context={"error_type": type(exc).__name__}) from exc


def _safe_filename(provider: str, operation: str, sha256: str) -> str:
    for raw_part in (provider, operation):
        if any(separator in raw_part for separator in ("/", "\\")) or ".." in raw_part:
            raise RawStorageError("Raw storage filename rejected.")
    provider_part = SAFE_PART_RE.sub("-", provider.strip().lower()).strip(".-")
    operation_part = SAFE_PART_RE.sub("-", operation.strip().lower()).strip(".-")
    if not provider_part or not operation_part or ".." in provider_part or ".." in operation_part:
        raise RawStorageError("Raw storage filename rejected.")
    name = f"{provider_part}-{operation_part}-{sha256[:16]}.raw"
    if len(name) > MAX_FILENAME_LEN:
        name = name[: MAX_FILENAME_LEN - 4] + ".raw"
    return name
