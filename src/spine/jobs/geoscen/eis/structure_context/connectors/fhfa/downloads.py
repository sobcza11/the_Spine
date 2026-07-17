from __future__ import annotations

from spine.jobs.geoscen.eis.contracts import UpstreamResponse
from spine.jobs.geoscen.eis.exceptions import ResponseValidationError
from spine.jobs.geoscen.eis.structure_context.connectors.fhfa.datasets import ALLOWED_CONTENT_TYPES, FHFADataset, validate_official_url


def validate_download_response(response: UpstreamResponse, dataset: FHFADataset) -> None:
    validate_official_url(response.url)
    content_type = (response.content_type or "").split(";")[0].strip().lower()
    if content_type and content_type not in ALLOWED_CONTENT_TYPES:
        raise ResponseValidationError("FHFA content type rejected.", provider="fhfa", context={"content_type": content_type})
    if not response.content:
        raise ResponseValidationError("FHFA download is empty.", provider="fhfa")
    if len(response.content) > dataset.maximum_expected_size:
        raise ResponseValidationError("FHFA download exceeds size bound.", provider="fhfa")
    if response.content.lstrip().lower().startswith(b"<!doctype html") or response.content.lstrip().lower().startswith(b"<html"):
        raise ResponseValidationError("FHFA download appears to be HTML.", provider="fhfa")
