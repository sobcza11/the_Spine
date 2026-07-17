from __future__ import annotations

import json

import pytest

from spine.jobs.geoscen.eis.artifacts import EISArtifactError, normalized_artifact_payload, safe_artifact_dir, write_normalized_artifact
from spine.jobs.geoscen.eis.bootstrap import register_eis_connectors
from spine.jobs.geoscen.eis.contracts import ConnectorRequest
from spine.jobs.geoscen.eis.dispatcher import dispatch
from spine.jobs.geoscen.eis.orchestrator import FixtureHttpClient, _temporary_credentials
from spine.jobs.geoscen.eis.registry import ConnectorRegistry


def _response():
    registry = ConnectorRegistry()
    register_eis_connectors(registry)
    env = _temporary_credentials()
    try:
        return dispatch(ConnectorRequest(provider="hud", operation="list_states"), registry=registry, http_client=FixtureHttpClient(b'{"status":"success","data":[{"state_id":"MI","state_code":"MI","state_name":"Michigan"}]}'))
    finally:
        env.restore()


def test_normalized_artifact_contract_atomic_sha_and_no_raw_body(tmp_path) -> None:
    response = _response()
    payload = normalized_artifact_payload(response, specification_id="hud_state_lookup", run_id="run1")
    assert payload["artifact_type"] == "eis_normalized_source"
    assert payload["row_count"] == 1
    ref = write_normalized_artifact(response, specification_id="hud_state_lookup", run_id="run1", root=tmp_path)
    assert ref.sha256
    text = open(ref.path, encoding="utf-8").read()
    assert "HUD_USER_ACCESS_TOKEN" not in text
    assert '"data":' not in text
    assert json.loads(text)["specification_id"] == "hud_state_lookup"
    with pytest.raises(EISArtifactError):
        write_normalized_artifact(response, specification_id="hud_state_lookup", run_id="run1", root=tmp_path)
    with pytest.raises(EISArtifactError):
        safe_artifact_dir(tmp_path, "..", "spec", "run")
