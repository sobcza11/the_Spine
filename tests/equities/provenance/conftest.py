import pytest
from spine.equities.provenance.acquisition_manifest import build_equities_acquisition_manifest
@pytest.fixture
def raw(tmp_path):
 p=tmp_path/"raw.json";p.write_text('[{"symbol":"SPY","date":"2026-01-01","close":1}]');return p
@pytest.fixture
def manifest(raw):
 return build_equities_acquisition_manifest(dataset_id="INDEX",provider="FIXTURE",provider_dataset="EOD",acquisition_method_id="FIXTURE_ACQ_V1",acquisition_method_version="1.0.0",request_identity="r",request_parameters={"b":2,"a":1},request_started_at="2026-01-02T00:00:00Z",response_received_at="2026-01-02T00:01:00Z",available_at="2026-01-02T00:01:00Z",source_observation_start="2026-01-01",source_observation_end="2026-01-01",symbols=["SPY"],record_count=1,raw_artifact=raw,provider_evidence={"fixture":True})
