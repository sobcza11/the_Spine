import pytest
from spine.equities.provenance.canonical_observation_metadata import *
def build(tmp_path,manifest):
 p=tmp_path/"canonical.json";p.write_text('[{"symbol":"SPY","observation_date":"2026-01-01","close":1}]')
 m=build_canonical_observation_metadata(dataset_id="INDEX",canonical_artifact=p,canonicalization_method_id="CANON_V1",canonicalization_method_version="1.0.0",manifests=[manifest],provider="FIXTURE",universe="INDEX",symbols=["SPY"],observation_start="2026-01-01T00:00:00Z",observation_end="2026-01-01T00:00:00Z",record_count=1,field_contract={},transformation_contract={},corporate_action_contract={});return p,m
def test_metadata(tmp_path,manifest):
 p,m=build(tmp_path,manifest);assert m["as_of_time"]==manifest["available_at"];assert match_metadata_to_canonical_artifact(m,p)
def test_hash(tmp_path,manifest):
 p,m=build(tmp_path,manifest);p.write_text("x")
 with pytest.raises(ValueError,match="HASH_MISMATCH"):validate_canonical_observation_metadata(m,p)
def test_multi_latest(raw,manifest):
 m2=dict(manifest);m2["manifest_id"]="other";m2["available_at"]="2026-01-03T00:00:00Z";m2["manifest_id"]=__import__("spine.equities.provenance.acquisition_manifest",fromlist=["compute_equities_acquisition_manifest_id"]).compute_equities_acquisition_manifest_id(m2)
 assert derive_canonical_as_of_time([manifest,m2])=="2026-01-03T00:00:00Z"
def test_duplicate_manifest(manifest):
 with pytest.raises(ValueError,match="DUPLICATE_MANIFEST"):derive_canonical_as_of_time([manifest,manifest])
