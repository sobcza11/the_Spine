import pytest
from spine.equities.provenance.canonical_observation_metadata import build_canonical_observation_metadata
from spine.equities.provenance.serving_export_metadata import *
def setup(tmp_path,manifest):
 c=tmp_path/"c.json";c.write_text("[]");cm=build_canonical_observation_metadata(dataset_id="I",canonical_artifact=c,canonicalization_method_id="C",canonicalization_method_version="1",manifests=[manifest],provider="F",universe="I",symbols=["SPY"],observation_start="2026-01-01T00:00:00Z",observation_end="2026-01-01T00:00:00Z",record_count=0,field_contract={},transformation_contract={},corporate_action_contract={});s=tmp_path/"s.json";s.write_text("[]");sm=build_serving_export_metadata(serving_artifact=s,exporter="E",exporter_method_id="E1",exporter_method_version="1",canonical_artifact=c,canonical_metadata=cm,acquisition_manifest_ids=[manifest["manifest_id"]],provider="F",universe="I",symbols=["SPY"],observation_start="2026-01-01",observation_end="2026-01-01");return s,cm,sm
def test_preserves_asof(tmp_path,manifest):
 s,c,m=setup(tmp_path,manifest);assert m["as_of_time"]==c["as_of_time"];assert validate_serving_export_metadata(m,s,c)
def test_invented_asof(tmp_path,manifest):
 s,c,m=setup(tmp_path,manifest);m["as_of_time"]="2027-01-01T00:00:00Z";m["metadata_id"]=compute_serving_export_metadata_id(m)
 with pytest.raises(ValueError,match="AS_OF_INVENTED"):validate_serving_export_metadata(m,s,c)
