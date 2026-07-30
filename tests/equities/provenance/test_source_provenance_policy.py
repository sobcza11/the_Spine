from pathlib import Path
import pytest
from spine.equities.provenance.source_provenance_policy import *
P=Path(__file__).parents[3]/"governance/equities_source_provenance_and_canonical_observation_v1.json"
def test_policy():
 p=load_equities_source_provenance_policy(P);assert validate_equities_source_provenance_policy(p);assert compute_equities_source_provenance_policy_id(p)==p["deterministic_content_id"]
def test_wrong_owner():
 p=load_equities_source_provenance_policy(P);p["canonical_owner"]="EQUITIES"
 with pytest.raises(ValueError):validate_equities_source_provenance_policy(p)
def test_contracts():
 p=load_equities_source_provenance_policy(P);assert get_acquisition_manifest_contract(p)["schema_version"]=="1.0.0";assert get_canonical_observation_contract(p);assert get_serving_export_contract(p)
