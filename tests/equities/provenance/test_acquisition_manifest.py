import pytest
from spine.equities.provenance.acquisition_manifest import *
def test_deterministic_sorted(manifest):
 assert manifest["symbols"]==["SPY"];assert compute_equities_acquisition_manifest_id(manifest)==manifest["manifest_id"]
def test_hash_mismatch(manifest,raw):
 raw.write_text("changed")
 with pytest.raises(ValueError,match="HASH_MISMATCH"):validate_equities_acquisition_manifest(manifest,raw)
def test_duplicate(manifest):
 manifest["symbols"]=["SPY","SPY"];manifest["manifest_id"]=compute_equities_acquisition_manifest_id(manifest)
 with pytest.raises(ValueError,match="SYMBOLS_INVALID"):validate_equities_acquisition_manifest(manifest)
def test_time_inversion(manifest):
 manifest["available_at"]="2025-01-01T00:00:00Z";manifest["manifest_id"]=compute_equities_acquisition_manifest_id(manifest)
 with pytest.raises(ValueError,match="TIMESTAMP_INVERSION"):validate_equities_acquisition_manifest(manifest)
def test_forbidden_time(manifest):
 manifest["file_mtime"]="x";manifest["manifest_id"]=compute_equities_acquisition_manifest_id(manifest)
 with pytest.raises(ValueError,match="FORBIDDEN_TIMESTAMP"):validate_equities_acquisition_manifest(manifest)
