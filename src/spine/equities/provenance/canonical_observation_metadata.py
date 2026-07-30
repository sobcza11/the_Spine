from ._common import *
from .acquisition_manifest import validate_equities_acquisition_manifest
def compute_canonical_observation_metadata_id(v): return "equities-canonical-"+digest(v,("metadata_id",))[:32]
def derive_canonical_as_of_time(manifests):
 if not manifests: raise ValueError("EQUITIES_CANONICAL_MANIFEST_MISSING")
 ids=[m["manifest_id"] for m in manifests]
 if len(ids)!=len(set(ids)): raise ValueError("EQUITIES_CANONICAL_DUPLICATE_MANIFEST")
 for m in manifests: validate_equities_acquisition_manifest(m)
 return max(m["available_at"] for m in manifests)
def build_canonical_observation_metadata(*,dataset_id,canonical_artifact,canonicalization_method_id,canonicalization_method_version,manifests,provider,universe,symbols,observation_start,observation_end,record_count,field_contract,transformation_contract,corporate_action_contract,lifecycle="SHADOW"):
 asof=derive_canonical_as_of_time(manifests);v={"object_type":"EQUITIES_CANONICAL_OBSERVATION_SET_V1","schema_version":"1.0.0","metadata_id":"","domain":"EQUITIES","dataset_id":dataset_id,"canonical_artifact":str(canonical_artifact),"canonical_artifact_sha256":file_hash(canonical_artifact),"canonicalization_method_id":canonicalization_method_id,"canonicalization_method_version":canonicalization_method_version,"canonicalized_from_manifest_id":[m["manifest_id"] for m in manifests],"acquisition_manifests":[m["manifest_id"] for m in manifests],"acquisition_manifest_sha256":[digest(m) for m in manifests],"provider":provider,"universe":universe,"symbols":sorted(symbols),"observation_start":observation_start,"observation_end":observation_end,"observation_time_policy":"SOURCE_OBSERVATION_DATE","available_at":asof,"as_of_time":asof,"as_of_time_derivation":"MAX_REQUIRED_MANIFEST_AVAILABLE_AT","record_count":record_count,"field_contract":field_contract,"transformation_contract":transformation_contract,"corporate_action_contract":corporate_action_contract,"lineage":[m["manifest_id"] for m in manifests],"evidence":[{"artifact":str(canonical_artifact)}],"lifecycle":lifecycle,"generated_deterministically":True,"serialization_contract":"CANONICAL_JSON_SORTED_KEYS_UTF8_NEWLINE"}
 v["metadata_id"]=compute_canonical_observation_metadata_id(v);return validate_canonical_observation_metadata(v,canonical_artifact,manifests)
def load_canonical_observation_metadata(path): return load(path)
def validate_canonical_observation_metadata(v,artifact_path=None,manifests=None):
 reject_forbidden(v)
 if not v.get("canonicalization_method_id") or not v.get("universe") or not v.get("symbols"): raise ValueError("EQUITIES_CANONICAL_METADATA_INCOMPLETE")
 utc_order(v["observation_start"],v["observation_end"])
 if manifests is not None and v["as_of_time"]!=derive_canonical_as_of_time(manifests): raise ValueError("EQUITIES_CANONICAL_AS_OF_MISMATCH")
 if artifact_path is not None and file_hash(artifact_path)!=v["canonical_artifact_sha256"]: raise ValueError("EQUITIES_CANONICAL_HASH_MISMATCH")
 if v["metadata_id"]!=compute_canonical_observation_metadata_id(v): raise ValueError("EQUITIES_CANONICAL_ID_INVALID")
 return v
def match_metadata_to_canonical_artifact(v,path): return validate_canonical_observation_metadata(v,path)
def write_canonical_observation_metadata(v,path): validate_canonical_observation_metadata(v);atomic(path,v)
