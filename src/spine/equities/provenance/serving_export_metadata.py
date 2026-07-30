from ._common import *
from .canonical_observation_metadata import validate_canonical_observation_metadata
def compute_serving_export_metadata_id(v): return "equities-serving-"+digest(v,("metadata_id",))[:32]
def build_serving_export_metadata(*,serving_artifact,exporter,exporter_method_id,exporter_method_version,canonical_artifact,canonical_metadata,acquisition_manifest_ids,provider,universe,symbols,observation_start,observation_end,lifecycle="SHADOW"):
 validate_canonical_observation_metadata(canonical_metadata,canonical_artifact)
 v={"object_type":"EQUITIES_SERVING_EXPORT_METADATA_V1","schema_version":"1.0.0","metadata_id":"","domain":"EQUITIES","serving_artifact":str(serving_artifact),"serving_artifact_sha256":file_hash(serving_artifact),"exporter":exporter,"exporter_method_id":exporter_method_id,"exporter_method_version":exporter_method_version,"canonical_artifact":str(canonical_artifact),"canonical_artifact_sha256":file_hash(canonical_artifact),"canonical_metadata":canonical_metadata["metadata_id"],"canonical_metadata_sha256":digest(canonical_metadata),"acquisition_manifest_ids":list(acquisition_manifest_ids),"provider":provider,"universe":universe,"symbols":sorted(symbols),"observation_start":observation_start,"observation_end":observation_end,"observation_time_policy":canonical_metadata["observation_time_policy"],"available_at":canonical_metadata["available_at"],"as_of_time":canonical_metadata["as_of_time"],"lifecycle":lifecycle,"lineage":[canonical_metadata["metadata_id"]],"evidence":[{"artifact":str(serving_artifact)}],"generated_deterministically":True,"serialization_contract":"CANONICAL_JSON_SORTED_KEYS_UTF8_NEWLINE"}
 v["metadata_id"]=compute_serving_export_metadata_id(v);return validate_serving_export_metadata(v,serving_artifact,canonical_metadata)
def load_serving_export_metadata(path): return load(path)
def validate_serving_export_metadata(v,artifact_path=None,canonical_metadata=None):
 reject_forbidden(v)
 if v.get("lifecycle")!="SHADOW" or not v.get("exporter_method_id"): raise ValueError("EQUITIES_SERVING_METADATA_INCOMPLETE")
 if canonical_metadata is not None and v["as_of_time"]!=canonical_metadata["as_of_time"]: raise ValueError("EQUITIES_SERVING_AS_OF_INVENTED")
 if artifact_path is not None and file_hash(artifact_path)!=v["serving_artifact_sha256"]: raise ValueError("EQUITIES_SERVING_HASH_MISMATCH")
 if v["metadata_id"]!=compute_serving_export_metadata_id(v): raise ValueError("EQUITIES_SERVING_ID_INVALID")
 return v
def match_metadata_to_serving_artifact(v,path): return validate_serving_export_metadata(v,path)
def write_serving_export_metadata(v,path): validate_serving_export_metadata(v);atomic(path,v)
