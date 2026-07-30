from pathlib import Path
from ._common import *
def compute_equities_acquisition_manifest_id(v): return "equities-acq-"+digest(v,("manifest_id",))[:32]
def build_equities_acquisition_manifest(*,dataset_id,provider,provider_dataset,acquisition_method_id,acquisition_method_version,request_identity,request_parameters,request_started_at,response_received_at,available_at,source_observation_start,source_observation_end,symbols,record_count,raw_artifact,provider_evidence,lifecycle="SHADOW"):
 v={"object_type":"EQUITIES_ACQUISITION_MANIFEST_V1","schema_version":"1.0.0","manifest_id":"","domain":"EQUITIES","dataset_id":dataset_id,"provider":provider,"provider_dataset":provider_dataset,"acquisition_method_id":acquisition_method_id,"acquisition_method_version":acquisition_method_version,"request_identity":request_identity,"request_parameters":dict(sorted(request_parameters.items())),"request_started_at":request_started_at,"response_received_at":response_received_at,"available_at":available_at,"availability_semantics":"SYNCHRONOUS_RESPONSE_COMPLETE","source_observation_start":source_observation_start,"source_observation_end":source_observation_end,"symbols":sorted(symbols),"record_count":record_count,"raw_artifact":str(raw_artifact),"raw_artifact_sha256":file_hash(raw_artifact),"provider_evidence":provider_evidence,"lifecycle":lifecycle,"generated_deterministically":True,"serialization_contract":"CANONICAL_JSON_SORTED_KEYS_UTF8_NEWLINE"}
 v["manifest_id"]=compute_equities_acquisition_manifest_id(v);return validate_equities_acquisition_manifest(v,raw_artifact)
def load_equities_acquisition_manifest(path): return load(path)
def validate_equities_acquisition_manifest(v,artifact_path=None):
 reject_forbidden(v)
 for k in ("provider","acquisition_method_id","acquisition_method_version","request_started_at","response_received_at","available_at"): 
  if not v.get(k): raise ValueError(f"EQUITIES_ACQUISITION_REQUIRED:{k}")
 if len(v["symbols"])!=len(set(v["symbols"])) or not v["symbols"]: raise ValueError("EQUITIES_ACQUISITION_SYMBOLS_INVALID")
 utc_order(v["request_started_at"],v["response_received_at"]);utc_order(v["response_received_at"],v["available_at"])
 if v["source_observation_start"]>v["source_observation_end"]: raise ValueError("EQUITIES_ACQUISITION_OBSERVATION_RANGE_INVALID")
 if v["record_count"]<0: raise ValueError("EQUITIES_ACQUISITION_RECORD_COUNT_INVALID")
 if artifact_path is not None and file_hash(artifact_path)!=v["raw_artifact_sha256"]: raise ValueError("EQUITIES_ACQUISITION_RAW_HASH_MISMATCH")
 if v["manifest_id"]!=compute_equities_acquisition_manifest_id(v): raise ValueError("EQUITIES_ACQUISITION_ID_INVALID")
 return v
def write_equities_acquisition_manifest(v,path): validate_equities_acquisition_manifest(v);atomic(path,v)
