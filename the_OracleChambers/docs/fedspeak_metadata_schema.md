# FedSpeak Metadata Schema (Phase III)

Each FedSpeak artifact SHOULD have a sidecar JSON metadata file with:

{
  "artifact_name": "string",           // e.g., "BeigeBook_canonical_sentences"
  "artifact_type": "canonical|features|leaf",
  "channel": "beige_book|minutes|statement|sep|speech|combined",
  "version": "string",                 // semantic version or git hash
  "data_date": "YYYY-MM-DD",          // date associated with Fed event (or range)
  "ingest_timestamp_utc": "ISO-8601", // when raw was ingested
  "process_timestamp_utc": "ISO-8601",// when this artifact was produced
  "source_reference": "string",       // URL or doc reference
  "record_count": "int",
  "schema_columns": ["col1", "col2", "..."],
  "schema_hash": "string",            // hash of schema_columns
  "pipeline_config_version": "string",// e.g., fedspeak_pipeline.yaml version
  "notes": "string"                   // free-form notes
}
