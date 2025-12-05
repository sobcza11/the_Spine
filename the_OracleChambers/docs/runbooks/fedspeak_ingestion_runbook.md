# FedSpeak Ingestion & Data Preparation Runbook

## 1. Purpose
This runbook documents how FedSpeak data is ingested, canonicalized, and prepared
for modeling within the_Spine, in compliance with CPMAI Phase III.

## 2. Scope
- Channels: Beige Book, FOMC Minutes, FOMC Statements (SEP & Speeches later).
- Artifacts: canonical text, topics, sentiment, combined_policy_leaf.

## 3. Data Sources
- Beige Book: Federal Reserve Board website (HTML).
- Minutes: FOMC PDF releases.
- Statements: FOMC statement HTML pages.
- All source URLs must be recorded in metadata.

## 4. Ingestion Steps (per release)
1. Identify new Fed release dates.
2. Download raw documents (HTML/PDF).
3. Run canonicalization script:
   - Output to data/canonical/FedSpeak/<Channel>/<date>_canonical_*.parquet
   - Generate corresponding *_meta.json using metadata schema.

## 5. Data Preparation Steps
1. Run topic modeling for Beige Book using parameters from config/pipelines/fedspeak_pipeline.yaml.
2. Compute sentiment scores at sentence/paragraph level.
3. Derive channel-specific features:
   - beige_inflation_risk, beige_growth_risk, beige_policy_bias
   - minutes_* feature set
4. Aggregate into combined_policy_leaf:
   - Apply weights defined in fedspeak_pipeline.yaml.
   - Normalize features to [-1,1].
5. Write combined_policy_leaf.parquet and associated meta JSON.

## 6. Quality Checks
- Ensure all required channels for the event are available or allowed as missing.
- Run pytest tests/test_artifacts_validation.py.
- Check logs/logfile for ingestion or processing errors.

## 7. Versioning & Lineage
- Each canonical artifact must have:
  - version
  - data_date
  - ingest & process timestamps
  - schema_hash
- Each pipeline run should record:
  - pipeline_id, version
  - git commit hash
  - configuration file used

## 8. Recovery & Reprocessing
- If a test fails:
  - Review pytest output.
  - Check metadata and logs.
  - Fix data or parameters, rerun pipeline.
- For historical reprocessing:
  - Re-run canonical + prep with pinned config and record new versions in meta.

## 9. Handoff & Ownership
- FedSpeak Lab owns this runbook and the pipeline config.
- Changes require:
  - config update
  - test updates (if schema/feature changes)
  - version bump in fedspeak_pipeline.yaml and fedspeak_features.yaml
