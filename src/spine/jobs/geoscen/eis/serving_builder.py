from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping, Sequence

from spine.jobs.geoscen.eis.analytical_contracts import AnalyticalObservation, StructureContextServingArtifact
from spine.jobs.geoscen.eis.completion_ledger import build_completion_ledger, ledger_entry
from spine.jobs.geoscen.eis.contracts import utc_now_iso
from spine.jobs.geoscen.eis.evidence import build_evidence_record, evidence_id
from spine.jobs.geoscen.eis.geography_normalization import normalize_geography
from spine.jobs.geoscen.eis.indicator_specifications import IndicatorSpecification, initial_indicator_specifications, validate_indicator_specifications
from spine.jobs.geoscen.eis.period_normalization import normalize_period
from spine.jobs.geoscen.eis.serving_validation import validate_serving_artifact
from spine.jobs.geoscen.eis.transformations import latest_valid, parse_numeric, percent_change

FAMILY_DISPLAY_NAMES = {
    "labor": "Labor",
    "income": "Income",
    "housing": "Housing",
    "population": "Population",
    "production_activity": "Production / Activity",
    "fiscal_government": "Fiscal / Government",
    "external_trade": "External / Trade",
}


def build_structure_context_serving_artifact(
    normalized_sources: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
    generated_at: str | None = None,
    indicator_specifications: Sequence[IndicatorSpecification] | None = None,
    source_manifest_reference: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    specs = tuple(indicator_specifications or initial_indicator_specifications())
    validate_indicator_specifications(specs)
    sources = {str(source.get("specification_id")): source for source in normalized_sources}
    observations: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    ledger_entries: list[dict[str, Any]] = []
    warnings: list[str] = []

    for spec in specs:
        source = sources.get(spec.source_specification_id)
        built = _build_indicator_observations(spec, source)
        spec_observations = [row["observation"] for row in built]
        spec_evidence = [row["evidence"] for row in built]
        observations.extend(spec_observations)
        evidence_rows.extend(spec_evidence)
        entry_warnings = [warning for row in spec_observations for warning in row.get("warnings", ())]
        if source is None:
            entry_warnings.append("source_artifact_missing")
        ledger_entries.append(
            ledger_entry(
                spec,
                observations=spec_observations,
                source_specification_exists=True,
                source_artifact_available=source is not None,
                required_fields_available=any(row["completion_status"] in {"complete", "stale"} for row in spec_observations),
                transformation_passed=any(row["status"] in {"measured", "derived"} for row in spec_observations),
                validation_passed=all(row.get("validation", {}).get("valid") for row in spec_observations),
                evidence_created=bool(spec_evidence),
                warnings=entry_warnings,
            ),
        )

    completion = build_completion_ledger(ledger_entries)
    unavailable = [row for row in observations if row["completion_status"] in {"unavailable", "invalid"}]
    artifact = StructureContextServingArtifact(
        run_id=run_id,
        generated_at=generated_at or utc_now_iso(),
        domain="U.S. Economic Structure / Context",
        geography_scope=tuple(sorted({row["geography"]["geography_level"] for row in observations})),
        source_manifest_reference=source_manifest_reference,
        completion=completion,
        families=_family_summary(observations),
        observations=observations,
        evidence=evidence_rows,
        unavailable=unavailable,
        validation={"valid": True, "errors": (), "warnings": tuple(warnings)},
        lineage_index=_lineage_index(observations),
        source_summary=_source_summary(normalized_sources),
        warnings=warnings,
    ).to_dict()
    validate_serving_artifact(artifact)
    return artifact


def _build_indicator_observations(spec: IndicatorSpecification, source: Mapping[str, Any] | None) -> list[dict[str, dict[str, Any]]]:
    rows = _filtered_rows(spec, list(source.get("rows", ())) if source else [])
    if source is None or not rows:
        return [_unavailable(spec, source, "source_artifact_missing" if source is None else "source_rows_missing")]
    if spec.provider == "census_acs" and spec.transformation == "parse_numeric":
        return [_single_row_numeric(spec, source, row, spec.parameters.get("variable") or spec.source_fields[0]) for row in rows]
    if spec.transformation == "latest_valid":
        value_field = spec.source_fields[0]
        period_field = spec.source_fields[1] if len(spec.source_fields) > 1 else "period"
        result = latest_valid(rows, value_field=value_field, period_field=period_field)
        if result["row"] is None:
            return [_unavailable(spec, source, *result["warnings"])]
        return [_from_transform_result(spec, source, result)]
    if spec.transformation == "percent_change":
        value_field = spec.source_fields[0]
        period_field = spec.source_fields[1] if len(spec.source_fields) > 1 else "period"
        result = percent_change(rows, value_field=value_field, period_field=period_field)
        if result["status"] == "unavailable":
            return [_unavailable(spec, source, *result["warnings"], row=result.get("row"))]
        return [_from_transform_result(spec, source, result)]
    return [_unavailable(spec, source, "transformation_waiting_for_source_shape")]


def _single_row_numeric(spec: IndicatorSpecification, source: Mapping[str, Any], row: Mapping[str, Any], variable_or_field: str) -> dict[str, dict[str, Any]]:
    value = row.get("variables", {}).get(variable_or_field, {}).get("value") if isinstance(row.get("variables"), Mapping) else _lookup(row, variable_or_field)
    raw_value = row.get("variables", {}).get(variable_or_field, {}).get("raw_value", value) if isinstance(row.get("variables"), Mapping) else value
    parsed, status, warnings = parse_numeric(value)
    return _observation_bundle(spec, source, row, value=parsed, raw_value=raw_value, status=status, warnings=warnings)


def _from_transform_result(spec: IndicatorSpecification, source: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return _observation_bundle(spec, source, result["row"], value=result["value"], raw_value=result["raw_value"], status=result["status"], warnings=result.get("warnings", ()))


def _unavailable(spec: IndicatorSpecification, source: Mapping[str, Any] | None, *warnings: str, row: Mapping[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    fallback_row: Mapping[str, Any] = row or {}
    return _observation_bundle(spec, source, fallback_row, value=None, raw_value=None, status="unavailable", warnings=list(warnings or ("value_unavailable",)))


def _observation_bundle(spec: IndicatorSpecification, source: Mapping[str, Any] | None, row: Mapping[str, Any], *, value: float | int | None, raw_value: Any, status: str, warnings: Sequence[str]) -> dict[str, dict[str, Any]]:
    geography = normalize_geography(row, provider=spec.provider, expected_level=spec.geography_level)
    period = normalize_period(row, frequency=spec.frequency, source_retrieved_at=str(source.get("retrieved_at")) if source else None)
    freshness = _freshness_status(status, period)
    completion_status = "complete" if status in {"measured", "derived"} else "unavailable"
    if freshness in {"stale", "very_stale"} and completion_status == "complete":
        completion_status = "stale"
    ev_id = evidence_id(indicator_id=spec.indicator_id, source_specification_id=spec.source_specification_id, geography_id=str(geography["geography_id"]), period=str(period["period"]))
    source_ref = _source_reference(source, spec)
    lineage_reference = _lineage_reference(source, spec)
    validation = {"valid": status != "invalid", "errors": (), "warnings": tuple(warnings), "rules": spec.validation_rules}
    observation = AnalyticalObservation(
        indicator_id=spec.indicator_id,
        display_name=spec.display_name,
        family=spec.family,
        classification=spec.classification,
        geography=geography,
        period=period,
        value=value,
        raw_value=raw_value,
        unit=spec.unit,
        status=status,
        freshness=freshness,
        direction=spec.direction_semantics,
        source=source_ref,
        source_specification_id=spec.source_specification_id,
        provider=spec.provider,
        transformation=spec.transformation,
        derived=status == "derived",
        input_references=_input_references(spec, source, row),
        evidence_ids=(ev_id,),
        validation=validation,
        lineage_reference=lineage_reference,
        measurement_as_of=period.get("measurement_as_of"),
        publication_date=period.get("publication_date"),
        retrieval_timestamp=period.get("retrieval_timestamp") or (source.get("retrieved_at") if source else None),
        completion_status=completion_status,
        warnings=tuple(warnings),
    ).to_dict()
    evidence = build_evidence_record(
        evidence_id_value=ev_id,
        indicator_id=spec.indicator_id,
        geography=geography,
        period=period,
        claim_type=_claim_type(status, spec.transformation),
        observation=_evidence_observation(spec, value, status, period, warnings),
        source_artifact=source,
        source_fields=spec.source_fields,
        transformation_version=spec.transformation_version,
        lineage_reference=lineage_reference,
        confidence="governed_source" if status in {"measured", "derived"} else "unavailable",
        limitations=tuple(warnings),
    )
    return {"observation": observation, "evidence": evidence}


def _family_summary(observations: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in observations:
        grouped[str(row["family"])].append(row)
    summaries: list[dict[str, Any]] = []
    for family_id, rows in sorted(grouped.items()):
        providers = sorted({str(row["provider"]) for row in rows})
        summaries.append(
            {
                "family_id": family_id,
                "display_name": FAMILY_DISPLAY_NAMES.get(family_id, family_id.replace("_", " ").title()),
                "structure_indicator_ids": sorted({str(row["indicator_id"]) for row in rows if row["classification"] == "structure"}),
                "context_indicator_ids": sorted({str(row["indicator_id"]) for row in rows if row["classification"] == "context"}),
                "available_count": sum(1 for row in rows if row["completion_status"] == "complete"),
                "unavailable_count": sum(1 for row in rows if row["completion_status"] == "unavailable"),
                "stale_count": sum(1 for row in rows if row["completion_status"] == "stale"),
                "latest_measurement_as_of": max((str(row["measurement_as_of"]) for row in rows if row.get("measurement_as_of")), default=None),
                "source_providers": providers,
                "warnings": sorted({warning for row in rows for warning in row.get("warnings", ())}),
            },
        )
    return summaries


def _lineage_index(observations: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [{"indicator_id": row["indicator_id"], "evidence_ids": row["evidence_ids"], "lineage_reference": row["lineage_reference"]} for row in observations]


def _source_summary(sources: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "provider": source.get("provider"),
            "operation": source.get("operation"),
            "specification_id": source.get("specification_id"),
            "status": source.get("status"),
            "row_count": source.get("row_count"),
            "retrieved_at": source.get("retrieved_at"),
        }
        for source in sources
    ]


def _freshness_status(status: str, period: Mapping[str, Any]) -> str:
    if status in {"invalid"}:
        return "invalid"
    if status == "unavailable":
        return "unavailable"
    if period.get("period") in {"unknown-monthly", "unknown-acs5", "metadata", ""}:
        return "unavailable"
    return "current"


def _claim_type(status: str, transformation: str) -> str:
    if status == "derived":
        return "derived_value"
    if status == "unavailable":
        return "source_unavailable"
    if transformation in {"percent_change", "absolute_change", "basis_point_change", "index_change"}:
        return "measured_change"
    return "measured_value"


def _evidence_observation(spec: IndicatorSpecification, value: float | int | None, status: str, period: Mapping[str, Any], warnings: Sequence[str]) -> str:
    if status == "unavailable":
        reason = ", ".join(warnings) if warnings else "source value unavailable"
        return f"{spec.display_name} was unavailable for {period.get('period')} because {reason}."
    verb = "derived" if status == "derived" else "measured"
    return f"{spec.provider.upper()} {verb} {spec.display_name} at {value} {spec.unit} for {period.get('period')}."


def _source_reference(source: Mapping[str, Any] | None, spec: IndicatorSpecification) -> dict[str, Any]:
    return {
        "provider": spec.provider,
        "operation": spec.source_operation,
        "source_specification_id": spec.source_specification_id,
        "source_artifact_available": source is not None,
    }


def _lineage_reference(source: Mapping[str, Any] | None, spec: IndicatorSpecification) -> dict[str, Any]:
    lineage = dict(source.get("lineage", {})) if source else {}
    return {
        "provider": spec.provider,
        "source_specification_id": spec.source_specification_id,
        "normalized_sha256": lineage.get("normalized_sha256"),
        "transformation_version": spec.transformation_version,
    }


def _input_references(spec: IndicatorSpecification, source: Mapping[str, Any] | None, row: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    return tuple({"source_specification_id": spec.source_specification_id, "field": field, "available": _lookup(row, field) is not None or (field in row), "source_artifact_available": source is not None} for field in spec.source_fields)


def _lookup(row: Mapping[str, Any], dotted: str) -> Any:
    current: Any = row
    for part in dotted.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _filtered_rows(spec: IndicatorSpecification, rows: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    series_id = spec.parameters.get("series_id")
    if series_id:
        return [row for row in rows if str(row.get("series_id")) == str(series_id)]
    return rows
