from pathlib import Path

from spine.equities.iv.build_equities_iv_vector_contribution import (
    build_equities_domain_contribution,
    validate_equities_domain_contribution,
)
from spine.sys.aggregator import SysAggregator
from spine.sys.contracts import AvailabilityState, DomainName, ValidationStatus

FIXTURE = Path(__file__).parent / "fixtures" / "governed_equities_contribution.json"
OBS = "2026-07-29T00:00:00Z"
AS_OF = "2026-07-29T01:00:00Z"


def test_equities_to_canonical_sys_pipeline_is_deterministic():
    contribution = build_equities_domain_contribution(
        (FIXTURE,), observation_time=OBS, as_of_time=AS_OF, allow_partial=True,
    )
    validate_equities_domain_contribution(contribution)
    first = SysAggregator().aggregate((contribution,), observation_time=OBS, as_of_time=AS_OF)
    second = SysAggregator().aggregate((contribution,), observation_time=OBS, as_of_time=AS_OF)
    assert first.canonical_owner == "SYS"
    assert first.vector_order == ("P", "F", "L", "D", "M", "X", "C", "S")
    assert first.record_id == second.record_id
    by_name = {item.coordinate.value: item for item in first.coordinates}
    assert by_name["P"].contributors[0].domain is DomainName.EQUITIES
    assert by_name["D"].contributors[0].domain is DomainName.EQUITIES
    assert by_name["P"].lineage and by_name["P"].supporting_evidence
    assert by_name["L"].state is AvailabilityState.UNAVAILABLE and by_name["L"].value is None
    assert by_name["S"].value is None
    assert by_name["S"].validation_status is ValidationStatus.METHODOLOGY_NOT_AUTHORIZED
