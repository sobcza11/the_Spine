from spine.equities.provenance.check_p_source_readiness import check_p_source_readiness
def test_historical_blocked(tmp_path):
 r=check_p_source_readiness("data/serving/equities/us_equity_index_data.json",tmp_path/"missing-i","data/serving/equities/us_sector_etf_data.json",tmp_path/"missing-s")
 assert not r["ready"] and r["status"]=="SOURCE_PROVENANCE_BLOCKED" and "HISTORICAL_SOURCE_PROVENANCE_UNAVAILABLE" in r["reason_codes"]
