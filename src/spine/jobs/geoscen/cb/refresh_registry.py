############################################################
# refresh_registry.py
############################################################

BANK_REGISTRY = {

    "FED": {
        "name": "Federal Reserve",
        "pipeline": [
            "spine.jobs.geoscen.fomc.ingest_fomc_minutes_t1",
            "spine.jobs.geoscen.fomc.validate_fomc_minutes_t1",
            "spine.jobs.geoscen.fomc.historical.ingest_fomc_historical_materials_t1",
            "spine.jobs.geoscen.fomc.historical.validate_fomc_historical_materials_t1",
            "spine.jobs.geoscen.fomc.historical.extract_fomc_historical_pdf_text_v2",
            "spine.jobs.geoscen.fomc.historical.validate_fomc_historical_pdf_text_v2",
            "spine.jobs.geoscen.fomc.upload_fomc_minutes_to_r2",
            "spine.jobs.geoscen.fomc.historical.upload_fomc_historical_materials_to_r2",
        ],
    },

    "ECB": {
        "name": "European Central Bank",
        "pipeline": [
            "spine.jobs.geoscen.cb.ecb.ingest_ecb_policy_decisions_t1",
            "spine.jobs.geoscen.cb.ecb.validate_ecb_policy_decisions_t1",
            "spine.jobs.geoscen.cb.ecb.upload_ecb_policy_decisions_to_r2",
            "spine.jobs.geoscen.cb.ecb.ingest_ecb_accounts_t1",
            "spine.jobs.geoscen.cb.ecb.validate_ecb_accounts_t1",
            "spine.jobs.geoscen.cb.ecb.upload_ecb_accounts_to_r2",
            "spine.jobs.geoscen.cb.ecb.build_ecb_combined_canonical_v1",
            "spine.jobs.geoscen.cb.ecb.validate_ecb_combined_canonical_v1",
            "spine.jobs.geoscen.cb.ecb.upload_ecb_combined_canonical_to_r2",
            "spine.jobs.geoscen.cb.build_macro_cb_canonical_v1",
            "spine.jobs.geoscen.cb.validate_macro_cb_canonical_v1",
            "spine.jobs.geoscen.cb.upload_macro_cb_canonical_to_r2",
        ],
    },

    "BOE": {
        "name": "Bank of England",
        "pipeline": [
            "spine.jobs.geoscen.cb.boe.extract_boe_policy_minutes_text_t1",
        ],
    },

    "BOJ": {
        "name": "Bank of Japan",
        "pipeline": [
            "spine.jobs.geoscen.cb.boj.extract_boj_outlook_text_t1",
        ],
    },

    "BOC": {
        "name": "Bank of Canada",
        "pipeline": [
            "spine.jobs.geoscen.cb.boc.extract_boc_rate_text_t1",
        ],
    },

}