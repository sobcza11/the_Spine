############################################################
# refresh_registry.py
############################################################

from pathlib import Path

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

}


############################################################
# refresh_cb.py
############################################################

import argparse
import importlib
import sys

from refresh_registry import BANK_REGISTRY


def execute_pipeline(bank):

    cfg = BANK_REGISTRY[bank]

    print("=" * 70)
    print(cfg["name"])
    print("=" * 70)

    for module_name in cfg["pipeline"]:

        print(f"\n>>> {module_name}")

        module = importlib.import_module(module_name)

        if hasattr(module, "main"):
            module.main()

        elif hasattr(module, "run"):
            module.run()

        else:
            raise RuntimeError(
                f"{module_name} has no main() or run()"
            )

    print("\nPipeline Complete.")


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bank",
        required=True,
        choices=BANK_REGISTRY.keys(),
    )

    args = parser.parse_args()

    execute_pipeline(args.bank.upper())


if __name__ == "__main__":
    main()

