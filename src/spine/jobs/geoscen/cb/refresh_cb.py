import argparse
import importlib
import sys

from spine.jobs.geoscen.cb.refresh_registry import BANK_REGISTRY


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

