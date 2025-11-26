from US_TeaPlant.trunk.us_spine_core_builder import (
    SpineUSCoreConfig,
    build_spine_us_core,
)


def main() -> None:
    print("[spine_us] Starting Spine-US core build (WTI-only)...")

    cfg = SpineUSCoreConfig()
    core = build_spine_us_core(cfg)

    print("[spine_us] Core built. Sample:")
    print(core.head())


if __name__ == "__main__":
    main()

