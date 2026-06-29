from spine.jobs.rates.policy_language import build_latest


JURISDICTIONS = [

    "US",
    "EU",
    "GB",
    "JP",
    "CA",
    "AU",

]


def main():

    print()
    print("=" * 70)
    print("GeoScen Policy Language Builder")
    print("=" * 70)

    for jurisdiction in JURISDICTIONS:

        print()
        print("=" * 70)
        print(f"Building {jurisdiction}")
        print("=" * 70)

        try:

            build_latest(jurisdiction)

            print(f"{jurisdiction} COMPLETE")

        except Exception as e:

            print(f"{jurisdiction} FAILED")
            print(e)

            continue

    print()
    print("=" * 70)
    print("All policy-language builds complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
