import os
import boto3


def classify_key(key: str) -> str:
    k = key.lower()

    if "rates" in k:
        return "RATES"
    elif "commflow" in k or "cot" in k:
        return "COMMFLOW"
    elif "wti" in k:
        return "WTI"
    elif "equities" in k or "etf" in k:
        return "EQUITIES"
    elif "fx" in k:
        return "FX"
    elif "geoscen" in k or "macro_cb" in k or "pmi" in k:
        return "GEOSCEN / OC"
    else:
        return "OTHER"


def main():
    session = boto3.session.Session()

    client = session.client(
        service_name="s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("R2_REGION", "auto"),
    )

    bucket = os.environ["R2_BUCKET_NAME"]
    paginator = client.get_paginator("list_objects_v2")

    grouped = {
        "RATES": [],
        "COMMFLOW": [],
        "WTI": [],
        "EQUITIES": [],
        "FX": [],
        "GEOSCEN / OC": [],
        "OTHER": []
    }

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                bucket_name = classify_key(key)
                grouped[bucket_name].append(key)

    print("\n=== R2 PARQUET FILES BY MODULE ===\n")

    for group, files in grouped.items():
        print(f"\n--- {group} ({len(files)}) ---")
        for f in sorted(files):
            print(f)


if __name__ == "__main__":
    main()
