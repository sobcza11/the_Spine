import boto3
import pandas as pd
from io import BytesIO

ACCOUNT_ID = "51f902078bc0e5d7e38896e8a209ccd2"
ACCESS_KEY = "309950b4584ab2992107fdbfade53f95"
SECRET_KEY = "5a5c2db64e3f02b6cb71767d4ce05c3f2262513e5d455977aae6cb5d23359169"
BUCKET = "thespine-us-hub"

KEY = "spine_us/us_ir_diff_canonical.parquet"

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="auto",
)

obj = s3.get_object(Bucket=BUCKET, Key=KEY)
df = pd.read_parquet(BytesIO(obj["Body"].read()))

print(df.tail())
print("\nmax as_of_date:", pd.to_datetime(df["as_of_date"]).max())
print("shape:", df.shape)
print("columns:", df.columns.tolist())