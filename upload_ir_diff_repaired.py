import boto3

ACCOUNT_ID = "51f902078bc0e5d7e38896e8a209ccd2"
ACCESS_KEY = "309950b4584ab2992107fdbfade53f95"
SECRET_KEY = "5a5c2db64e3f02b6cb71767d4ce05c3f2262513e5d455977aae6cb5d23359169"
BUCKET = "thespine-us-hub"

local_file = r"C:\Users\Rand Sobczak Jr\_rts\3_AI\IsoVector\_py\r2_tmp\us_ir_diff_canonical.parquet"
r2_key = "spine_us/us_ir_diff_canonical.parquet"

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="auto",
)

s3.upload_file(local_file, BUCKET, r2_key)

print("uploaded:", r2_key)