import boto3
import pyarrow.parquet as pq
import os
from config import LOCALSTACK_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

# Initialize S3 client for LocalStack
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    endpoint_url=LOCALSTACK_ENDPOINT,  # Ensure LocalStack is used
)

def list_s3_objects(bucket, prefix=""):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]

def get_parquet_metadata(bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(obj["Body"])
    schema = table.schema
    return {
        "columns": [str(f) for f in schema],
        "num_rows": table.num_rows,
    }
