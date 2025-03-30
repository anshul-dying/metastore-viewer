import os
import boto3

S3_ENDPOINT = "http://localhost:9000"  # Change to your MinIO endpoint
AWS_ACCESS_KEY_ID = "admin"  
AWS_SECRET_ACCESS_KEY = "password"  
BUCKET_NAME = "test-bucket"  # Change as needed

# Initialize MinIO client
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)



import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO

# Generate sample data
df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 22]
})

# Convert to Parquet format
buffer = BytesIO()
df.to_parquet(buffer, engine="pyarrow")
buffer.seek(0)

# Upload to MinIO inside the 'data/' folder
file_key = "iceberg/data/sample.parquet"
s3_client.put_object(Bucket=BUCKET_NAME, Key=file_key, Body=buffer.getvalue())

print(f"Uploaded data file: {file_key}")






import json

# Metadata structure
metadata = {
    "file_name": file_key,
    "columns": list(df.columns),
    "num_rows": len(df),
    "file_size": buffer.getbuffer().nbytes,
    "format": "parquet"
}

# Convert to JSON format
metadata_json = json.dumps(metadata, indent=4)
metadata_buffer = BytesIO(metadata_json.encode())

# Upload metadata to MinIO
metadata_key = "iceberg/metadata/sample_metadata.json"
s3_client.put_object(Bucket=BUCKET_NAME, Key=metadata_key, Body=metadata_buffer.getvalue())

print(f"Uploaded metadata file: {metadata_key}")


def list_files(prefix="iceberg/"):
    """List all objects under the specified prefix."""
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    if "Contents" in response:
        return [obj["Key"] for obj in response["Contents"]]
    return []

print("Files in MinIO:", list_files())