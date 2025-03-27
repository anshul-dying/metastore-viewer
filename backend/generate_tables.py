import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from deltalake.writer import write_deltalake
import shutil
import json

# MinIO Configuration
S3_ENDPOINT = "http://localhost:9000"
AWS_ACCESS_KEY_ID = "admin"
AWS_SECRET_ACCESS_KEY = "password"
BUCKET_NAME = "test-bucket"

# Sample Data
data = [
    {"id": 1, "name": "Alice", "date": datetime(2023, 1, 1), "value": 100},
    {"id": 2, "name": "Bob", "date": datetime(2023, 1, 2), "value": 200},
    {"id": 3, "name": "Cathy", "date": datetime(2023, 2, 1), "value": 300}
]
pandas_df = pd.DataFrame(data)

# 1. Standalone Parquet File
def generate_parquet_file():
    table = pa.Table.from_pandas(pandas_df)
    pq.write_table(table, "sample_parquet.parquet")
    os.system(f"aws --endpoint-url={S3_ENDPOINT} s3 cp sample_parquet.parquet s3://{BUCKET_NAME}/sample_parquet.parquet")
    os.remove("sample_parquet.parquet")

# 2. Parquet Directory (Partitioned)
def generate_parquet_directory():
    os.makedirs("sample_parquet_dir/date=2023-01-01", exist_ok=True)
    os.makedirs("sample_parquet_dir/date=2023-01-02", exist_ok=True)
    os.makedirs("sample_parquet_dir/date=2023-02-01", exist_ok=True)
    df_2023_01_01 = pandas_df[pandas_df["date"] == datetime(2023, 1, 1)]
    df_2023_01_02 = pandas_df[pandas_df["date"] == datetime(2023, 1, 2)]
    df_2023_02_01 = pandas_df[pandas_df["date"] == datetime(2023, 2, 1)]
    pq.write_table(pa.Table.from_pandas(df_2023_01_01), "sample_parquet_dir/date=2023-01-01/data.parquet")
    pq.write_table(pa.Table.from_pandas(df_2023_01_02), "sample_parquet_dir/date=2023-01-02/data.parquet")
    pq.write_table(pa.Table.from_pandas(df_2023_02_01), "sample_parquet_dir/date=2023-02-01/data.parquet")
    os.system(f"aws --endpoint-url={S3_ENDPOINT} s3 cp sample_parquet_dir s3://{BUCKET_NAME}/sample_parquet_dir --recursive")
    shutil.rmtree("sample_parquet_dir")

# 3. Iceberg Table (Manual Structure)
def generate_iceberg_table():
    os.makedirs("sample_iceberg/data", exist_ok=True)
    os.makedirs("sample_iceberg/metadata", exist_ok=True)
    table = pa.Table.from_pandas(pandas_df)
    pq.write_table(table, "sample_iceberg/data/data.parquet")
    # Create a simple metadata.json (minimal for detection)
    metadata = {
        "format-version": 1,
        "table-uuid": "123e4567-e89b-12d3-a456-426614174000",
        "location": f"s3://{BUCKET_NAME}/sample_iceberg",
        "last-updated-ms": int(datetime.now().timestamp() * 1000),
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "id", "type": "int", "required": True},
                {"id": 2, "name": "name", "type": "string", "required": False},
                {"id": 3, "name": "date", "type": "date", "required": False},
                {"id": 4, "name": "value", "type": "int", "required": False}
            ]
        },
        "partition-spec": [{"field": "date", "source-id": 3, "transform": "identity"}],
        "snapshots": [
            {
                "snapshot-id": 1,
                "timestamp-ms": int(datetime.now().timestamp() * 1000),
                "manifest-list": "metadata/manifest-list.avro"
            }
        ]
    }
    with open("sample_iceberg/metadata/metadata.json", "w") as f:
        json.dump(metadata, f)
    # Minimal manifest list (not fully functional but enough for detection)
    with open("sample_iceberg/metadata/manifest-list.avro", "w") as f:
        f.write("placeholder")  # Simplistic for detection
    os.system(f"aws --endpoint-url={S3_ENDPOINT} s3 cp sample_iceberg s3://{BUCKET_NAME}/sample_iceberg --recursive")
    shutil.rmtree("sample_iceberg")

# 4. Delta Table
def generate_delta_table():
    table_path = f"s3://{BUCKET_NAME}/sample_delta"
    storage_options = {
        "AWS_ENDPOINT_URL": S3_ENDPOINT,
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "ALLOW_HTTP": "true"
    }
    write_deltalake(
        table_or_uri=table_path,
        data=pa.Table.from_pandas(pandas_df),
        partition_by=["date"],
        mode="append",
        storage_options=storage_options
    )

# 5. Hudi Table (Simplified)
def generate_hudi_table():
    os.makedirs("sample_hudi/.hoodie", exist_ok=True)
    with open("sample_hudi/.hoodie/hoodie.properties", "w") as f:
        f.write("hoodie.table.name=sample_hudi\n")
        f.write("hoodie.datasource.write.recordkey.field=id\n")
    pq.write_table(pa.Table.from_pandas(pandas_df), "sample_hudi/data.parquet")
    os.system(f"aws --endpoint-url={S3_ENDPOINT} s3 cp sample_hudi s3://{BUCKET_NAME}/sample_hudi --recursive")
    shutil.rmtree("sample_hudi")

# Generate and Upload
print("Generating sample tables...")
generate_parquet_file()
generate_parquet_directory()
generate_iceberg_table()
generate_delta_table()
generate_hudi_table()
print("Done! Tables uploaded to s3://test-bucket/")