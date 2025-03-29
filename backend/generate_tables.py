import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from deltalake.writer import write_deltalake
import shutil
import boto3
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, TimestampType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

# MinIO Configuration
S3_ENDPOINT = "http://localhost:9000"
AWS_ACCESS_KEY_ID = "admin"
AWS_SECRET_ACCESS_KEY = "password"
BUCKET_NAME = "test-bucket"

s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Sample Data
data = [
    {"id": 1, "name": "Alice", "date": datetime(2023, 1, 1), "value": 100},
    {"id": 2, "name": "Bob", "date": datetime(2023, 1, 2), "value": 200},
    {"id": 3, "name": "Cathy", "date": datetime(2023, 2, 1), "value": 300}
]
pandas_df = pd.DataFrame(data)

data_v2 = [
    {"id": 4, "name": "David", "date": datetime(2023, 3, 1), "value": 400},
    {"id": 5, "name": "Eve", "date": datetime(2023, 3, 2), "value": 500}
]
pandas_df_v2 = pd.DataFrame(data_v2)

data_v3 = [
    {"id": 6, "name": "Frank", "date": datetime(2023, 4, 1), "value": 600}
]
pandas_df_v3 = pd.DataFrame(data_v3)

# 1. Standalone Parquet File
def generate_parquet_file():
    table = pa.Table.from_pandas(pandas_df)
    pq.write_table(table, "sample_parquet.parquet")
    s3_client.upload_file("sample_parquet.parquet", BUCKET_NAME, "sample_parquet.parquet")
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
    for root, _, files in os.walk("sample_parquet_dir"):
        for file in files:
            local_path = os.path.join(root, file)
            s3_path = f"sample_parquet_dir/{local_path.split('sample_parquet_dir/')[1]}"
            s3_client.upload_file(local_path, BUCKET_NAME, s3_path)
    shutil.rmtree("sample_parquet_dir")

# 3. Iceberg Table
def generate_iceberg_table():
    try:
        catalog = load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": "http://localhost:19120/api/v1",
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY_ID,
                "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
                "s3.allow-http": "true",
                "downcast-ns-timestamp-to-us-on-write": "true"
            }
        )
    except Exception as e:
        print(f"Failed to load Iceberg catalog: {e}")
        return

    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "date", TimestampType(), required=False),
        NestedField(4, "value", IntegerType(), required=False)
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="date")
    )
    namespace = (BUCKET_NAME,)
    try:
        catalog.create_namespace(namespace)
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise e

    table_identifier = (BUCKET_NAME, "sample_iceberg")
    try:
        catalog.drop_table(table_identifier)
    except:
        pass
    table = catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        partition_spec=partition_spec,
        location=f"s3://{BUCKET_NAME}/sample_iceberg"
    )

    for df in [pandas_df, pandas_df_v2, pandas_df_v3]:
        df["id"] = df["id"].astype("int32")
        df["value"] = df["value"].astype("int32")
        df["date"] = df["date"].astype("datetime64[us]")
    arrow_schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("date", pa.timestamp("us"), nullable=True),
        pa.field("value", pa.int32(), nullable=True)
    ])
    table.append(pa.Table.from_pandas(pandas_df).cast(arrow_schema))
    table.append(pa.Table.from_pandas(pandas_df_v2).cast(arrow_schema))
    table.append(pa.Table.from_pandas(pandas_df_v3).cast(arrow_schema))

# 4. Delta Table
def generate_delta_table():
    table_path = f"s3://{BUCKET_NAME}/sample_delta"
    storage_options = {
        "AWS_ENDPOINT_URL": S3_ENDPOINT,
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "ALLOW_HTTP": "true"
    }
    for df in [pandas_df, pandas_df_v2, pandas_df_v3]:
        df["date"] = df["date"].astype("datetime64[us]")
    write_deltalake(table_path, pa.Table.from_pandas(pandas_df), partition_by=["date"], mode="append", storage_options=storage_options)
    write_deltalake(table_path, pa.Table.from_pandas(pandas_df_v2), partition_by=["date"], mode="append", storage_options=storage_options)
    write_deltalake(table_path, pa.Table.from_pandas(pandas_df_v3), partition_by=["date"], mode="append", storage_options=storage_options)

# 5. Hudi Table
def generate_hudi_table():
    os.makedirs("sample_hudi/.hoodie", exist_ok=True)
    with open("sample_hudi/.hoodie/hoodie.properties", "w") as f:
        f.write("hoodie.table.name=sample_hudi\n")
        f.write("hoodie.datasource.write.recordkey.field=id\n")
    pandas_df["date"] = pandas_df["date"].astype("datetime64[us]")
    pq.write_table(pa.Table.from_pandas(pandas_df), "sample_hudi/data.parquet")
    s3_client.upload_file("sample_hudi/.hoodie/hoodie.properties", BUCKET_NAME, "sample_hudi/.hoodie/hoodie.properties")
    s3_client.upload_file("sample_hudi/data.parquet", BUCKET_NAME, "sample_hudi/data.parquet")
    shutil.rmtree("sample_hudi")

if __name__ == "__main__":
    print("Generating sample tables...")
    generate_parquet_file()
    generate_parquet_directory()
    generate_iceberg_table()
    generate_delta_table()
    generate_hudi_table()
    print("Done! Tables uploaded to s3://test-bucket/")