import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from deltalake.writer import write_deltalake
import shutil
import boto3
import time
import json
from deltalake import DeltaTable

# MinIO Configuration
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
BUCKET_NAME = "test-bucket"

s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Expanded Sample Data with More Updates and Partitions
initial_data = [
    {"id": 1, "name": "Alice", "date": datetime(2023, 1, 1), "value": 100, "region": "North", "category": "A"},
    {"id": 2, "name": "Bob", "date": datetime(2023, 1, 2), "value": 200, "region": "South", "category": "B"},
    {"id": 3, "name": "Cathy", "date": datetime(2023, 2, 1), "value": 300, "region": "East", "category": "A"},
    {"id": 4, "name": "David", "date": datetime(2023, 3, 1), "value": 400, "region": "West", "category": "C"},
]
update_data_v1 = [
    {"id": 1, "name": "Alice", "date": datetime(2023, 1, 1), "value": 150, "region": "North", "category": "A"},
    {"id": 5, "name": "Eve", "date": datetime(2023, 4, 1), "value": 500, "region": "Central", "category": "D"},
]
update_data_v2 = [
    {"id": 2, "name": "Bob", "date": datetime(2023, 1, 2), "value": 250, "region": "South", "category": "B"},
    {"id": 6, "name": "Frank", "date": datetime(2023, 5, 1), "value": 600, "region": "Northwest", "category": "E"},
]
update_data_v3 = [
    {"id": 3, "name": "Cathy", "date": datetime(2023, 2, 1), "value": 350, "region": "East", "category": "A"},
    {"id": 7, "name": "Grace", "date": datetime(2023, 6, 1), "value": 700, "region": "Southwest", "category": "F"},
]
update_data_v4 = [
    {"id": 4, "name": "David", "date": datetime(2023, 3, 1), "value": 450, "region": "West", "category": "C"},
    {"id": 8, "name": "Hank", "date": datetime(2023, 7, 1), "value": 800, "region": "Northeast", "category": "G"},
]
update_data_v5 = [
    {"id": 5, "name": "Eve", "date": datetime(2023, 4, 1), "value": 550, "region": "Central", "category": "D"},
    {"id": 9, "name": "Ivy", "date": datetime(2023, 8, 1), "value": 900, "region": "Southeast", "category": "H"},
]

pandas_df_initial = pd.DataFrame(initial_data)
pandas_df_v1 = pd.DataFrame(update_data_v1)
pandas_df_v2 = pd.DataFrame(update_data_v2)
pandas_df_v3 = pd.DataFrame(update_data_v3)
pandas_df_v4 = pd.DataFrame(update_data_v4)
pandas_df_v5 = pd.DataFrame(update_data_v5)

def delete_s3_prefix(bucket, prefix):
    """Delete all objects under a given S3 prefix."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" in response:
            for obj in response["Contents"]:
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
        print(f"Deleted existing objects under s3://{bucket}/{prefix}")
    except Exception as e:
        print(f"Failed to delete s3://{bucket}/{prefix}: {e}")

def generate_parquet_file():
    """Generate multiple Parquet snapshots (6 versions)."""
    try:
        # Initial snapshot
        table_initial = pa.Table.from_pandas(pandas_df_initial)
        pq.write_table(table_initial, "sample_parquet_v0.parquet")
        s3_client.upload_file("sample_parquet_v0.parquet", BUCKET_NAME, "sample_parquet/sample_parquet_v0.parquet")
        
        # Snapshot v1
        table_v1 = pa.Table.from_pandas(pandas_df_v1)
        pq.write_table(table_v1, "sample_parquet_v1.parquet")
        s3_client.upload_file("sample_parquet_v1.parquet", BUCKET_NAME, "sample_parquet/sample_parquet_v1.parquet")
        
        # Snapshot v2
        table_v2 = pa.Table.from_pandas(pandas_df_v2)
        pq.write_table(table_v2, "sample_parquet_v2.parquet")
        s3_client.upload_file("sample_parquet_v2.parquet", BUCKET_NAME, "sample_parquet/sample_parquet_v2.parquet")
        
        # Snapshot v3
        table_v3 = pa.Table.from_pandas(pandas_df_v3)
        pq.write_table(table_v3, "sample_parquet_v3.parquet")
        s3_client.upload_file("sample_parquet_v3.parquet", BUCKET_NAME, "sample_parquet/sample_parquet_v3.parquet")
        
        # Snapshot v4
        table_v4 = pa.Table.from_pandas(pandas_df_v4)
        pq.write_table(table_v4, "sample_parquet_v4.parquet")
        s3_client.upload_file("sample_parquet_v4.parquet", BUCKET_NAME, "sample_parquet/sample_parquet_v4.parquet")
        
        # Snapshot v5
        table_v5 = pa.Table.from_pandas(pandas_df_v5)
        pq.write_table(table_v5, "sample_parquet_v5.parquet")
        s3_client.upload_file("sample_parquet_v5.parquet", BUCKET_NAME, "sample_parquet/sample_parquet_v5.parquet")
        
        print("Uploaded 6 snapshots to s3://test-bucket/sample_parquet/")
    except Exception as e:
        print(f"Failed to generate sample_parquet snapshots: {e}")
    finally:
        for f in ["sample_parquet_v0.parquet", "sample_parquet_v1.parquet", 
                 "sample_parquet_v2.parquet", "sample_parquet_v3.parquet",
                 "sample_parquet_v4.parquet", "sample_parquet_v5.parquet"]:
            if os.path.exists(f):
                os.remove(f)

def generate_parquet_directory():
    """Generate Parquet directory with more partitions."""
    partition_dirs = [
        "region=North/date=2023-01-01/category=A",
        "region=South/date=2023-01-02/category=B",
        "region=East/date=2023-02-01/category=A",
        "region=West/date=2023-03-01/category=C",
        "region=Central/date=2023-04-01/category=D",
        "region=Northwest/date=2023-05-01/category=E",
        "region=Southwest/date=2023-06-01/category=F",
        "region=Northeast/date=2023-07-01/category=G",
        "region=Southeast/date=2023-08-01/category=H",
    ]
    for partition in partition_dirs:
        os.makedirs(f"sample_parquet_dir/{partition}", exist_ok=True)
    
    try:
        df_north = pandas_df_initial[(pandas_df_initial["region"] == "North") & (pandas_df_initial["category"] == "A")]
        df_south = pandas_df_initial[(pandas_df_initial["region"] == "South") & (pandas_df_initial["category"] == "B")]
        df_east = pandas_df_initial[(pandas_df_initial["region"] == "East") & (pandas_df_initial["category"] == "A")]
        df_west = pandas_df_initial[(pandas_df_initial["region"] == "West") & (pandas_df_initial["category"] == "C")]
        df_central = pandas_df_v1[(pandas_df_v1["region"] == "Central") & (pandas_df_v1["category"] == "D")]
        df_northwest = pandas_df_v2[(pandas_df_v2["region"] == "Northwest") & (pandas_df_v2["category"] == "E")]
        df_southwest = pandas_df_v3[(pandas_df_v3["region"] == "Southwest") & (pandas_df_v3["category"] == "F")]
        df_northeast = pandas_df_v4[(pandas_df_v4["region"] == "Northeast") & (pandas_df_v4["category"] == "G")]
        df_southeast = pandas_df_v5[(pandas_df_v5["region"] == "Southeast") & (pandas_df_v5["category"] == "H")]

        pq.write_table(pa.Table.from_pandas(df_north), "sample_parquet_dir/region=North/date=2023-01-01/category=A/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_south), "sample_parquet_dir/region=South/date=2023-01-02/category=B/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_east), "sample_parquet_dir/region=East/date=2023-02-01/category=A/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_west), "sample_parquet_dir/region=West/date=2023-03-01/category=C/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_central), "sample_parquet_dir/region=Central/date=2023-04-01/category=D/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_northwest), "sample_parquet_dir/region=Northwest/date=2023-05-01/category=E/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_southwest), "sample_parquet_dir/region=Southwest/date=2023-06-01/category=F/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_northeast), "sample_parquet_dir/region=Northeast/date=2023-07-01/category=G/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_southeast), "sample_parquet_dir/region=Southeast/date=2023-08-01/category=H/data.parquet")
        
        for root, _, files in os.walk("sample_parquet_dir"):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, "sample_parquet_dir")
                s3_path = f"sample_parquet_dir/{relative_path.replace(os.sep, '/')}"
                s3_client.upload_file(local_path, BUCKET_NAME, s3_path)
        print("Uploaded sample_parquet_dir/ to s3://test-bucket/ with 9 partitions (region, date, category)")
    except Exception as e:
        print(f"Failed to generate sample_parquet_dir/: {e}")
    finally:
        if os.path.exists("sample_parquet_dir"):
            shutil.rmtree("sample_parquet_dir")

def generate_delta_table():
    """Generate Delta table with 6 versions."""
    table_path = f"s3://{BUCKET_NAME}/sample_delta"
    storage_options = {
        "AWS_ENDPOINT_URL": S3_ENDPOINT,
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "ALLOW_HTTP": "true"
    }
    try:
        delete_s3_prefix(BUCKET_NAME, "sample_delta")
        dfs = [pandas_df_initial, pandas_df_v1, pandas_df_v2, pandas_df_v3, pandas_df_v4, pandas_df_v5]
        for df in dfs:
            df["date"] = df["date"].astype("datetime64[us]")

        write_deltalake(table_path, pa.Table.from_pandas(pandas_df_initial), 
                       partition_by=["region", "date", "category"], mode="append", 
                       storage_options=storage_options)
        print("Initial Delta table written (version 0)")
        time.sleep(2)

        dt = DeltaTable(table_path, storage_options=storage_options)
        for version, new_df in enumerate([pandas_df_v1, pandas_df_v2, pandas_df_v3, pandas_df_v4, pandas_df_v5], 1):
            new_table = pa.Table.from_pandas(new_df)
            existing_table = dt.to_pyarrow_table()
            existing_df = existing_table.to_pandas()
            new_df = new_table.to_pandas()
            merged_df = existing_df.merge(new_df, on="id", how="outer", suffixes=("", "_new"))
            for col in new_df.columns:
                if col != "id":
                    merged_df[col] = merged_df[f"{col}_new"].combine_first(merged_df[col])
            merged_df = merged_df[existing_df.columns]
            merged_table = pa.Table.from_pandas(merged_df)
            write_deltalake(table_path, merged_table, partition_by=["region", "date", "category"], 
                          mode="overwrite", storage_options=storage_options)
            print(f"Delta table updated with v{version} data (version {version})")
            time.sleep(2)
            dt = DeltaTable(table_path, storage_options=storage_options)

        print("Uploaded sample_delta/ to s3://test-bucket/ with 6 snapshots")
        history = dt.history()
        print("Delta Table History:", history)
    except Exception as e:
        print(f"Failed to generate sample_delta/: {e}")

def generate_hudi_table():
    """Generate Hudi table with more partitions and 6 detailed snapshots."""
    base_path = "sample_hudi"
    partitions = [
        "region=North/date=2023-01-01/category=A",
        "region=South/date=2023-01-02/category=B",
        "region=East/date=2023-02-01/category=A",
        "region=West/date=2023-03-01/category=C",
        "region=Central/date=2023-04-01/category=D",
        "region=Northwest/date=2023-05-01/category=E",
        "region=Southwest/date=2023-06-01/category=F",
        "region=Northeast/date=2023-07-01/category=G",
        "region=Southeast/date=2023-08-01/category=H",
    ]
    os.makedirs(f"{base_path}/.hoodie", exist_ok=True)
    for partition in partitions:
        os.makedirs(f"{base_path}/{partition}", exist_ok=True)

    with open(f"{base_path}/.hoodie/hoodie.properties", "w") as f:
        f.write("hoodie.table.name=sample_hudi\n")
        f.write("hoodie.table.type=COPY_ON_WRITE\n")
        f.write("hoodie.datasource.write.recordkey.field=id\n")
        f.write("hoodie.datasource.write.partitionpath.field=region,date,category\n")

    try:
        # Initial data (v0)
        df_north_initial = pandas_df_initial[(pandas_df_initial["region"] == "North") & (pandas_df_initial["category"] == "A")]
        df_south_initial = pandas_df_initial[(pandas_df_initial["region"] == "South") & (pandas_df_initial["category"] == "B")]
        df_east_initial = pandas_df_initial[(pandas_df_initial["region"] == "East") & (pandas_df_initial["category"] == "A")]
        df_west_initial = pandas_df_initial[(pandas_df_initial["region"] == "West") & (pandas_df_initial["category"] == "C")]
        
        pq.write_table(pa.Table.from_pandas(df_north_initial), f"{base_path}/region=North/date=2023-01-01/category=A/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_south_initial), f"{base_path}/region=South/date=2023-01-02/category=B/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_east_initial), f"{base_path}/region=East/date=2023-02-01/category=A/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_west_initial), f"{base_path}/region=West/date=2023-03-01/category=C/data.parquet")

        # Updates and new data (v1-v5)
        df_central_v1 = pandas_df_v1[(pandas_df_v1["region"] == "Central") & (pandas_df_v1["category"] == "D")]
        df_north_v1 = pandas_df_v1[(pandas_df_v1["region"] == "North") & (pandas_df_v1["category"] == "A")]
        df_northwest_v2 = pandas_df_v2[(pandas_df_v2["region"] == "Northwest") & (pandas_df_v2["category"] == "E")]
        df_south_v2 = pandas_df_v2[(pandas_df_v2["region"] == "South") & (pandas_df_v2["category"] == "B")]
        df_southwest_v3 = pandas_df_v3[(pandas_df_v3["region"] == "Southwest") & (pandas_df_v3["category"] == "F")]
        df_east_v3 = pandas_df_v3[(pandas_df_v3["region"] == "East") & (pandas_df_v3["category"] == "A")]
        df_northeast_v4 = pandas_df_v4[(pandas_df_v4["region"] == "Northeast") & (pandas_df_v4["category"] == "G")]
        df_west_v4 = pandas_df_v4[(pandas_df_v4["region"] == "West") & (pandas_df_v4["category"] == "C")]
        df_southeast_v5 = pandas_df_v5[(pandas_df_v5["region"] == "Southeast") & (pandas_df_v5["category"] == "H")]
        df_central_v5 = pandas_df_v5[(pandas_df_v5["region"] == "Central") & (pandas_df_v5["category"] == "D")]

        pq.write_table(pa.Table.from_pandas(df_central_v1), f"{base_path}/region=Central/date=2023-04-01/category=D/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_north_v1), f"{base_path}/region=North/date=2023-01-01/category=A/data_v1.parquet")
        pq.write_table(pa.Table.from_pandas(df_northwest_v2), f"{base_path}/region=Northwest/date=2023-05-01/category=E/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_south_v2), f"{base_path}/region=South/date=2023-01-02/category=B/data_v2.parquet")
        pq.write_table(pa.Table.from_pandas(df_southwest_v3), f"{base_path}/region=Southwest/date=2023-06-01/category=F/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_east_v3), f"{base_path}/region=East/date=2023-02-01/category=A/data_v3.parquet")
        pq.write_table(pa.Table.from_pandas(df_northeast_v4), f"{base_path}/region=Northeast/date=2023-07-01/category=G/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_west_v4), f"{base_path}/region=West/date=2023-03-01/category=C/data_v4.parquet")
        pq.write_table(pa.Table.from_pandas(df_southeast_v5), f"{base_path}/region=Southeast/date=2023-08-01/category=H/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_central_v5), f"{base_path}/region=Central/date=2023-04-01/category=D/data_v5.parquet")

        # Simulate detailed commits for snapshots
        base_time = datetime(2023, 1, 1, 0, 0, 0)
        commit_metadata = [
            {"operation": "commit", "numRecords": 4, "is_current": False},  # Initial
            {"operation": "commit", "numRecords": 2, "is_current": False},  # v1
            {"operation": "commit", "numRecords": 2, "is_current": False},  # v2
            {"operation": "commit", "numRecords": 2, "is_current": False},  # v3
            {"operation": "commit", "numRecords": 2, "is_current": False},  # v4
            {"operation": "commit", "numRecords": 2, "is_current": True},   # v5 (latest)
        ]
        for i, meta in enumerate(commit_metadata):
            commit_time = (base_time + timedelta(hours=i)).strftime("%Y%m%d%H%M%S")
            commit_data = {
                "timestamp": commit_time,
                "version": i,
                "operation": meta["operation"],
                "operation_metrics": {"numRecords": meta["numRecords"]},
                "is_current": meta["is_current"],
                "partitionToWriteStats": {
                    p: {"numAdds": len([df for df in [df_north_initial, df_south_initial, df_east_initial, df_west_initial,
                                                      df_central_v1, df_north_v1, df_northwest_v2, df_south_v2,
                                                      df_southwest_v3, df_east_v3, df_northeast_v4, df_west_v4,
                                                      df_southeast_v5, df_central_v5] if not df.empty and p in f"{df.iloc[0]['region']}/date={df.iloc[0]['date'].strftime('%Y-%m-%d')}/category={df.iloc[0]['category']}"])}
                    for p in partitions
                }
            }
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=f"sample_hudi/.hoodie/{commit_time}.commit",
                Body=json.dumps(commit_data)
            )

        # Upload all files to S3
        for root, _, files in os.walk(base_path):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, base_path)
                s3_path = f"sample_hudi/{relative_path.replace(os.sep, '/')}"
                s3_client.upload_file(local_path, BUCKET_NAME, s3_path)
        print("Uploaded sample_hudi/ to s3://test-bucket/ with 9 partitions and 6 detailed snapshots")
    except Exception as e:
        print(f"Failed to generate sample_hudi/: {e}")
    finally:
        if os.path.exists(base_path):
            shutil.rmtree(base_path)

if __name__ == "__main__":
    print("Generating sample tables...")
    generate_parquet_file()
    generate_parquet_directory()
    generate_delta_table()
    generate_hudi_table()
    print("Done! Tables uploaded to s3://test-bucket/")