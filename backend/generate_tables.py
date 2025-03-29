import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from deltalake.writer import write_deltalake
import shutil
import boto3

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

# Sample Data with Meaningful Updates
initial_data = [
    {"id": 1, "name": "Alice", "date": datetime(2023, 1, 1), "value": 100, "region": "North"},
    {"id": 2, "name": "Bob", "date": datetime(2023, 1, 2), "value": 200, "region": "South"},
    {"id": 3, "name": "Cathy", "date": datetime(2023, 2, 1), "value": 300, "region": "East"}
]
update_data_v1 = [
    {"id": 1, "name": "Alice", "date": datetime(2023, 1, 1), "value": 150, "region": "North"},  # Update value
    {"id": 4, "name": "David", "date": datetime(2023, 3, 1), "value": 400, "region": "West"}   # New record
]
update_data_v2 = [
    {"id": 2, "name": "Bob", "date": datetime(2023, 1, 2), "value": 250, "region": "South"},  # Update value
    {"id": 5, "name": "Eve", "date": datetime(2023, 3, 2), "value": 500, "region": "North"}   # New record
]

pandas_df_initial = pd.DataFrame(initial_data)
pandas_df_v1 = pd.DataFrame(update_data_v1)
pandas_df_v2 = pd.DataFrame(update_data_v2)

def generate_weather_parquet():
    weather_data = [
        {"city": "New York", "temperature": 20, "date": datetime(2023, 1, 1), "condition": "Sunny"},
        {"city": "London", "temperature": 15, "date": datetime(2023, 1, 1), "condition": "Rainy"}
    ]
    df = pd.DataFrame(weather_data)
    try:
        pq.write_table(pa.Table.from_pandas(df), "weather.parquet")
        s3_client.upload_file("weather.parquet", BUCKET_NAME, "weather.parquet")
        print("Uploaded weather.parquet to s3://test-bucket/")
    except Exception as e:
        print(f"Failed to generate weather.parquet: {e}")
    finally:
        if os.path.exists("weather.parquet"):
            os.remove("weather.parquet")

def generate_flights_parquet():
    flights_data = [
        {"flight_id": 1, "destination": "Paris", "date": datetime(2023, 1, 1), "status": "On Time"},
        {"flight_id": 2, "destination": "Tokyo", "date": datetime(2023, 1, 2), "status": "Delayed"}
    ]
    df = pd.DataFrame(flights_data)
    try:
        pq.write_table(pa.Table.from_pandas(df), "flights-1m.parquet")
        s3_client.upload_file("flights-1m.parquet", BUCKET_NAME, "flights-1m.parquet")
        print("Uploaded flights-1m.parquet to s3://test-bucket/")
    except Exception as e:
        print(f"Failed to generate flights-1m.parquet: {e}")
    finally:
        if os.path.exists("flights-1m.parquet"):
            os.remove("flights-1m.parquet")

def generate_parquet_file():
    table = pa.Table.from_pandas(pandas_df_initial)
    try:
        pq.write_table(table, "sample_parquet.parquet")
        s3_client.upload_file("sample_parquet.parquet", BUCKET_NAME, "sample_parquet.parquet")
        print("Uploaded sample_parquet.parquet to s3://test-bucket/")
    except Exception as e:
        print(f"Failed to generate sample_parquet.parquet: {e}")
    finally:
        if os.path.exists("sample_parquet.parquet"):
            os.remove("sample_parquet.parquet")

def generate_parquet_directory():
    os.makedirs("sample_parquet_dir/region=North/date=2023-01-01", exist_ok=True)
    os.makedirs("sample_parquet_dir/region=South/date=2023-01-02", exist_ok=True)
    os.makedirs("sample_parquet_dir/region=East/date=2023-02-01", exist_ok=True)
    df_north = pandas_df_initial[pandas_df_initial["region"] == "North"]
    df_south = pandas_df_initial[pandas_df_initial["region"] == "South"]
    df_east = pandas_df_initial[pandas_df_initial["region"] == "East"]
    try:
        pq.write_table(pa.Table.from_pandas(df_north), "sample_parquet_dir/region=North/date=2023-01-01/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_south), "sample_parquet_dir/region=South/date=2023-01-02/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_east), "sample_parquet_dir/region=East/date=2023-02-01/data.parquet")
        for root, _, files in os.walk("sample_parquet_dir"):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, "sample_parquet_dir")
                s3_path = f"sample_parquet_dir/{relative_path.replace(os.sep, '/')}"
                s3_client.upload_file(local_path, BUCKET_NAME, s3_path)
        print("Uploaded sample_parquet_dir/ to s3://test-bucket/ with partitions region and date")
    except Exception as e:
        print(f"Failed to generate sample_parquet_dir/: {e}")
    finally:
        if os.path.exists("sample_parquet_dir"):
            shutil.rmtree("sample_parquet_dir")

def generate_delta_table():
    table_path = f"s3://{BUCKET_NAME}/sample_delta"
    storage_options = {
        "AWS_ENDPOINT_URL": S3_ENDPOINT,
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "ALLOW_HTTP": "true"
    }
    try:
        # Initial write
        for df in [pandas_df_initial, pandas_df_v1, pandas_df_v2]:
            df["date"] = df["date"].astype("datetime64[us]")
        write_deltalake(
            table_path, 
            pa.Table.from_pandas(pandas_df_initial), 
            partition_by=["region", "date"], 
            mode="append", 
            storage_options=storage_options
        )
        print("Initial Delta table written with partition by region and date")

        # Update v1: Append new records and update existing
        write_deltalake(
            table_path, 
            pa.Table.from_pandas(pandas_df_v1), 
            partition_by=["region", "date"], 
            mode="append", 
            storage_options=storage_options
        )
        print("Delta table updated with v1 data (new records and updates)")

        # Update v2: Append new records and update existing
        write_deltalake(
            table_path, 
            pa.Table.from_pandas(pandas_df_v2), 
            partition_by=["region", "date"], 
            mode="append", 
            storage_options=storage_options
        )
        print("Delta table updated with v2 data (new records and updates)")
        print("Uploaded sample_delta/ to s3://test-bucket/ with meaningful snapshots")
    except Exception as e:
        print(f"Failed to generate sample_delta/: {e}")

def generate_hudi_table():
    os.makedirs("sample_hudi/region=North/date=2023-01-01", exist_ok=True)
    os.makedirs("sample_hudi/region=South/date=2023-01-02", exist_ok=True)
    os.makedirs("sample_hudi/region=East/date=2023-02-01", exist_ok=True)
    os.makedirs("sample_hudi/region=West/date=2023-03-01", exist_ok=True)
    os.makedirs("sample_hudi/.hoodie", exist_ok=True)

    # Write Hudi properties
    with open("sample_hudi/.hoodie/hoodie.properties", "w") as f:
        f.write("hoodie.table.name=sample_hudi\n")
        f.write("hoodie.datasource.write.recordkey.field=id\n")
        f.write("hoodie.datasource.write.partitionpath.field=region,date\n")

    # Partitioned data
    df_north_initial = pandas_df_initial[pandas_df_initial["region"] == "North"]
    df_south_initial = pandas_df_initial[pandas_df_initial["region"] == "South"]
    df_east_initial = pandas_df_initial[pandas_df_initial["region"] == "East"]
    df_west_v1 = pandas_df_v1[pandas_df_v1["region"] == "West"]
    df_north_v1 = pandas_df_v1[pandas_df_v1["region"] == "North"]
    df_south_v2 = pandas_df_v2[pandas_df_v2["region"] == "South"]
    df_north_v2 = pandas_df_v2[pandas_df_v2["region"] == "North"]

    try:
        # Initial write
        pq.write_table(pa.Table.from_pandas(df_north_initial), "sample_hudi/region=North/date=2023-01-01/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_south_initial), "sample_hudi/region=South/date=2023-01-02/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_east_initial), "sample_hudi/region=East/date=2023-02-01/data.parquet")

        # Simulate updates
        pq.write_table(pa.Table.from_pandas(df_west_v1), "sample_hudi/region=West/date=2023-03-01/data.parquet")
        pq.write_table(pa.Table.from_pandas(df_north_v1), "sample_hudi/region=North/date=2023-01-01/data_v1.parquet")
        pq.write_table(pa.Table.from_pandas(df_south_v2), "sample_hudi/region=South/date=2023-01-02/data_v2.parquet")
        pq.write_table(pa.Table.from_pandas(df_north_v2), "sample_hudi/region=North/date=2023-03-02/data_v2.parquet")

        # Upload to S3
        for root, _, files in os.walk("sample_hudi"):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, "sample_hudi")
                s3_path = f"sample_hudi/{relative_path.replace(os.sep, '/')}"
                s3_client.upload_file(local_path, BUCKET_NAME, s3_path)
        print("Uploaded sample_hudi/ to s3://test-bucket/ with partitions region and date, and simulated snapshots")
    except Exception as e:
        print(f"Failed to generate sample_hudi/: {e}")
    finally:
        if os.path.exists("sample_hudi"):
            shutil.rmtree("sample_hudi")

if __name__ == "__main__":
    print("Generating sample tables...")
    generate_weather_parquet()
    generate_flights_parquet()
    generate_parquet_file()
    generate_parquet_directory()
    generate_delta_table()
    generate_hudi_table()
    print("Done! Tables uploaded to s3://test-bucket/")