import os
import boto3
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta
import random
from deltalake import write_deltalake
import duckdb

# MinIO Configuration
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "test-bucket"

# Set up S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def generate_data(num_records):
    """Generate sample data with partitions."""
    now = datetime.now()
    return pd.DataFrame({
        'id': [f'ID-{i:05d}' for i in range(1, num_records+1)],
        'name': [f'User_{i}' for i in range(1, num_records+1)],
        'email': [f'user_{i}@company.com' for i in range(1, num_records+1)],
        'age': [random.randint(18, 65) for _ in range(num_records)],
        'salary': [round(random.uniform(30000, 120000), 2) for _ in range(num_records)],
        'country': [random.choice(['US', 'UK', 'IN', 'DE', 'FR']) for _ in range(num_records)],
        'department': [random.choice(['HR', 'Engineering', 'Finance']) for _ in range(num_records)],
        'join_date': [(now - timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d') for _ in range(num_records)]
    })

def generate_delta_snapshots():
    """Generate 5+ Delta Lake snapshots."""
    print("\n🚀 Generating Delta Lake snapshots...")
    
    # Configure Delta Lake
    os.environ.update({
        "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
        "AWS_ALLOW_HTTP": "true"
    })
    
    base_data = generate_data(200)
    delta_path = f"s3://{BUCKET_NAME}/delta_table"
    columns = base_data.columns.tolist()  # Store the original schema
    
    for version in range(1, 6):
        if version == 1:
            write_deltalake(delta_path, base_data, partition_by=["country", "department"], mode="overwrite")
        else:
            updates = base_data.sample(50).copy()
            updates['salary'] *= 1.1  # Increase salary by 10%

            # **Ensure all columns match the original schema**
            missing_cols = set(columns) - set(updates.columns)
            for col in missing_cols:
                updates[col] = None  # Add missing columns with default values

            updates = updates[columns]  # Enforce column order
            print(f"Appending update with columns: {updates.columns.tolist()}")  # Debugging output
            write_deltalake(delta_path, updates, mode="append")  
        
        print(f"✅ Delta Snapshot {version} created")


def generate_hudi_snapshots():
    """Generate 5+ Hudi snapshots using DuckDB."""
    print("\n🔥 Generating Hudi snapshots with DuckDB...")
    hudi_path = f"s3://{BUCKET_NAME}/hudi_table"
    conn = duckdb.connect()
    
    for version in range(1, 6):
        data = generate_data(200)
        table_name = f"hudi_snapshot_v{version}"
        
        conn.execute(f"""
            CREATE TABLE {table_name} AS SELECT * FROM data;
        """)
        
        file_path = f"{hudi_path}/snapshot_v{version}.parquet"
        conn.execute(f"""
            COPY {table_name} TO '{file_path}' (FORMAT PARQUET);
        """)
        print(f"✅ Hudi Snapshot {version} created at {file_path}")

if __name__ == "__main__":
    print("\n✨ Starting Snapshot Generation ✨")
    generate_delta_snapshots()
    generate_hudi_snapshots()
    print("\n🎉 All Snapshots Created! 🎉")
