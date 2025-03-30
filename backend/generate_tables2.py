import os
import boto3
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta
import random
from deltalake import write_deltalake

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

def clean_directory(bucket, prefix):
    """Completely clean a directory in MinIO"""
    try:
        objects = []
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                objects.extend([{'Key': obj['Key']} for obj in page['Contents']])
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={'Objects': objects})
        print(f"🧹 Completely cleaned {prefix}")
    except Exception as e:
        print(f"⚠️ Cleaning error: {str(e)}")

def generate_data(num_records):
    """Generate perfectly consistent sample data"""
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

def generate_parquet_samples():
    """Generate perfect Parquet samples"""
    print("\n🌟 Generating Parquet files...")
    df = generate_data(200)
    
    # Clean existing data
    clean_directory(BUCKET_NAME, "sample_parquet/")
    
    # Write partitioned data
    for country in ['US', 'UK', 'IN', 'DE', 'FR']:
        for dept in ['HR', 'Engineering', 'Finance']:
            subset = df[(df.country == country) & (df.department == dept)]
            if not subset.empty:
                buffer = BytesIO()
                subset.to_parquet(buffer, engine='pyarrow')
                buffer.seek(0)
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=f"sample_parquet/country={country}/department={dept}/data.parquet",
                    Body=buffer
                )
    print("✅ Generated partitioned Parquet files")

def generate_delta_table():
    """Generate bulletproof Delta table"""
    print("\n🚀 Generating Delta Lake table...")
    
    # Clean existing table
    clean_directory(BUCKET_NAME, "sample_delta/")
    
    # Configure Delta Lake
    os.environ.update({
        "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
        "AWS_ALLOW_HTTP": "true"
    })
    
    # Generate consistent base data
    base_data = generate_data(100)
    columns = base_data.columns.tolist()
    
    # Version 1 - Initial data
    write_deltalake(
        f"s3://{BUCKET_NAME}/sample_delta",
        base_data,
        partition_by=["country", "department"],
        mode="overwrite"
    )
    print("✅ Version 1 created")
    
    # Version 2 - Updates
    updates = base_data.sample(20).copy()
    updates['salary'] = updates['salary'] * 1.1
    write_deltalake(
        f"s3://{BUCKET_NAME}/sample_delta",
        updates[columns],  # Strict column order
        mode="append"
    )
    print("✅ Version 2 (updates) added")
    
    # Version 3 - New data
    new_data = generate_data(30)
    new_data['id'] = [f'NEW-{i:03d}' for i in range(1, 31)]
    write_deltalake(
        f"s3://{BUCKET_NAME}/sample_delta",
        new_data[columns],  # Strict column order
        mode="append"
    )
    print("✅ Version 3 (new records) added")

if __name__ == "__main__":
    print("\n✨ Starting Data Generation ✨")
    generate_parquet_samples()
    generate_delta_table()
    
    print("\n🎉 All Done! 🎉")
    print("Check your MinIO bucket for:")
    print(f"- Partitioned Parquet: s3://{BUCKET_NAME}/sample_parquet")
    print(f"- Delta Table: s3://{BUCKET_NAME}/sample_delta")