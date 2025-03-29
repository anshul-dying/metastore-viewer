import duckdb

# Connect to DuckDB
conn = duckdb.connect()

# Install & load HTTPFS extension for S3 access
conn.execute("INSTALL httpfs; LOAD httpfs;")

# Set MinIO connection details
conn.execute(f"""
    SET s3_endpoint = 'localhost:9000';  -- Ensure the correct MinIO port
    SET s3_access_key_id = 'admin';
    SET s3_secret_access_key = 'password';
    SET s3_url_style = 'path';
    SET s3_use_ssl = false;  -- Disable SSL for local MinIO
""")

# Query the Parquet file from MinIO
query = "SELECT MaxTemp FROM 's3://test-bucket/weather.parquet' LIMIT 10;"
df = conn.execute(query).fetchdf()

# Print results
print(df)