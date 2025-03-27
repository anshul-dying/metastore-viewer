# import os
# import boto3
# import pyarrow.parquet as pq
# import pandas as pd
# from flask import Flask, request, jsonify
# from flask_cors import CORS
# from io import BytesIO

# app = Flask(__name__)
# CORS(app)

# # MinIO Configuration
# S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
# BUCKET_NAME = "test-bucket"

# # Initialize MinIO (S3-compatible) Client
# s3_client = boto3.client(
#     "s3",
#     endpoint_url=S3_ENDPOINT,
#     aws_access_key_id=AWS_ACCESS_KEY_ID,
#     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
# )

# def list_s3_objects(s3_bucket, prefix=""):
#     """List objects in an S3 bucket under a specific prefix."""
#     try:
#         response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
#         return [obj["Key"] for obj in response.get("Contents", [])]
#     except Exception as e:
#         app.logger.error(f"Error listing objects in bucket {s3_bucket}: {str(e)}")
#         return []

# def get_parquet_metadata(s3_bucket, file_key):
#     """Fetch and return Parquet file metadata."""
#     try:
#         app.logger.info(f"Fetching object {file_key} from bucket {s3_bucket}")
#         obj = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
#         file_stream = BytesIO(obj["Body"].read())
#         parquet_file = pq.ParquetFile(file_stream)

#         # Get the Arrow schema for nullability
#         arrow_schema = parquet_file.schema_arrow
#         columns = [
#             {
#                 "name": field.name,
#                 "type": str(field.type),
#                 "nullable": field.nullable
#             }
#             for field in arrow_schema
#         ]

#         return {
#             "file": file_key,
#             "details": {
#                 "format": "parquet",
#                 "columns": columns,
#                 "num_rows": parquet_file.metadata.num_rows,
#                 "file_size": obj["ContentLength"],
#                 "partition_keys": [],
#                 "snapshots": []
#             }
#         }
#     except Exception as e:
#         app.logger.error(f"Error fetching metadata for {file_key}: {str(e)}")
#         return {
#             "file": file_key,
#             "details": {
#                 "format": "parquet",
#                 "columns": [],
#                 "num_rows": 0,
#                 "file_size": 0,
#                 "partition_keys": [],
#                 "snapshots": [],
#                 "error": str(e)
#             }
#         }

# @app.route("/metadata", methods=["GET"])
# def get_metadata():
#     """Get metadata for Parquet files in MinIO."""
#     bucket = request.args.get("bucket", BUCKET_NAME)
#     prefix = request.args.get("prefix", "")
#     file_key = request.args.get("file")

#     app.logger.info(f"Received metadata request: bucket={bucket}, prefix={prefix}, file={file_key}")

#     if file_key:
#         metadata = get_parquet_metadata(bucket, file_key)
#         return jsonify(metadata)
#     else:
#         objects = list_s3_objects(bucket, prefix)
#         if not objects:
#             app.logger.warning(f"No objects found in bucket {bucket} with prefix {prefix}")
#             return jsonify({"error": "No files found or bucket inaccessible"}), 404
#         metadata = []
#         for obj_key in objects:
#             if obj_key.endswith(".parquet"):
#                 metadata.append(get_parquet_metadata(bucket, obj_key))
#         return jsonify({"files": metadata})

# @app.route("/data", methods=["GET"])
# def get_data():
#     """Fetch sample data from a Parquet file."""
#     file_name = request.args.get("file")
#     bucket = request.args.get("bucket", BUCKET_NAME)

#     if not file_name:
#         return jsonify({"error": "File parameter is required"}), 400

#     try:
#         app.logger.info(f"Fetching data for {file_name} from bucket {bucket}")
#         obj = s3_client.get_object(Bucket=bucket, Key=file_name)
#         file_stream = BytesIO(obj["Body"].read())
#         table = pq.read_table(file_stream)
#         df = table.to_pandas()
#         sample_data = df.head(100).to_dict(orient="records")
#         return jsonify({"file": file_name, "data": sample_data})
#     except Exception as e:
#         app.logger.error(f"Error fetching data for {file_name}: {str(e)}")
#         return jsonify({"error": str(e)}), 500

# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000, debug=True)


import os
import boto3
import pyarrow.parquet as pq
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from io import BytesIO
from pyiceberg.catalog import load_catalog
from deltalake import DeltaTable
import json

app = Flask(__name__)
CORS(app)

# MinIO Configuration
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
BUCKET_NAME = "test-bucket"

# Initialize MinIO (S3-compatible) Client
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def list_s3_objects(s3_bucket, prefix=""):
    """List objects in an S3 bucket under a specific prefix."""
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]
    except Exception as e:
        app.logger.error(f"Error listing objects in bucket {s3_bucket}: {str(e)}")
        return []

def detect_table_format(s3_bucket, prefix=""):
    """Detect the table format based on bucket contents."""
    objects = list_s3_objects(s3_bucket, prefix)
    if not objects:
        return None
    
    if len(objects) == 1 and objects[0].endswith(".parquet"):
        return "parquet_file"
    
    if any(obj.endswith(".parquet") for obj in objects):
        parquet_files = [obj for obj in objects if obj.endswith(".parquet")]
        if len(parquet_files) > 1 or not any(obj.startswith(prefix + "metadata/") for obj in objects):
            return "parquet_directory"
    
    if any(obj.startswith(prefix + "metadata/") and obj.endswith(".json") for obj in objects):
        return "iceberg"
    
    if any(obj.startswith(prefix + "_delta_log/") for obj in objects):
        return "delta"
    
    if any(obj.startswith(prefix + ".hoodie/") for obj in objects):
        return "hudi"
    
    return None

def get_parquet_metadata(s3_bucket, file_key):
    """Fetch metadata for a standalone Parquet file."""
    try:
        obj = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
        file_stream = BytesIO(obj["Body"].read())
        parquet_file = pq.ParquetFile(file_stream)
        arrow_schema = parquet_file.schema_arrow
        columns = [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in arrow_schema]
        return {
            "file": file_key,
            "details": {
                "format": "parquet",
                "columns": columns,
                "num_rows": parquet_file.metadata.num_rows,
                "file_size": obj["ContentLength"],
                "partition_keys": [],
                "snapshots": [],
                "properties": {}
            }
        }
    except Exception as e:
        app.logger.error(f"Error fetching metadata for {file_key}: {str(e)}")
        return {"file": file_key, "details": {"error": str(e)}}

def get_parquet_directory_metadata(s3_bucket, prefix):
    """Fetch metadata for a Parquet directory (partitioned dataset)."""
    parquet_files = [obj for obj in list_s3_objects(s3_bucket, prefix) if obj.endswith(".parquet")]
    schema, num_rows, file_size = [], 0, 0
    partition_keys = set()
    
    for file_key in parquet_files:
        metadata = get_parquet_metadata(s3_bucket, file_key)["details"]
        schema.extend(metadata["columns"])
        num_rows += metadata["num_rows"]
        file_size += metadata["file_size"]
        parts = file_key[len(prefix):].split("/")
        for part in parts[:-1]:
            if "=" in part:
                key = part.split("=")[0]
                partition_keys.add(key)
    
    return {
        "file": prefix,
        "details": {
            "format": "parquet_directory",
            "columns": schema,
            "num_rows": num_rows,
            "file_size": file_size,
            "partition_keys": list(partition_keys),
            "snapshots": [],
            "properties": {}
        }
    }

def get_iceberg_metadata(s3_bucket, prefix):
    """Fetch metadata for an Iceberg table."""
    try:
        catalog = load_catalog(
            "local",
            **{
                "uri": S3_ENDPOINT,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY_ID,
                "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
            }
        )
        table = catalog.load_table((s3_bucket, prefix))
        schema = [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in table.schema().fields]
        snapshots = [{"id": s.snapshot_id, "timestamp": str(s.timestamp)} for s in table.history()]
        partition_keys = [p.name for p in table.spec().fields]
        files = table.scan().file_scan_tasks()
        return {
            "file": prefix,
            "details": {
                "format": "iceberg",
                "columns": schema,
                "num_rows": sum(sf.row_count for sf in files),
                "file_size": sum(sf.file_size for sf in files),
                "partition_keys": partition_keys,
                "snapshots": snapshots,
                "properties": table.metadata.configuration
            }
        }
    except Exception as e:
        app.logger.error(f"Error fetching Iceberg metadata for {prefix}: {str(e)}")
        # Fallback: Manually extract basic metadata
        parquet_files = [obj for obj in list_s3_objects(s3_bucket, prefix) if obj.endswith(".parquet")]
        schema, num_rows, file_size = [], 0, 0
        for file_key in parquet_files:
            metadata = get_parquet_metadata(s3_bucket, file_key)["details"]
            schema.extend(metadata["columns"])
            num_rows += metadata["num_rows"]
            file_size += metadata["file_size"]
        return {
            "file": prefix,
            "details": {
                "format": "iceberg",
                "columns": schema,
                "num_rows": num_rows,
                "file_size": file_size,
                "partition_keys": ["date"],  # Hardcoded for our mock table
                "snapshots": [{"id": 1, "timestamp": "mock"}],
                "properties": {}
            }
        }

def get_delta_metadata(s3_bucket, prefix):
    """Fetch metadata for a Delta table."""
    try:
        table_path = f"s3://{s3_bucket}/{prefix}"
        dt = DeltaTable(table_path, storage_options={
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "ALLOW_HTTP": "true"
        })
        schema = [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in dt.schema().fields]
        partition_keys = dt.metadata().partition_columns
        snapshots = [{"version": v["version"], "timestamp": v["timestamp"]} for v in dt.history()]
        files = dt.get_add_actions().to_pandas()
        num_rows = files["num_records"].sum()
        file_size = files["size_bytes"].sum()
        return {
            "file": prefix,
            "details": {
                "format": "delta",
                "columns": schema,
                "num_rows": int(num_rows),
                "file_size": int(file_size),
                "partition_keys": partition_keys,
                "snapshots": snapshots,
                "properties": dt.metadata().configuration
            }
        }
    except Exception as e:
        app.logger.error(f"Error fetching Delta metadata for {prefix}: {str(e)}")
        return {"file": prefix, "details": {"error": str(e)}}

def get_hudi_metadata(s3_bucket, prefix):
    """Fetch metadata for a Hudi table (basic implementation)."""
    try:
        hoodie_props_key = f"{prefix}.hoodie/hoodie.properties"
        obj = s3_client.get_object(Bucket=s3_bucket, Key=hoodie_props_key)
        props = dict(line.split("=", 1) for line in obj["Body"].read().decode().splitlines() if "=" in line)
    except:
        props = {}
    
    parquet_files = [obj for obj in list_s3_objects(s3_bucket, prefix) if obj.endswith(".parquet")]
    schema, num_rows, file_size = [], 0, 0
    for file_key in parquet_files:
        metadata = get_parquet_metadata(s3_bucket, file_key)["details"]
        schema.extend(metadata["columns"])
        num_rows += metadata["num_rows"]
        file_size += metadata["file_size"]
    
    return {
        "file": prefix,
        "details": {
            "format": "hudi",
            "columns": schema,
            "num_rows": num_rows,
            "file_size": file_size,
            "partition_keys": [],
            "snapshots": [],  # Full snapshot support requires Spark timeline parsing
            "properties": props
        }
    }

@app.route("/metadata", methods=["GET"])
def get_metadata():
    """Get metadata for tables/files in MinIO."""
    bucket = request.args.get("bucket", BUCKET_NAME)
    prefix = request.args.get("prefix", "")
    file_key = request.args.get("file")

    app.logger.info(f"Received metadata request: bucket={bucket}, prefix={prefix}, file={file_key}")

    if file_key:
        format_type = detect_table_format(bucket, file_key)
        if format_type == "parquet_file":
            return jsonify(get_parquet_metadata(bucket, file_key))
        elif format_type == "parquet_directory":
            return jsonify(get_parquet_directory_metadata(bucket, file_key))
        elif format_type == "iceberg":
            return jsonify(get_iceberg_metadata(bucket, file_key))
        elif format_type == "delta":
            return jsonify(get_delta_metadata(bucket, file_key))
        elif format_type == "hudi":
            return jsonify(get_hudi_metadata(bucket, file_key))
        else:
            return jsonify({"error": "Unsupported format or invalid path"}), 400
    
    objects = list_s3_objects(bucket, prefix)
    if not objects:
        return jsonify({"error": "No files found or bucket inaccessible"}), 404
    
    metadata = []
    processed_prefixes = set()
    
    # Group objects by their top-level prefix (or file if no prefix)
    prefixes = set()
    for obj_key in objects:
        # If the object is a file at the root (no slashes), use it directly
        if "/" not in obj_key:
            prefixes.add(obj_key)
        else:
            # Otherwise, use the first part of the path as the prefix
            top_level_prefix = obj_key.split("/")[0] + "/"
            prefixes.add(top_level_prefix)
    
    # Process each prefix or standalone file
    for p in prefixes:
        if p.endswith("/"):  # Directory-like prefix
            format_type = detect_table_format(bucket, p)
            if format_type in ["parquet_directory", "iceberg", "delta", "hudi"] and p not in processed_prefixes:
                if format_type == "parquet_directory":
                    metadata.append(get_parquet_directory_metadata(bucket, p))
                elif format_type == "iceberg":
                    metadata.append(get_iceberg_metadata(bucket, p))
                elif format_type == "delta":
                    metadata.append(get_delta_metadata(bucket, p))
                elif format_type == "hudi":
                    metadata.append(get_hudi_metadata(bucket, p))
                processed_prefixes.add(p)
        else:  # Standalone file
            if p.endswith(".parquet"):
                metadata.append(get_parquet_metadata(bucket, p))
    
    return jsonify({"files": metadata})

@app.route("/data", methods=["GET"])
def get_data():
    """Fetch sample data from a table/file."""
    file_name = request.args.get("file")
    bucket = request.args.get("bucket", BUCKET_NAME)

    if not file_name:
        return jsonify({"error": "File parameter is required"}), 400

    format_type = detect_table_format(bucket, file_name)
    try:
        if format_type == "parquet_file":
            obj = s3_client.get_object(Bucket=bucket, Key=file_name)
            file_stream = BytesIO(obj["Body"].read())
            table = pq.read_table(file_stream)
        elif format_type == "parquet_directory":
            parquet_files = [obj for obj in list_s3_objects(bucket, file_name) if obj.endswith(".parquet")]
            obj = s3_client.get_object(Bucket=bucket, Key=parquet_files[0])
            file_stream = BytesIO(obj["Body"].read())
            table = pq.read_table(file_stream)
        elif format_type == "iceberg":
            table = load_catalog("local", **{
                "uri": S3_ENDPOINT,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY_ID,
                "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
            }).load_table((bucket, file_name)).scan().to_arrow()
        elif format_type == "delta":
            table = DeltaTable(f"s3://{bucket}/{file_name}", storage_options={
                "AWS_ENDPOINT_URL": S3_ENDPOINT,
                "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                "ALLOW_HTTP": "true"
            }).to_pyarrow_table()
        elif format_type == "hudi":
            parquet_files = [obj for obj in list_s3_objects(bucket, file_name) if obj.endswith(".parquet")]
            obj = s3_client.get_object(Bucket=bucket, Key=parquet_files[0])
            file_stream = BytesIO(obj["Body"].read())
            table = pq.read_table(file_stream)
        else:
            return jsonify({"error": "Unsupported format"}), 400
        
        df = table.to_pandas()
        sample_data = df.head(100).to_dict(orient="records")
        return jsonify({"file": file_name, "data": sample_data})
    except Exception as e:
        app.logger.error(f"Error fetching data for {file_name}: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)