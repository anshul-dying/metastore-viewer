# from flask import Flask, request, jsonify
# from flask_cors import CORS
# import boto3
# import pyarrow.parquet as pq
# import pandas as pd
# import os
# import io
# from metadata_parser import list_s3_objects, get_parquet_metadata

# app = Flask(__name__)
# CORS(app)

# # Load S3 credentials from environment variables (set them in your system)
# s3_client = boto3.client(
#     "s3",
#     aws_access_key_id="test",
#     aws_secret_access_key="test",
#     region_name="us-east-1",
#     endpoint_url="http://localhost:4566"  # Ensure LocalStack is used
# )

# def list_s3_objects(bucket, prefix):
#     """List objects in an S3 bucket under a specific prefix."""
#     response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
#     return [obj['Key'] for obj in response.get('Contents', [])]

# def get_parquet_metadata(bucket, key):
#     """Fetch and parse Parquet file metadata from S3."""
#     obj = s3_client.get_object(Bucket=bucket, Key=key)
#     file_stream = io.BytesIO(obj['Body'].read())  # Wrap in BytesIO
#     table = pq.read_table(file_stream)  # Now it's seekable
#     schema = table.schema
#     return {
#         "columns": [str(f) for f in schema],
#         "num_rows": table.num_rows
#     }

# @app.route("/metadata", methods=["GET"])
# def fetch_metadata():
#     bucket = request.args.get("bucket")
#     prefix = request.args.get("prefix", "")

#     if not bucket:
#         return jsonify({"error": "Bucket parameter is required"}), 400

#     objects = list_s3_objects(bucket, prefix)
#     metadata = []

#     for obj_key in objects:
#         if obj_key.endswith(".parquet"):
#             metadata.append(
#                 {"file": obj_key, "details": get_parquet_metadata(bucket, obj_key)}
#             )

#     return jsonify({"files": metadata})  # ✅ Wrap in an object with "files"

# @app.route("/data", methods=["GET"])
# def get_data():
#     file_name = request.args.get("file")
#     bucket = "test-bucket"  # Ensure you use the correct bucket

#     if not file_name:
#         return jsonify({"error": "File parameter is required"}), 400

#     try:
#         # ✅ Fetch file from S3
#         obj = s3_client.get_object(Bucket=bucket, Key=file_name)
#         file_stream = io.BytesIO(obj['Body'].read())  # Make it seekable
#         table = pq.read_table(file_stream)  # Read Parquet file
#         df = table.to_pandas()

#         sample_data = df.head(100).to_dict(orient="records")  # Get first 100 rows
#         return jsonify({"file": file_name, "data": sample_data})

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500


# if __name__ == "__main__":
#     app.run(debug=True)


# backend/app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3
import pyarrow.parquet as pq
import pandas as pd
import io
from botocore.exceptions import ClientError

app = Flask(__name__)
CORS(app)

# Configure S3 client for LocalStack in Docker
s3_client = boto3.client(
    "s3",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
    endpoint_url="http://localhost:4566"  # LocalStack endpoint
)

def list_s3_objects(bucket, prefix=""):
    """List objects in an S3 bucket under a specific prefix."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj['Key'] for obj in response.get('Contents', [])]
    except ClientError as e:
        app.logger.error(f"Error listing objects: {str(e)}")
        return []

def get_parquet_metadata(bucket, key):
    """Fetch metadata for a Parquet file."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        file_stream = io.BytesIO(obj['Body'].read())
        
        # Read the table
        table = pq.read_table(file_stream)
        schema = table.schema
        
        return {
            "format": "parquet",
            "columns": [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in schema],
            "num_rows": table.num_rows,
            "file_size": obj['ContentLength'],
            "partition_keys": [],  # Add partition logic later if needed
            "snapshots": []
        }
    except ClientError as e:
        app.logger.error(f"ClientError fetching metadata for {key}: {str(e)}")
        return {"error": f"Failed to fetch metadata for {key}: {str(e)}"}
    except Exception as e:
        app.logger.error(f"Error fetching metadata for {key}: {str(e)}")
        return {"error": f"Failed to process {key}: {str(e)}"}

@app.route("/metadata", methods=["GET"])
def fetch_metadata():
    bucket = request.args.get("bucket")
    prefix = request.args.get("prefix", "")

    if not bucket:
        return jsonify({"error": "Bucket parameter is required"}), 400

    objects = list_s3_objects(bucket, prefix)
    metadata = []

    for obj_key in objects:
        if obj_key.endswith(".parquet"):
            file_metadata = get_parquet_metadata(bucket, obj_key)
            metadata.append({"file": obj_key, "details": file_metadata})

    return jsonify({"files": metadata})

@app.route("/data", methods=["GET"])
def get_data():
    file_name = request.args.get("file")
    bucket = request.args.get("bucket", "test-bucket")

    if not file_name:
        return jsonify({"error": "File parameter is required"}), 400

    try:
        obj = s3_client.get_object(Bucket=bucket, Key=file_name)
        file_stream = io.BytesIO(obj['Body'].read())
        table = pq.read_table(file_stream)
        df = table.to_pandas()
        sample_data = df.head(100).to_dict(orient="records")
        return jsonify({"file": file_name, "data": sample_data})
    except Exception as e:
        app.logger.error(f"Error fetching data for {file_name}: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)