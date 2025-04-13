import os
import boto3
import pyarrow.parquet as pq
import pandas as pd
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from io import BytesIO
from deltalake import DeltaTable
import json
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from datetime import datetime
import numpy as np
import traceback
import pyarrow as pa
import pyarrow.compute as pc

app = Flask(__name__)
# Configure CORS properly
CORS(app, resources={r"/*": {"origins": "*"}})

# MinIO Configuration
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
BUCKET_NAME = "test-bucket"

s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# Error handling decorator
def handle_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            app.logger.error(f"Error in {func.__name__}: {str(e)}")
            app.logger.error(traceback.format_exc())
            return jsonify({
                "error": str(e),
                "status": "error",
                "timestamp": datetime.now().isoformat()
            }), 500
    wrapper.__name__ = func.__name__
    return wrapper

def convert_numpy_types(obj):
    if isinstance(obj, np.generic):
        return obj.item()
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_numpy_types(v) for v in obj]
    return obj

def list_s3_objects(s3_bucket, prefix=""):
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]
    except Exception as e:
        app.logger.error(f"Error listing objects in bucket {s3_bucket}: {str(e)}")
        return []

def detect_table_format(s3_bucket, prefix=""):
    prefix = prefix.rstrip('/')
    objects = list_s3_objects(s3_bucket, prefix)
    if not objects:
        return None
    
    # Check for Delta Lake (highest priority)
    delta_log_prefix = f"{prefix}/_delta_log/" if prefix else "_delta_log/"
    if any(obj.startswith(delta_log_prefix) for obj in objects):
        return "delta"
    
    hudi_metadata_files = [
        obj for obj in objects 
        if '.hoodie/' in obj and 
        any(obj.endswith(x) for x in ['hoodie.properties', 'instant.json', '.commit'])
    ]
    if hudi_metadata_files:
        return "hudi"
    
    # Enhanced Iceberg detection
    iceberg_metadata = [
        obj for obj in objects
        if 'metadata/' in obj and 
        (obj.endswith('.metadata.json') or 
         obj.endswith('.avro'))
    ]
    
    if iceberg_metadata:
        # Verify at least one snapshot exists
        snapshots = [obj for obj in objects if 'snapshots/' in obj]
        if snapshots:
            return "iceberg"
    
    # Check for single Parquet file (could be part of any format)
    if len(objects) == 1 and objects[0].endswith(".parquet"):
        return "parquet_file"
    
    # Check for Parquet directory (could be part of any format)
    if any(obj.endswith(".parquet") for obj in objects):
        # Check if this might be a partitioned table without metadata
        # Look for Hive-style partitioning (key=value)
        if any('/' in obj and '=' in obj.split('/')[-2] for obj in objects if obj.endswith('.parquet')):
            return "parquet_directory"
    
    # Check for other known data file extensions
    extensions = set()
    for obj in objects:
        if '.' in obj:
            ext = obj.split('.')[-1].lower()
            extensions.add(ext)
    
    known_data_extensions = {'parquet', 'avro', 'orc', 'csv', 'json'}
    if extensions & known_data_extensions:
        return "data_files"
    
    return None


def get_data_files_metadata(s3_bucket, prefix):
    objects = [obj for obj in list_s3_objects(s3_bucket, prefix) if '.' in obj]
    if not objects:
        return {"file": prefix, "details": {"error": "No data files found"}}

    # Group by extension
    extensions = {}
    for obj in objects:
        ext = obj.split('.')[-1].lower()
        if ext not in extensions:
            extensions[ext] = []
        extensions[ext].append(obj)

    # Get metadata for the first file of each type
    samples = {}
    for ext, files in extensions.items():
        try:
            if ext == 'parquet':
                samples[ext] = get_parquet_metadata(s3_bucket, files[0])
            else:
                # For other formats, just return basic info
                head = s3_client.head_object(Bucket=s3_bucket, Key=files[0])
                samples[ext] = {
                    "file": files[0],
                    "details": {
                        "format": ext,
                        "file_size": head["ContentLength"],
                        "type": "data_file"
                    }
                }
        except Exception as e:
            samples[ext] = {
                "file": files[0],
                "details": {
                    "error": str(e)
                }
            }

    return {
        "file": prefix,
        "details": {
            "format": "data_files",
            "file_types": samples,
            "total_files": len(objects),
            "total_size": sum(f["details"].get("file_size", 0) for f in samples.values())
        }
    }

def get_parquet_metadata(s3_bucket, file_key):
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
                "partitions": [],
                "snapshots": [],
                "properties": {},
                "changes": []
            }
        }
    except Exception as e:
        app.logger.error(f"Error fetching metadata for {file_key}: {str(e)}")
        return {"file": file_key, "details": {"error": str(e)}}

def get_parquet_directory_metadata(s3_bucket, prefix):
    parquet_files = [obj for obj in list_s3_objects(s3_bucket, prefix) if obj.endswith(".parquet")]
    if not parquet_files:
        return {"file": prefix, "details": {"error": "No parquet files found"}}

    # Initialize metadata structure
    metadata = {
        "file": prefix,
        "details": {
            "format": "parquet_directory",
            "columns": [],
            "num_rows": 0,
            "file_size": 0,
            "partition_keys": [],
            "partitions": [],
            "partition_details": [],  # New field for detailed partition info
            "snapshots": [],
            "properties": {}
        }
    }

    # Process each file to collect schema and partition info
    partition_info = {}  # Track partition values by key
    schema = None

    for file_key in parquet_files:
        try:
            # Get file metadata
            file_meta = get_parquet_metadata(s3_bucket, file_key)["details"]
            
            # Set schema from first file (assuming consistent schema)
            if not metadata["details"]["columns"] and "columns" in file_meta:
                metadata["details"]["columns"] = file_meta["columns"]
            
            # Add to totals
            metadata["details"]["num_rows"] += file_meta.get("num_rows", 0)
            metadata["details"]["file_size"] += file_meta.get("file_size", 0)
            
            # Extract partition information from path
            rel_path = file_key[len(prefix):].strip("/")
            if "/" in rel_path:  # Has partitions
                parts = rel_path.split("/")
                partition_path = "/".join(parts[:-1])
                
                # Add to partitions list if not already present
                if partition_path not in metadata["details"]["partitions"]:
                    metadata["details"]["partitions"].append(partition_path)
                
                # Parse partition key=value pairs
                for part in parts[:-1]:
                    if "=" in part:
                        key, value = part.split("=", 1)
                        if key not in metadata["details"]["partition_keys"]:
                            metadata["details"]["partition_keys"].append(key)
                        
                        # Track all values for each key
                        if key not in partition_info:
                            partition_info[key] = set()
                        partition_info[key].add(value)
            
        except Exception as e:
            app.logger.error(f"Error processing file {file_key}: {str(e)}")

    # Add detailed partition information
    if partition_info:
        metadata["details"]["partition_details"] = [
            {"key": k, "values": sorted(list(v))} 
            for k, v in partition_info.items()
        ]

    return metadata

def get_delta_metadata(s3_bucket, prefix):
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
        history = dt.history()
        
        # Get detailed partition information
        partition_details = []
        partition_values = set()
        
        if partition_keys:
            # Get all files to extract partition values
            files = dt.get_add_actions().to_pandas()
            
            # Collect all partition values
            partition_info = {key: set() for key in partition_keys}
            
            for _, row in files.iterrows():
                path = row["path"]
                parts = path.split("/")
                
                # Extract partition values from path
                for part in parts:
                    if "=" in part:
                        key, value = part.split("=", 1)
                        if key in partition_info:
                            partition_info[key].add(value)
                
                # Add full partition path
                partition_path = "/".join([p for p in parts if "=" in p])
                if partition_path:
                    partition_values.add(partition_path)
            
            # Convert to partition details structure
            partition_details = [
                {"key": k, "values": sorted(list(v))} 
                for k, v in partition_info.items()
            ]
        
        # Prepare metadata
        metadata = {
            "file": prefix,
            "details": {
                "format": "delta",
                "columns": schema,
                "num_rows": files["num_records"].sum(),
                "file_size": files["size_bytes"].sum(),
                "partition_keys": partition_keys,
                "partitions": sorted(list(partition_values)),
                "partition_details": partition_details,  # Detailed partition info
                "snapshots": [{
                    "version": v["version"],
                    "timestamp": v["timestamp"],
                    "operation": v["operation"],
                    "is_current": i == len(history) - 1
                } for i, v in enumerate(history)],
                "properties": dt.metadata().configuration,
                "current_version": history[-1]["version"] if history else 0
            }
        }
        
        return metadata
        
    except Exception as e:
        app.logger.error(f"Error fetching Delta metadata for {prefix}: {str(e)}")
        return {"file": prefix, "details": {"error": str(e)}}


def get_hudi_metadata(s3_bucket, prefix):
    try:
        # Ensure prefix ends with /
        if not prefix.endswith('/'):
            prefix += '/'
            
        # Initialize metadata structure
        metadata = {
            "file": prefix,
            "details": {
                "format": "hudi",
                "columns": [],
                "num_rows": 0,
                "file_size": 0,
                "partition_keys": [],
                "partitions": [],
                "partition_details": [],
                "snapshots": [],
                "properties": {},
                "current_commit": None,
                "error": None
            }
        }

        # 1. Process .hoodie/hoodie.properties
        try:
            hoodie_props_key = f"{prefix}.hoodie/hoodie.properties"
            obj = s3_client.get_object(Bucket=s3_bucket, Key=hoodie_props_key)
            props = dict(line.split("=", 1) for line in obj["Body"].read().decode().splitlines() if "=" in line)
            metadata["details"]["properties"] = props
            
            # Extract partition keys from properties
            if "hoodie.datasource.write.partitionpath.field" in props:
                partition_keys = props["hoodie.datasource.write.partitionpath.field"].split(",")
                metadata["details"]["partition_keys"] = [k.strip() for k in partition_keys]
        except Exception as e:
            app.logger.warning(f"Could not read hoodie.properties: {str(e)}")
            metadata["details"]["error"] = f"Properties error: {str(e)}"

        # 2. Process .hoodie/timeline/ for snapshots
        try:
            timeline_key = f"{prefix}.hoodie/timeline/"
            commit_files = [obj for obj in list_s3_objects(s3_bucket, timeline_key) 
                          if obj.endswith(".commit")]
            
            # Sort commits chronologically (by timestamp in filename)
            commit_files.sort(key=lambda x: x.split('/')[-1].split('.')[0])
            
            for i, commit_file in enumerate(commit_files):
                commit_id = commit_file.split('/')[-1].split('.')[0]
                metadata["details"]["snapshots"].append({
                    "id": i,
                    "timestamp": commit_id,
                    "commit_file": commit_file,
                    "is_current": i == len(commit_files) - 1
                })
            
            if commit_files:
                metadata["details"]["current_commit"] = commit_files[-1].split('/')[-1].split('.')[0]
        except Exception as e:
            app.logger.warning(f"Could not process timeline: {str(e)}")
            if not metadata["details"]["error"]:
                metadata["details"]["error"] = f"Timeline error: {str(e)}"

        # 3. Process data files (parquet files in the root directory)
        try:
            data_files = [obj for obj in list_s3_objects(s3_bucket, prefix) 
                         if obj.endswith(".parquet") and not obj.startswith(".hoodie/")]
            
            if data_files:
                # Get schema from the first parquet file
                try:
                    first_file = data_files[0]
                    file_meta = get_parquet_metadata(s3_bucket, first_file)["details"]
                    metadata["details"]["columns"] = file_meta["columns"]
                    metadata["details"]["num_rows"] = file_meta.get("num_rows", 0) * len(data_files)
                except Exception as e:
                    app.logger.warning(f"Could not read schema from {first_file}: {str(e)}")
                    if not metadata["details"]["error"]:
                        metadata["details"]["error"] = f"Schema error: {str(e)}"
                
                # Calculate total size
                total_size = 0
                for file_key in data_files:
                    try:
                        head = s3_client.head_object(Bucket=s3_bucket, Key=file_key)
                        total_size += head["ContentLength"]
                    except Exception as e:
                        app.logger.warning(f"Could not get size for {file_key}: {str(e)}")
                
                metadata["details"]["file_size"] = total_size
                
                # Check for partitions in file paths
                partition_values = set()
                for file_key in data_files:
                    rel_path = file_key[len(prefix):]  # Remove the prefix
                    if '/' in rel_path:  # Has partitions
                        partition_path = '/'.join(rel_path.split('/')[:-1])
                        partition_values.add(partition_path)
                
                if partition_values:
                    metadata["details"]["partitions"] = sorted(list(partition_values))
                    
                    # If we have partition keys but no values from properties, extract from paths
                    if not metadata["details"]["partition_keys"]:
                        first_partition = next(iter(partition_values), None)
                        if first_partition and '=' in first_partition:
                            metadata["details"]["partition_keys"] = [
                                part.split('=')[0] 
                                for part in first_partition.split('/') 
                                if '=' in part
                            ]
                    
                    # Build partition details
                    if metadata["details"]["partition_keys"]:
                        partition_info = {key: set() for key in metadata["details"]["partition_keys"]}
                        for partition_path in partition_values:
                            parts = partition_path.split('/')
                            for part in parts:
                                if '=' in part:
                                    key, value = part.split('=', 1)
                                    if key in partition_info:
                                        partition_info[key].add(value)
                        
                        metadata["details"]["partition_details"] = [
                            {"key": k, "values": sorted(list(v))} 
                            for k, v in partition_info.items()
                        ]
        except Exception as e:
            app.logger.warning(f"Could not process data files: {str(e)}")
            if not metadata["details"]["error"]:
                metadata["details"]["error"] = f"Data files error: {str(e)}"
        
        return metadata
        
    except Exception as e:
        app.logger.error(f"Error fetching Hudi metadata for {prefix}: {str(e)}")
        return {
            "file": prefix,
            "details": {
                "format": "hudi",
                "error": str(e),
                "columns": [],
                "partition_details": []
            }
        }

def get_iceberg_metadata(s3_bucket, prefix):
    try:
        prefix = prefix.rstrip('/')
        
        # Initialize catalog with explicit configuration
        catalog = load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": f"{S3_ENDPOINT}/iceberg",
                "warehouse": f"s3://{s3_bucket}",
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY_ID,
                "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
                "io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
            }
        )

        # Try multiple naming formats
        table_identifiers = [
            prefix.replace('/', '.'),
            f"{s3_bucket}.{prefix.replace('/', '.')}",
            prefix.split('/')[-1]  # Just the table name
        ]
        
        table = None
        for identifier in table_identifiers:
            try:
                table = catalog.load_table(identifier)
                break
            except NoSuchTableError:
                continue
                
        if not table:
            return {"file": prefix, "details": {"error": "Iceberg table not found"}}

        # Force metadata refresh
        table.refresh()
        
        # Get partition information
        partition_keys = [spec.field_name for spec in table.spec().fields]
        partition_values = set()
        partition_details = []
        
        if partition_keys:
            partition_info = {key: set() for key in partition_keys}
            
            if table.current_snapshot():
                for manifest in table.current_snapshot().manifests:
                    for entry in manifest.fetch_entries(table.io):
                        if hasattr(entry, 'partition'):
                            partition_str = "/".join(f"{k}={v}" for k, v in entry.partition.items())
                            if partition_str:
                                partition_values.add(partition_str)
                            for key, value in entry.partition.items():
                                if key in partition_info:
                                    partition_info[key].add(str(value))
            
            partition_details = [
                {"key": k, "values": sorted(list(v))} 
                for k, v in partition_info.items()
            ]
        
        # Get snapshots
        snapshots = []
        current_snapshot_id = table.current_snapshot().snapshot_id if table.current_snapshot() else None
        for snap in table.history():
            snapshots.append({
                "snapshot_id": snap.snapshot_id,
                "timestamp": snap.timestamp_ms,
                "operation": snap.operation,
                "summary": snap.summary,
                "is_current": snap.snapshot_id == current_snapshot_id
            })
        
        # Get table statistics
        num_rows = 0
        file_size = 0
        if table.current_snapshot():
            try:
                scan = table.scan()
                num_rows = table.current_snapshot().summary().get("total-records", 0)
                file_size = sum(f.file_size_in_bytes for f in scan.plan_files())
            except Exception as e:
                app.logger.error(f"Error calculating Iceberg table stats: {str(e)}")
        
        return {
            "file": prefix,
            "details": {
                "format": "iceberg",
                "columns": schema,
                "num_rows": int(num_rows),
                "file_size": int(file_size),
                "partition_keys": partition_keys,
                "partitions": list(partition_values),
                "partition_details": partition_details,
                "snapshots": snapshots,
                "properties": table.properties,
                "current_snapshot_id": current_snapshot_id,
                "changes": []  # Changes would require comparing snapshots
            }
        }
        
    except NoSuchTableError:
        return {"file": prefix, "details": {"error": f"Iceberg table {prefix} not found", "partition_details": []}}
    except Exception as e:
        app.logger.error(f"Error processing Iceberg table {prefix}: {str(e)}")
        return {"file": prefix, "details": {"error": str(e), "partition_details": []}}

@app.route("/metadata", methods=["GET"])
@handle_errors
def get_metadata():
    bucket = request.args.get("bucket", BUCKET_NAME)
    prefix = request.args.get("prefix", "")
    file_key = request.args.get("file")

    if file_key:
        format_type = detect_table_format(bucket, file_key)
        if format_type == "parquet_file":
            result = get_parquet_metadata(bucket, file_key)
        elif format_type == "parquet_directory":
            result = get_parquet_directory_metadata(bucket, file_key)
        elif format_type == "delta":
            result = get_delta_metadata(bucket, file_key)
        elif format_type == "hudi":
            result = get_hudi_metadata(bucket, file_key)
        elif format_type == "iceberg":
            result = get_iceberg_metadata(bucket, file_key)
        elif format_type == "data_files":
            result = get_data_files_metadata(bucket, file_key)
        else:
            return jsonify({"error": "Unsupported format or invalid path", "details": f"Detected format: {format_type}"}), 400
    else:
        # List all tables in the bucket/prefix
        objects = list_s3_objects(bucket, prefix)
        if not objects:
            return jsonify({"error": "No files found or bucket inaccessible"}), 404
        
        metadata = []
        processed_prefixes = set()
        
        # First check for root-level tables
        prefixes = set()
        for obj_key in objects:
            if "/" not in obj_key:
                # Single file
                if obj_key.endswith(".parquet"):
                    metadata.append(get_parquet_metadata(bucket, obj_key))
            else:
                # Directory structure
                top_level_prefix = obj_key.split("/")[0] + "/"
                prefixes.add(top_level_prefix)
        
        # Now process each prefix to detect table formats
        for p in sorted(prefixes):
            format_type = detect_table_format(bucket, p)
            if format_type and p not in processed_prefixes:
                if format_type == "parquet_directory":
                    metadata.append(get_parquet_directory_metadata(bucket, p))
                elif format_type == "delta":
                    metadata.append(get_delta_metadata(bucket, p))
                elif format_type == "hudi":
                    metadata.append(get_hudi_metadata(bucket, p))
                elif format_type == "iceberg":
                    metadata.append(get_iceberg_metadata(bucket, p))
                processed_prefixes.add(p)

        for item in metadata:
            if "details" in item and "partition_details" not in item["details"]:
                item["details"]["partition_details"] = []
        
        result = {"files": metadata}

    # Convert numpy types before returning
    response = make_response(jsonify(convert_numpy_types(result)))
    response.headers['Cache-Control'] = 'no-cache'
    return response


@app.route("/snapshot_changes", methods=["GET"])
@handle_errors
def get_snapshot_changes():
    file_name = request.args.get("file")
    bucket = request.args.get("bucket", BUCKET_NAME)
    version = request.args.get("version")
    
    if not file_name or not version:
        return jsonify({"error": "File and version parameters are required"}), 400

    format_type = detect_table_format(bucket, file_name)
    
    if format_type == "delta":
        try:
            table_path = f"s3://{bucket}/{file_name}"
            storage_options = {
                "AWS_ENDPOINT_URL": S3_ENDPOINT,
                "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                "ALLOW_HTTP": "true"
            }
            
            dt = DeltaTable(table_path, version=int(version), storage_options=storage_options)
            current_version = dt.version()
            
            if current_version == 0:
                # Initial version - all data is added
                current_data = dt.to_pyarrow_table().to_pandas()
                response = make_response(jsonify({
                    "added": current_data.to_dict(orient="records"),
                    "updated": [],
                    "deleted": []
                }))
                response.headers['Cache-Control'] = 'no-cache'
                return response
            
            # Get previous version
            prev_version = current_version - 1
            prev_dt = DeltaTable(table_path, version=prev_version, storage_options=storage_options)
            
            # Get current and previous data
            current_df = dt.to_pyarrow_table().to_pandas()
            prev_df = prev_dt.to_pyarrow_table().to_pandas()
            
            # Convert to dictionaries for easier comparison
            current_records = current_df.to_dict('records')
            prev_records = prev_df.to_dict('records')
            
            # Create dictionaries by ID for quick lookup
            current_by_id = {str(row['id']): row for row in current_records if 'id' in row}
            prev_by_id = {str(row['id']): row for row in prev_records if 'id' in row}
            
            # Find changes
            current_ids = set(current_by_id.keys())
            prev_ids = set(prev_by_id.keys())
            
            # Added records (in current but not in previous)
            added_ids = current_ids - prev_ids
            added = [current_by_id[id] for id in added_ids]
            
            # Deleted records (in previous but not in current)
            deleted_ids = prev_ids - current_ids
            deleted = [prev_by_id[id] for id in deleted_ids]
            
            # Updated records (in both but different)
            updated = []
            common_ids = current_ids & prev_ids
            
            for id_val in common_ids:
                current_row = current_by_id[id_val]
                prev_row = prev_by_id[id_val]
                
                # Find all changed fields
                changed_fields = {}
                for key in current_row:
                    if key != 'id' and current_row[key] != prev_row[key]:
                        changed_fields[key] = {
                            'old_value': prev_row[key],
                            'new_value': current_row[key]
                        }
                
                if changed_fields:
                    updated.append({
                        'id': id_val,
                        'changes': changed_fields
                    })
            
            response = make_response(jsonify({
                "added": added,
                "updated": updated,
                "deleted": deleted,
                "current_version": current_version,
                "previous_version": prev_version
            }))
            response.headers['Cache-Control'] = 'no-cache'
            return response
        except Exception as e:
            app.logger.error(f"Error processing Delta changes: {str(e)}")
            return jsonify({"error": f"Failed to process Delta changes: {str(e)}"}), 500
    
    elif format_type == "hudi":
        try:
            # Get all data files (excluding .hoodie and crc files)
            all_files = list_s3_objects(bucket, file_name)
            
            # Get the .hoodie/timeline directory
            timeline_dir = f"{file_name.rstrip('/')}/.hoodie/timeline/"
            commit_files = [
                obj for obj in list_s3_objects(bucket, timeline_dir) 
                if obj.endswith('.commit')
            ]
            
            # Sort commits by timestamp (they're in format: yyyyMMddHHmmss.commit)
            commit_files.sort(key=lambda x: x.split('/')[-1].split('.')[0])
            
            if not commit_files:
                return jsonify({"error": "No commit history found for Hudi table"}), 404
                
            # Find the requested version (timestamp) and the previous version
            requested_timestamp = version
            current_commit_index = -1
            
            for i, commit_file in enumerate(commit_files):
                timestamp = commit_file.split('/')[-1].split('.')[0]
                if timestamp == requested_timestamp:
                    current_commit_index = i
                    break
            
            if current_commit_index == -1:
                return jsonify({"error": f"No commit found for version {version}"}), 404
                
            # Read data for current version
            current_version_files = []
            for obj in all_files:
                if obj.endswith('.parquet') and requested_timestamp in obj:
                    current_version_files.append(obj)
            
            if not current_version_files:
                return jsonify({"error": f"No data files found for version {version}"}), 404
                
            # Create a combined dataframe for current version
            current_tables = []
            for file_key in current_version_files[:10]:  # Limit to 10 files
                obj = s3_client.get_object(Bucket=bucket, Key=file_key)
                file_stream = BytesIO(obj["Body"].read())
                parquet_file = pq.ParquetFile(file_stream)
                table = parquet_file.read()
                current_tables.append(table)
                
            if not current_tables:
                return jsonify({"error": "Failed to read any current version files"}), 500
                
            # If this is the first commit, all data is added
            if current_commit_index == 0:
                current_df = pa.concat_tables(current_tables).to_pandas()
                response = make_response(jsonify({
                    "added": current_df.to_dict(orient="records"),
                    "updated": [],
                    "deleted": [],
                    "current_version": requested_timestamp,
                    "previous_version": None
                }))
                response.headers['Cache-Control'] = 'no-cache'
                return response
                
            # Get previous version timestamp
            prev_timestamp = commit_files[current_commit_index - 1].split('/')[-1].split('.')[0]
            
            # Read data for previous version
            prev_version_files = []
            for obj in all_files:
                if obj.endswith('.parquet') and prev_timestamp in obj:
                    prev_version_files.append(obj)
            
            if not prev_version_files:
                # No previous version files found, all data in current version is new
                current_df = pa.concat_tables(current_tables).to_pandas()
                response = make_response(jsonify({
                    "added": current_df.to_dict(orient="records"),
                    "updated": [],
                    "deleted": [],
                    "current_version": requested_timestamp,
                    "previous_version": prev_timestamp
                }))
                response.headers['Cache-Control'] = 'no-cache'
                return response
                
            # Create a combined dataframe for previous version
            prev_tables = []
            for file_key in prev_version_files[:10]:  # Limit to 10 files
                obj = s3_client.get_object(Bucket=bucket, Key=file_key)
                file_stream = BytesIO(obj["Body"].read())
                parquet_file = pq.ParquetFile(file_stream)
                table = parquet_file.read()
                prev_tables.append(table)
                
            if not prev_tables:
                return jsonify({"error": "Failed to read any previous version files"}), 500
                
            # Compare records
            current_df = pa.concat_tables(current_tables).to_pandas()
            prev_df = pa.concat_tables(prev_tables).to_pandas()
            
            # Look for ID columns
            id_column = None
            for column in current_df.columns:
                if column.lower() in ['id', 'uuid', '_hoodie_record_key']:
                    id_column = column
                    break
                    
            if not id_column:
                # Without an ID column, we can't reliably track changes - return simple stats
                response = make_response(jsonify({
                    "current_version": requested_timestamp,
                    "previous_version": prev_timestamp,
                    "current_record_count": len(current_df),
                    "previous_record_count": len(prev_df),
                    "record_count_change": len(current_df) - len(prev_df),
                    "column_changes": list(set(current_df.columns) - set(prev_df.columns)),
                    "message": "Cannot track individual record changes without ID column"
                }))
                response.headers['Cache-Control'] = 'no-cache'
                return response
                
            # Convert to dictionaries for easier comparison
            current_records = current_df.to_dict('records')
            prev_records = prev_df.to_dict('records')
            
            # Create dictionaries by ID for quick lookup
            current_by_id = {str(row[id_column]): row for row in current_records}
            prev_by_id = {str(row[id_column]): row for row in prev_records}
            
            # Find changes
            current_ids = set(current_by_id.keys())
            prev_ids = set(prev_by_id.keys())
            
            # Added records (in current but not in previous)
            added_ids = current_ids - prev_ids
            added = [current_by_id[id] for id in added_ids]
            
            # Deleted records (in previous but not in current)
            deleted_ids = prev_ids - current_ids
            deleted = [prev_by_id[id] for id in deleted_ids]
            
            # Updated records (in both but different)
            updated = []
            common_ids = current_ids & prev_ids
            
            for id_val in common_ids:
                current_row = current_by_id[id_val]
                prev_row = prev_by_id[id_val]
                
                # Find all changed fields
                changed_fields = {}
                for key in current_row:
                    if key != id_column and current_row[key] != prev_row.get(key):
                        changed_fields[key] = {
                            'old_value': prev_row.get(key),
                            'new_value': current_row[key]
                        }
                
                if changed_fields:
                    updated.append({
                        'id': id_val,
                        'changes': changed_fields
                    })
            
            response = make_response(jsonify({
                "added": added[:100],  # Limit to 100 records for performance
                "updated": updated[:100],
                "deleted": deleted[:100],
                "current_version": requested_timestamp,
                "previous_version": prev_timestamp,
                "added_count": len(added),
                "updated_count": len(updated),
                "deleted_count": len(deleted)
            }))
            response.headers['Cache-Control'] = 'no-cache'
            return response
            
        except Exception as e:
            app.logger.error(f"Error processing Hudi changes: {str(e)}")
            app.logger.error(traceback.format_exc())
            return jsonify({"error": f"Failed to process Hudi changes: {str(e)}"}), 500
        
    else:
        return jsonify({"error": f"Format {format_type} does not support versioning or is unsupported"}), 400

@app.route("/data", methods=["GET"])
@handle_errors
def get_data():
    file_name = request.args.get("file")
    bucket = request.args.get("bucket", BUCKET_NAME)
    version = request.args.get("version")
    max_rows = int(request.args.get("max_rows", 100))

    if not file_name:
        return jsonify({"error": "File parameter is required"}), 400

    format_type = detect_table_format(bucket, file_name)
    try:
        if format_type == "parquet_file":
            obj = s3_client.get_object(Bucket=bucket, Key=file_name)
            file_stream = BytesIO(obj["Body"].read())
            parquet_file = pq.ParquetFile(file_stream)
            table = parquet_file.read_row_group(0).slice(0, max_rows)
        elif format_type == "parquet_directory":
            parquet_files = [obj for obj in list_s3_objects(bucket, file_name) if obj.endswith(".parquet")]
            obj = s3_client.get_object(Bucket=bucket, Key=parquet_files[0])
            file_stream = BytesIO(obj["Body"].read())
            parquet_file = pq.ParquetFile(file_stream)
            table = parquet_file.read_row_group(0).slice(0, max_rows)
        elif format_type == "delta":
            dt = DeltaTable(f"s3://{bucket}/{file_name}", storage_options={
                "AWS_ENDPOINT_URL": S3_ENDPOINT,
                "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                "ALLOW_HTTP": "true"
            })
            if version:
                dt = DeltaTable(f"s3://{bucket}/{file_name}", 
                              version=int(version),
                              storage_options={
                                  "AWS_ENDPOINT_URL": S3_ENDPOINT,
                                  "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                                  "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                                  "ALLOW_HTTP": "true"
                              })
            table = dt.to_pyarrow_table().slice(0, max_rows)
        elif format_type == "hudi":
            # Get all data files (excluding .hoodie and crc files)
            all_files = list_s3_objects(bucket, file_name)
            parquet_files = [
                obj for obj in all_files 
                if obj.endswith('.parquet') and 
                not obj.startswith('.hoodie/') and
                not obj.endswith('.crc')
            ]

            if not parquet_files:
                return jsonify({"error": "No data files found in Hudi table"}), 404

            # If version specified, find files matching that version
            if version:
                # Hudi versions are typically timestamps (e.g., 20240330010101)
                version_files = [f for f in parquet_files if version in f]
                if version_files:
                    parquet_files = version_files
                else:
                    return jsonify({"error": f"No files found for version {version}"}), 404

            # Read and combine data from parquet files
            tables = []
            for file_key in parquet_files[:10]:  # Limit to 10 files for safety
                try:
                    # Skip CRC files explicitly
                    if file_key.endswith('.crc'):
                        continue
                        
                    obj = s3_client.get_object(Bucket=bucket, Key=file_key)
                    file_stream = BytesIO(obj["Body"].read())
                    
                    # Read parquet file with proper error handling
                    try:
                        parquet_file = pq.ParquetFile(file_stream)
                        # Read first row group with limit
                        table = parquet_file.read_row_group(0).slice(0, max_rows)
                        tables.append(table)
                    except Exception as e:
                        app.logger.warning(f"Error reading {file_key}: {str(e)}")
                        continue
                        
                except Exception as e:
                    app.logger.warning(f"Error accessing {file_key}: {str(e)}")
                    continue

            if not tables:
                return jsonify({"error": "Could not read any valid data files"}), 500

            # Combine all tables and apply final row limit
            combined = pa.concat_tables(tables).slice(0, max_rows)
            df = combined.to_pandas()
            
            response = make_response(jsonify({
                "file": file_name,
                "version": version if version else "latest",
                "data": df.to_dict(orient="records")
            }))
            response.headers['Cache-Control'] = 'no-cache'
            return response

        elif format_type == "iceberg":
            try:
                # Initialize Iceberg catalog
                catalog = load_catalog(
                    "default",
                    **{
                        "uri": S3_ENDPOINT,
                        "s3.endpoint": S3_ENDPOINT,
                        "s3.access-key-id": AWS_ACCESS_KEY_ID,
                        "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
                        "warehouse": f"s3://{bucket}",
                    }
                )
                
                # Load table
                table = catalog.load_table(file_name.replace('/', '.'))
                
                # Configure scan with limit
                scan = table.scan()
                if version:
                    scan = scan.use_snapshot(int(version))
                
                # Execute scan with limit
                arrow_table = scan.to_arrow()
                limited_table = arrow_table.slice(0, max_rows)
                df = limited_table.to_pandas()
                
                response = make_response(jsonify({
                    "file": file_name,
                    "version": version if version else "latest",
                    "data": df.to_dict(orient="records")
                }))
                response.headers['Cache-Control'] = 'no-cache'
                return response

            except NoSuchTableError:
                return jsonify({"error": f"Iceberg table {file_name} not found"}), 404
            except Exception as e:
                app.logger.error(f"Error reading Iceberg table: {str(e)}")
                return jsonify({"error": "Failed to read Iceberg table", "details": str(e)}), 500 
        elif format_type == "data_files":
            # Handle generic data files
            obj = s3_client.get_object(Bucket=bucket, Key=file_name)
            file_stream = BytesIO(obj["Body"].read())
            
            if file_name.endswith('.csv'):
                # Read CSV
                df = pd.read_csv(file_stream, nrows=max_rows)
            elif file_name.endswith('.json'):
                # Read JSON
                df = pd.read_json(file_stream, lines=True, nrows=max_rows)
            else:
                return jsonify({"error": "Unsupported file format"}), 400
                
            response = make_response(jsonify({
                "file": file_name,
                "version": "latest",
                "data": df.to_dict(orient="records")
            }))
            response.headers['Cache-Control'] = 'no-cache'
            return response
        else:
            return jsonify({"error": "Unsupported format", "details": f"Detected format: {format_type}"}), 400
        
        df = table.to_pandas()
        sample_data = df.to_dict(orient="records")
        
        result = {
            "file": file_name,
            "version": version if version else "latest",
            "data": sample_data
        }
        
        # Convert numpy types before returning
        return jsonify(convert_numpy_types(result))
        
    except Exception as e:
        app.logger.error(f"Error fetching data for {file_name}: {str(e)}")
        return jsonify({"error": "Failed to fetch data", "details": str(e)}), 500
    
@app.route("/partition_data", methods=["GET"])
@handle_errors
def get_partition_data():
    file_name = request.args.get("file")
    bucket = request.args.get("bucket", BUCKET_NAME)
    partition = request.args.get("partition")
    max_rows = int(request.args.get("max_rows", 100))

    if not file_name or not partition:
        return jsonify({"error": "File and partition parameters are required"}), 400

    format_type = detect_table_format(bucket, file_name)
    try:
        if format_type == "parquet_directory":
            # Parse partition filters from the path
            partition_filters = {}
            for part in partition.split("/"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    partition_filters[key] = value
            
            # Find files matching the partition
            parquet_files = []
            for obj in list_s3_objects(bucket, file_name):
                if obj.endswith(".parquet") and all(f"{k}={v}" in obj for k, v in partition_filters.items()):
                    parquet_files.append(obj)
            
            if not parquet_files:
                return jsonify({"error": "No data found for partition"}), 404
            
            # Read first matching file
            obj = s3_client.get_object(Bucket=bucket, Key=parquet_files[0])
            file_stream = BytesIO(obj["Body"].read())
            parquet_file = pq.ParquetFile(file_stream)
            table = parquet_file.read_row_group(0).slice(0, max_rows)
            
        elif format_type == "delta":
            dt = DeltaTable(f"s3://{bucket}/{file_name}", storage_options={
                "AWS_ENDPOINT_URL": S3_ENDPOINT,
                "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                "ALLOW_HTTP": "true"
            })
            
            # Parse partition filters
            partition_filters = {}
            for part in partition.split("/"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    partition_filters[key] = value
            
            # Convert to PyArrow table and filter
            table = dt.to_pyarrow_table()
            
            # Apply partition filters
            for key, value in partition_filters.items():
                mask = pc.equal(pc.field(key), value)
                table = table.filter(mask)
            
            table = table.slice(0, max_rows)
            
        elif format_type == "hudi":
            # Parse partition filters
            partition_filters = {}
            for part in partition.split("/"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    partition_filters[key] = value
            
            # Find matching files
            parquet_files = []
            for obj in list_s3_objects(bucket, file_name):
                if obj.endswith(".parquet") and all(f"{k}={v}" in obj for k, v in partition_filters.items()):
                    parquet_files.append(obj)
            
            if not parquet_files:
                return jsonify({"error": "No data found for partition"}), 404
            
            # Read first matching file
            obj = s3_client.get_object(Bucket=bucket, Key=parquet_files[0])
            file_stream = BytesIO(obj["Body"].read())
            parquet_file = pq.ParquetFile(file_stream)
            table = parquet_file.read_row_group(0).slice(0, max_rows)
            
        elif format_type == "iceberg":
            catalog = load_catalog(
                "default",
                **{
                    "uri": S3_ENDPOINT,
                    "s3.endpoint": S3_ENDPOINT,
                    "s3.access-key-id": AWS_ACCESS_KEY_ID,
                    "s3.secret-access-key": AWS_SECRET_ACCESS_KEY,
                }
            )
            table = catalog.load_table(f"{bucket}.{file_name.strip('/')}")
            
            # Parse partition filters
            partition_filters = {}
            for part in partition.split("/"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    partition_filters[key] = value
            
            # Filter by partition
            scan = table.scan()
            for key, value in partition_filters.items():
                scan = scan.filter(pc.field(key) == value)
            table = scan.to_arrow().slice(0, max_rows)
            
        else:
            return jsonify({"error": "Format does not support partitions"}), 400
        
        df = table.to_pandas()
        sample_data = df.to_dict(orient="records")
        
        result = {
            "file": file_name,
            "partition": partition,
            "data": sample_data
        }
        
        # Return with proper headers
        response = make_response(jsonify(convert_numpy_types(result)))
        response.headers['Cache-Control'] = 'no-cache'
        return response
        
    except Exception as e:
        app.logger.error(f"Error fetching partition data: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Health check endpoint
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)