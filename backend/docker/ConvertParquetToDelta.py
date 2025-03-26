# ConvertParquetToDelta.py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Configure Spark session with Delta Lake and S3 settings
builder = SparkSession.builder \
    .appName("ConvertParquetToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define paths
parquet_path = "s3a://test-bucket/flights-1m.parquet"
delta_path = "s3a://test-bucket/flights-1m-delta"

# Read Parquet file
df = spark.read.parquet(parquet_path)

# Write as Delta table
df.write.format("delta").mode("overwrite").save(delta_path)

# Create a few snapshots by updating the table
# First snapshot: Initial table
# Second snapshot: Add a new column
df = spark.read.format("delta").load(delta_path)
df_with_new_column = df.withColumn("new_column", df["AIR_TIME"] * 2)  # Example: Add a new column
df_with_new_column.write.format("delta").mode("overwrite").save(delta_path)

# Third snapshot: Update some rows
from pyspark.sql.functions import col
df_updated = spark.read.format("delta").load(delta_path)
df_updated = df_updated.withColumn("AIR_TIME", col("AIR_TIME") + 10)  # Example: Update AIR_TIME
df_updated.write.format("delta").mode("overwrite").save(delta_path)

# Stop Spark session
spark.stop()