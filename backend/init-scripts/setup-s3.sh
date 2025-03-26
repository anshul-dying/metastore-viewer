#!/bin/sh
aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket
aws --endpoint-url=http://localhost:4566 s3 cp /data/flights-1m.parquet s3://test-bucket/flights-1m.parquet
aws --endpoint-url=http://localhost:4566 s3 cp /data/weather.parquet s3://test-bucket/weather.parquet
