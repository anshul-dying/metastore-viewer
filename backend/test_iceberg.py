from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "default",
    **{
        "type": "sql",
        "uri": "sqlite:///C:/Users/anshu/OneDrive/Documents/COEP Hackathon/metastore-viewer/backend/iceberg_catalog.db",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.allow-http": "true",
        "downcast-ns-timestamp-to-us-on-write": "true"
    }
)
table = catalog.load_table(("test-bucket", "sample_iceberg"))
print(table.scan().to_arrow().to_pandas())