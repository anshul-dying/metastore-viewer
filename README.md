Metastore Viewer for Parquet, Iceberg, Delta & Hudi Tables on S3

 Project Description
Metastore Viewer is a web-based tool designed to interact with Parquet, Iceberg, Delta, and Hudi tables stored in an 
S3-compatible object store (MinIO). It provides a user-friendly interface to explore metadata, execute queries, and analyze data efficiently.

Key Features
- Supports querying Parquet, Iceberg, Delta, and Hudi tables.
- Uses **FastAPI** for the backend and **React** for the frontend.
- Connects to an **S3-compatible object store** (MinIO) to fetch table metadata.
- Provides an intuitive web interface for metadata visualization.
- Eliminates the need for a traditional database.

Tech Stack
**Frontend:** React, Tailwind CSS  
**Backend:** FastAPI (Python)  
**Storage:** MinIO (S3-compatible)  
**Containerization:** Docker  

Require Libraries 
- flask
- boto3
- pyarrow
- pyiceberg
- deltalake
- pandas 
- minio 
- fastparquet
- awscli
- flask_cors
- Installation & Setup

1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo/metastore-viewer.git
   cd metastore-viewer
   ```

2. Start Docker Minio
   ```sh
   docker run -d --name minio `
     -p 9000:9000 -p 9090:9090 `
     -e "MINIO_ROOT_USER=admin" `
     -e "MINIO_ROOT_PASSWORD=password" `
     quay.io/minio/minio server /data --console-address ":9090"
   ```
3. Configure aws (requires awscli library in venv)
   ```sh
   aws configure set aws_access_key_id admin
   aws configure set aws_secret_access_key password
   aws configure set region us-east-1
   ```
4. Upload Test parquet files in minio/test-bucket
   ```sh
   aws --endpoint-url=http://localhost:9000 s3 mb s3://test-bucket-
   aws --endpoint-url=http://localhost:9000 s3 cp flights-1m.parquet s3://test-bucket/flights-1m.parquet
   aws --endpoint-url=http://localhost:9000 s3 cp weather.parquet s3://test-bucket/weather.parquet
   ```

2. Start the backend:
   ```sh
   cd backend
   python -m venv venv
   ./venv/Scripts/activate
   pip install -r requirements.txt
   python app.py
   ```

3. Start the frontend:
   ```sh
   cd frontend
   npm install
   npm start
   ```

API Endpoints 
| Method | Endpoint | Description |
|--------|---------|-------------|
| GET | `/metadata?bucket=BUCKET_NAME&prefix=` | To get metadata  |
| GET | `/snapshot_changes?file=filename&bucket=BUCKET_NAME` | Get snapshot changes | 
| GET | `/data?file=filename&bucket=BUCKET_NAME&version=num&max_rows=100` | Get snapshot changes |
| GET | `/partition_data?file=filename&bucket=BUCKET_NAME&partition=num&max_rows=100` | Get snapshot changes|


Contributors
| Name           |         Role        |                                                                       
|------              |--------         |                                             
| [Jayesh Bairagi](https://github.com/Thrasher2210)  | Backend Development |
| [Anshul Khaire](https://github.com/anshul-dying)  | Frontend Development |
| [Soham Misal](https://github.com/Soham-Misal22)  | DevOps & Infrastructure |
| [Anand Ambadkar](https://github.com/anand612)  | Testing & Documentation |


