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


Installation & Setup

1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo/metastore-viewer.git
   cd metastore-viewer
   ```

3. Start the backend:
   ```sh
   cd backend
   python app.py
   ```

5. Start the frontend:
   ```sh
   cd frontend
   npm install
   npm start
   ```

API Endpoints 
| Method | Endpoint | Description |
|--------|---------|-------------|
| GET | `/tables` | List all available tables |
| GET | `/tables/{table_name}` | Get metadata for a specific table|


Contributors
| Name           |         Role        |                                                                       
|------              |--------         |                                             
| [Jayesh Bairagi](https://github.com/Thrasher2210)  | Backend Development |
| [Anshul](https://github.com/anshul-dying)  | Frontend Development |
| [Soham Misal](https://github.com/Soham-Misal22)  | DevOps & Infrastructure |
| [Anand Ambadkar](https://github.com/anand612)  | Testing & Documentation |


