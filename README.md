# Flight Server

A high-performance, memory-efficient FastAPI application for SQL query processing with Arrow streaming.

## Configuration

All configuration values can be set via environment variables. Here's a list of all available configuration options:

### Core Configuration

- `FLIGHT_APP_PORT`: Port for the FastAPI server (default: 8000)
- `FLIGHT_MAX_WORKERS`: Maximum number of worker threads (default: 4)
- `FLIGHT_BATCH_SIZE`: Batch size for Arrow processing (default: 1000)
- `FLIGHT_QUERY_ROW_THRESHOLD`: Row threshold for Arrow vs JSON format (default: 10000)

### Database Configuration

- `FLIGHT_DB_PATH`: Path to DuckDB database (default: ':memory:')
- `FLIGHT_REGISTRY_PATH`: Path to SQLite registry database (default: 'job_registry.db')
- `FLIGHT_REGISTRY_DB`: Optional DuckDB registry database path

### S3 Configuration

- `FLIGHT_S3_BUCKET`: S3 bucket name for query results (default: 'my-query-cache-bucket')
- `FLIGHT_S3_REGION`: AWS region (default: 'us-east-1')
- `FLIGHT_S3_ENDPOINT_URL`: Custom S3 endpoint URL (optional)
- `FLIGHT_S3_ACCESS_KEY`: AWS access key ID (optional)
- `FLIGHT_S3_SECRET_KEY`: AWS secret access key (optional)

### Example .env File

```env
FLIGHT_APP_PORT=8000
FLIGHT_MAX_WORKERS=4
FLIGHT_BATCH_SIZE=1000
FLIGHT_QUERY_ROW_THRESHOLD=10000
FLIGHT_DB_PATH=":memory:"
FLIGHT_REGISTRY_PATH="job_registry.db"
FLIGHT_S3_BUCKET="my-query-cache-bucket"
FLIGHT_S3_REGION="us-east-1"
```

## Running the Server

You can run the server using:

```bash
# Using Python directly
python -m flight_server.main

# Or using uvicorn
uvicorn flight_server.main:app --port ${FLIGHT_APP_PORT:-8000}
```

## API Documentation

The server provides the following endpoints:

- `POST /query/submit`: Submit a new SQL query
- `GET /query/status/{job_id}`: Check query status
- `GET /query/download/{job_id}`: Download query results
- `POST /query/schema`: Get query schema
- `POST /query/metadata`: Get query metadata
- `GET /system/queue`: Get queue status
- `GET /metrics`: Prometheus metrics endpoint
