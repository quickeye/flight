from prometheus_client import (
    start_http_server, Counter, Histogram, Gauge
)
import psutil
import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

# Start Prometheus exporter
start_http_server(port=8001)

REQUEST_COUNT = Counter(
    "http_requests_total", "Total HTTP requests",
    ["method", "endpoint", "http_status"]
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "Request latency",
    ["method", "endpoint"]
)

REQUEST_SIZE = Histogram(
    "http_request_size_bytes", "Request size in bytes",
    ["method", "endpoint"]
)

RESPONSE_SIZE = Histogram(
    "http_response_size_bytes", "Response size in bytes",
    ["method", "endpoint"]
)

HTTP_STATUS_ERRORS = Counter(
    "http_status_errors_total", "Count of 4xx and 5xx errors",
    ["status_code"]
)

MEMORY_USAGE = Gauge(
    "flight_memory_usage_bytes", "Resident memory usage"
)

QUEUE_DEPTH = Gauge(
    "flight_queue_depth", "Pending executor queue size"
)

ACTIVE_THREADS = Gauge(
    "flight_active_threads", "Active executor threads"
)

class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        method = request.method
        endpoint = request.url.path
        content_length = request.headers.get("content-length")
        size = int(content_length) if content_length and content_length.isdigit() else 0
        REQUEST_SIZE.labels(method, endpoint).observe(size)

        response = await call_next(request)
        duration = time.time() - start_time
        status_code = response.status_code

        REQUEST_COUNT.labels(method, endpoint, str(status_code)).inc()
        REQUEST_LATENCY.labels(method, endpoint).observe(duration)
        RESPONSE_SIZE.labels(method, endpoint).observe(len(response.body or b""))
        if 400 <= status_code < 600:
            HTTP_STATUS_ERRORS.labels(str(status_code)).inc()

        MEMORY_USAGE.set(psutil.Process().memory_info().rss)
        QUEUE_DEPTH.set(executor._work_queue.qsize())
        ACTIVE_THREADS.set(len(executor._threads))

        return response

# Register middleware
app.add_middleware(PrometheusMiddleware)


from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor
import uuid
import asyncio
import os

from .query_runner import run_query
from .s3_utils import *
from .job_registry import JobRegistry

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "my-query-cache-bucket")
QUERY_ROW_THRESHOLD = 10_000
REGISTRY_PATH = "job_registry.db"
REGISTRY_DB = None  # Optional DuckDB registry

app = FastAPI()
executor = ThreadPoolExecutor(max_workers=4)
registry = JobRegistry(REGISTRY_PATH)

# Request model
class QueryRequest(BaseModel):
    sql: str

# In-memory cache of job statuses (temporary, replaced by registry)
@app.on_event("startup")
def setup():
    print("Server starting up...")

@app.on_event("shutdown")
def shutdown():
    executor.shutdown()

@app.post("/query/submit")
async def submit_query(req: QueryRequest):
    sql = req.sql.strip()
    query_hash = hash_query(sql)
    key_arrow = s3_key_for_query(sql, "arrow")
    key_json = s3_key_for_query(sql, "json.gz")

    # Check for existing job
    job = registry.get_by_hash(query_hash)
    if job and job["status"] == "ready":
        if job["format"] == "json.gz":
            return {"ready": True, "source": "cache", "job_id": job["job_id"]}
        else:
            return {"ready": False, "job_id": job["job_id"]}

    # Otherwise, execute the query
    job_id = str(uuid.uuid4())
    registry.insert_job(job_id, sql, query_hash, "arrow", key_arrow)

    def process():
        try:
            result = run_query(sql, registry_db=REGISTRY_DB)
            if result.num_rows <= QUERY_ROW_THRESHOLD:
                save_json_gz_to_s3(S3_BUCKET, key_json, result)
                registry.update_job_status(job_id, "ready", row_count=result.num_rows, file_size=len(result.to_pybytes()))
            else:
                save_arrow_stream_to_s3(S3_BUCKET, key_arrow, result)
                registry.update_job_status(job_id, "ready", row_count=result.num_rows, file_size=None)
        except Exception as e:
            registry.update_job_status(job_id, "error")
            print(f"Query failed: {e}")

    asyncio.get_event_loop().run_in_executor(executor, process)
    return {"ready": False, "job_id": job_id}

@app.get("/query/status/{job_id}")
def get_query_status(job_id: str):
    job = registry.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job ID not found")
    return {
        "status": job["status"],
        "format": job["format"],
        "job_id": job_id
    }

@app.get("/query/download/{job_id}")
def download_query_result(job_id: str):
    job = registry.get_job(job_id)
    if not job or job["status"] != "ready":
        raise HTTPException(status_code=404, detail="Result not available")
    if job["format"] != "arrow":
        raise HTTPException(status_code=400, detail="Only arrow format is supported for download")

    s3_key = job["s3_key"]

    def stream_generator():
        s3_obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        reader = pa_ipc.open_stream(s3_obj["Body"])
        for batch in reader:
            yield batch.to_pybytes()

    return StreamingResponse(stream_generator(), media_type="application/vnd.apache.arrow.stream")

@app.post("/query/schema")
def get_query_schema(req: QueryRequest):
    sql = req.sql.strip()
    query_hash = hash_query(sql)
    key_arrow = s3_key_for_query(sql, "arrow")

    if s3_key_exists(S3_BUCKET, key_arrow):
        table = stream_arrow_from_s3(S3_BUCKET, key_arrow)
    else:
        table = run_query(sql, registry_db=REGISTRY_DB)

    return {
        "columns": [{"name": field.name, "type": str(field.type)} for field in table.schema]
    }

@app.post("/query/metadata")
def get_query_metadata(req: QueryRequest):
    sql = req.sql.strip()
    query_hash = hash_query(sql)
    key_arrow = s3_key_for_query(sql, "arrow")
    metadata = {}

    if s3_key_exists(S3_BUCKET, key_arrow):
        table = stream_arrow_from_s3(S3_BUCKET, key_arrow)
        head = s3.head_object(Bucket=S3_BUCKET, Key=key_arrow)
        metadata["cached"] = True
        metadata["file_size"] = head["ContentLength"]
        metadata["last_modified"] = head["LastModified"].isoformat()
    else:
        table = run_query(sql, registry_db=REGISTRY_DB)
        metadata["cached"] = False
        metadata["file_size"] = None
        metadata["last_modified"] = None

    metadata.update({
        "columns": [{"name": f.name, "type": str(f.type)} for f in table.schema],
        "num_rows": table.num_rows,
        "num_columns": table.num_columns,
        "key": key_arrow
    })

    return metadata

@app.get("/system/queue")
def system_queue_status():
    return {
        "executor_queue_depth": executor._work_queue.qsize(),
        "active_workers": executor._threads and len(executor._threads),
        "max_workers": executor._max_workers
    }
