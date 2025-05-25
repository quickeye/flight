from prometheus_client import (
    start_http_server, Counter, generate_latest, REGISTRY
)
import psutil
import time
from starlette.requests import Request
from starlette.responses import Response

from .middleware import PrometheusMiddleware

from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor
import uuid
import asyncio
import os

from .query_runner import run_query
from .s3_utils import *
from .job_registry import JobRegistry
from .env_utils import get_env_var, get_int_env_var

# Configuration
S3_BUCKET = get_env_var("FLIGHT_S3_BUCKET", "my-query-cache-bucket")
QUERY_ROW_THRESHOLD = get_int_env_var("FLIGHT_QUERY_ROW_THRESHOLD", 10_000)
REGISTRY_PATH = get_env_var("FLIGHT_REGISTRY_PATH", "job_registry.db")
REGISTRY_DB = get_env_var("FLIGHT_REGISTRY_DB", None)



# Server configuration
APP_PORT = get_int_env_var("FLIGHT_APP_PORT", 8000)
MAX_WORKERS = get_int_env_var("FLIGHT_MAX_WORKERS", 4)

# Start Prometheus exporter
start_http_server(port=8001)

app = FastAPI(
    title="Flight Server API",
    description="A high-performance SQL query server with Arrow streaming",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Register middleware
app.add_middleware(PrometheusMiddleware)

# Register routes
from .routes import health_router, query_router, metrics_router
app.include_router(health_router)
app.include_router(query_router)
app.include_router(metrics_router)

# CORS Configuration
CORS_ORIGINS = get_env_var("FLIGHT_CORS_ORIGINS", "*")
CORS_ORIGINS = CORS_ORIGINS.split(',') if isinstance(CORS_ORIGINS, str) else CORS_ORIGINS

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize executor and registry
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
registry = JobRegistry()

# Import metrics from middleware
from .middleware import (
    REQUEST_COUNT, REQUEST_LATENCY, REQUEST_SIZE, RESPONSE_SIZE,
    HTTP_STATUS_ERRORS, MEMORY_USAGE, QUEUE_DEPTH, ACTIVE_THREADS
)

# API Models
class QueryRequest(BaseModel):
    """SQL query request model"""
    sql: str = Field(..., description="SQL query to execute")

class QueryStatusResponse(BaseModel):
    """Response model for query status"""
    status: str = Field(..., description="Job status: pending, ready, or error")
    format: str = Field(..., description="Result format: arrow or json.gz")
    job_id: str = Field(..., description="Unique job identifier")

class ColumnSchema(BaseModel):
    """Column schema information"""
    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Column data type")

class QuerySchemaResponse(BaseModel):
    """Response model for query schema"""
    columns: List[ColumnSchema] = Field(..., description="List of columns in the result set")

class QueryMetadataResponse(BaseModel):
    """Response model for query metadata"""
    columns: List[ColumnSchema] = Field(..., description="List of columns in the result set")
    num_rows: int = Field(..., description="Number of rows in the result")
    num_columns: int = Field(..., description="Number of columns in the result")
    cached: bool = Field(..., description="Whether the result is cached")
    file_size: Optional[int] = Field(None, description="Size of the cached file in bytes")
    last_modified: Optional[str] = Field(None, description="Timestamp of last modification")
    key: str = Field(..., description="S3 key for the cached result")

class QueueStatusResponse(BaseModel):
    """Response model for queue status"""
    executor_queue_depth: int = Field(..., description="Number of pending tasks in the executor queue")
    active_workers: int = Field(..., description="Number of currently active worker threads")
    max_workers: int = Field(..., description="Maximum number of worker threads")

# Event handlers
@app.on_event("startup")
def setup():
    """Startup event handler"""
    global registry
    registry = JobRegistry()
    app.state.executor = executor

@app.on_event("shutdown")
def shutdown():
    """Shutdown event handler"""
    if hasattr(app.state, 'executor'):
        app.state.executor.shutdown()
    if registry:
        registry.conn.close()
