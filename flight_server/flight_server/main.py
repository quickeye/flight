import os
import logging
import uuid
from typing import Optional, List, Dict, Annotated
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import start_http_server, generate_latest
from pydantic import BaseModel, Field
from .env_utils import get_env_var
from .middleware import PrometheusMiddleware
from .job_registry import JobRegistry

# Configure logging
log_level = get_env_var("FLIGHT_LOG_LEVEL", "info").upper()
logging.basicConfig(level=getattr(logging, log_level), format='%(asctime)s | %(levelname)s | %(process)d | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("flight_server")
logger.propagate = False  # Prevent duplicate messages

# Remove any existing handlers and set up a single handler
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create a single handler with our format
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(process)d | %(message)s', '%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Configuration
S3_BUCKET = get_env_var("FLIGHT_S3_BUCKET", "flight-cache")
QUERY_ROW_THRESHOLD = int(get_env_var("FLIGHT_QUERY_ROW_THRESHOLD", "10000"))
REGISTRY_PATH = get_env_var("FLIGHT_REGISTRY_PATH", "job_registry.db")
REGISTRY_DB = get_env_var("FLIGHT_REGISTRY_DB")

APP_HOST = get_env_var("FLIGHT_APP_HOST", "localhost")
APP_PORT = int(get_env_var("FLIGHT_APP_PORT", "8080"))
APP_DEBUG = get_env_var("FLIGHT_APP_DEBUG", "false").lower() == "true"

MAX_WORKERS = int(get_env_var("FLIGHT_MAX_WORKERS", "4"))
PROMETHEUS_PORT = int(get_env_var("FLIGHT_PROMETHEUS_PORT", "8081"))

CORS_ORIGINS = get_env_var("FLIGHT_CORS_ORIGINS", "*")
CORS_METHODS = get_env_var("FLIGHT_CORS_METHODS", "*")
CORS_HEADERS = get_env_var("FLIGHT_CORS_HEADERS", "*")

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


try:
    # Initialize Prometheus metrics
    from flight_server.middleware.prometheus import init_prometheus
    init_prometheus()
except Exception as e:
    logger.warning(f"Failed to initialize Prometheus metrics: {str(e)}")
    logger.warning("Prometheus metrics will not be available")

# Create FastAPI app with lifespan management
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager"""
    try:
        # Initialize ThreadPoolExecutor
        app.state.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        
        # Initialize registry
        app.state.registry = JobRegistry()
        
        yield
    finally:
        # Cleanup ThreadPoolExecutor
        if hasattr(app.state, 'executor'):
            app.state.executor.shutdown()
        
        # Cleanup registry
        if hasattr(app.state, 'registry'):
            app.state.registry.close()

app = FastAPI(
    title="Flight Server API",
    description="A high-performance SQL query server with Arrow streaming",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=[CORS_METHODS],
    allow_headers=[CORS_HEADERS],
)

# Add Prometheus metrics middleware and route
app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", lambda: generate_latest())

# Add logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Middleware to log requests and responses"""
    request_id = str(uuid.uuid4())
    request.scope["request_id"] = request_id
    request.state.request_id = request_id
    
    logger.info(f"Request: {request.method} {request.url.path} | Request ID: {request_id} | PID: {os.getpid()}")
    response = await call_next(request)
    logger.info(f"Response: {request.url.path} {response.status_code} | Request ID: {request_id} | PID: {os.getpid()}")
    return response

# Register routers
from flight_server.routes import health_router, query_router, metrics_router
app.include_router(query_router, tags=["query"])
app.include_router(metrics_router, tags=["metrics"])
app.include_router(health_router, tags=["health"])

# Main entry point
def main():
    """Main entry point for the application"""

    import uvicorn
    
    # Configure Uvicorn logging
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_error_logger = logging.getLogger("uvicorn.error")
    uvicorn_access_logger = logging.getLogger("uvicorn.access")
    
    # Remove all handlers from Uvicorn loggers and use our handler
    for logger in [uvicorn_logger, uvicorn_error_logger, uvicorn_access_logger]:
        logger.handlers = []
        logger.propagate = False
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    uvicorn_config = uvicorn.Config(
        app="flight_server.main:app",
        host=APP_HOST,
        port=APP_PORT,
        reload=True,
        log_level="info",
        log_config=None  # Disable default logging config
    )

    # Run the server
    server = uvicorn.Server(uvicorn_config)
    server.run()

if __name__ == "__main__":
    main()
