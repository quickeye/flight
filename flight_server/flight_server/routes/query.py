import boto3
from fastapi import APIRouter, HTTPException, Depends, Response, Request
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
import uuid
from uuid import UUID
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from ..job_registry import JobRegistry, QueryStatusResponse
from ..s3_utils import s3_key_for_query, get_env_var, hash_query
from ..env_utils import get_env_var
from ..query_runner import run_query
import logging

# Get the ThreadPoolExecutor from the app state
def get_executor(request: Request) -> ThreadPoolExecutor:
    return request.app.state.executor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Configuration
S3_BUCKET = get_env_var("FLIGHT_S3_BUCKET", "flight-cache")
QUERY_ROW_THRESHOLD = int(get_env_var("FLIGHT_QUERY_ROW_THRESHOLD", "10000"))

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

@router.post("/query", tags=["query"])
async def submit_query(
    req: QueryRequest,
    request: Request,
    registry: JobRegistry = Depends(),
    executor: ThreadPoolExecutor = Depends(get_executor)
):
    """
    Submit a SQL query for execution
    """
    sql = req.sql.strip()
    query_hash = hash_query(sql)
    key_arrow = s3_key_for_query(sql, "arrow")
    key_json = s3_key_for_query(sql, "json.gz")
    
    # Get request ID from scope
    request_id = request.scope.get("request_id")
    
    # Check if query exists in cache
    job = registry.get_job(query_hash)
    if job and job.status == "ready":
        logger.info(f"Query already cached with job_id: {job.job_id} | Request ID: {request_id} | PID: {os.getpid()}")
        return QueryStatusResponse(status="ready", format=job.format, job_id=job.job_id, request_id=request_id)
    
    # Create new job
    job_id = str(uuid.uuid4())
    registry.insert_job(job_id, "arrow", sql, key_arrow)
    logger.info(f"Created new job with job_id: {job_id} | Request ID: {request_id} | PID: {os.getpid()}")
    
    # Run query in ThreadPoolExecutor
    loop = asyncio.get_event_loop()
    loop.run_in_executor(
        executor,
        run_query,
        sql,
        job_id,
        registry
    )
    
    return QueryStatusResponse(status="pending", format="arrow", job_id=job_id, request_id=request_id)

@router.get("/query/{job_id}", tags=["query"])
async def get_query_status(job_id: str, request: Request, registry: JobRegistry = Depends()):
    """
    Get query execution status
    """
    # Get request ID from scope
    request_id = request.scope.get("request_id")
    
    job = registry.get_job(job_id)
    if not job:
        logger.error(f"Job not found for job_id: {job_id} | Request ID: {request_id} | PID: {os.getpid()}")
        raise HTTPException(status_code=404, detail="Job not found")
    
    logger.info(f"Job status for {job_id}: {job['status']} | Request ID: {request_id} | PID: {os.getpid()}")
    return QueryStatusResponse(status=job["status"], format=job["format"], job_id=job_id, request_id=request_id)

@router.get("/query/{job_id}/result", tags=["query"])
async def get_query_result(job_id: str, request: Request, registry: JobRegistry = Depends()):
    """
    Get query result data
    """
    job = registry.get_job(job_id)
    if not job:
        logger.error(f"Job not found for job_id: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job["status"] != "ready":
        logger.error(f"Query not ready for job_id: {job_id}")
        raise HTTPException(status_code=400, detail="Query not ready")
    
    # Get the S3 key for the arrow file
    key_arrow = s3_key_for_query(job["sql"], "arrow")
    
    # Download the file from S3
    s3_client = boto3.client(
        "s3",
        endpoint_url=get_env_var("FLIGHT_S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=get_env_var("FLIGHT_S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=get_env_var("FLIGHT_S3_SECRET_KEY", "minioadmin"),
        region_name=get_env_var("FLIGHT_S3_REGION", "us-east-1")
    )
    
    logger.info(f"Downloading result for job_id: {job_id} from S3 key: {key_arrow}")
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=key_arrow)
    
    # Return streaming response
    return StreamingResponse(
        response["Body"],
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename=result.arrow"
        }
    )

@router.get("/query/{job_id}/schema", tags=["query"])
async def get_query_schema(job_id: str, request: Request, registry: JobRegistry = Depends()):
    """
    Get query result schema
    """
    job = registry.get_job_by_id(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status != "ready":
        raise HTTPException(status_code=400, detail="Query is not ready")
    
    # Get schema from S3
    schema = await get_schema_from_s3(job.sql)
    return QuerySchemaResponse(columns=schema)

@router.get("/query/{job_id}/results", tags=["query"])
async def get_query_results(job_id: str, registry: JobRegistry = Depends()):
    """
    Get query results
    """
    job = registry.get_job_by_id(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status != "ready":
        raise HTTPException(status_code=400, detail="Query is not ready")
    
    # Get results from S3
    if job.format == "arrow":
        return StreamingResponse(
            get_arrow_stream(job.sql),
            media_type="application/octet-stream"
        )
    else:
        return StreamingResponse(
            get_json_stream(job.sql),
            media_type="application/json"
        )
