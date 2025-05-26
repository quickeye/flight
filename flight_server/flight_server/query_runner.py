import duckdb
import pyarrow as pa
import os
from typing import Generator, Optional
from .job_registry import JobRegistry
from .s3_utils import s3_key_for_query, save_arrow_stream_to_s3, S3_BUCKET
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_env_var(var_name: str, default: str) -> str:
    """Get environment variable with fallback to default value."""
    return os.getenv(var_name, default)

def get_batch_size() -> int:
    """Get batch size from environment variable or use default."""
    try:
        return int(get_env_var('FLIGHT_BATCH_SIZE', '1000'))
    except ValueError:
        return 1000

def run_query(
    sql: str, 
    job_id: str,
    registry: JobRegistry
) -> None:
    """
    Execute SQL query and save results to S3.
    
    Args:
        sql: SQL query to execute
        job_id: Unique job identifier
        registry: Job registry instance
    """
    try:
        import threading
        logger.info(f"Starting query execution in thread {threading.get_ident()} for job {job_id}")
        
        # Get configuration from environment variables
        db_path = get_env_var('FLIGHT_DB_PATH', ':memory:')
        registry_db = get_env_var('FLIGHT_REGISTRY_DB', '')
        
        with duckdb.connect(db_path) as conn:
            # Execute query and get Arrow batches
            result = conn.execute(sql)
            batch_size = get_batch_size()  # Re-enable batch size
            
            # Get a RecordBatchReader for streaming
            reader = result.fetch_record_batch()
            
            # Save result to S3 using batches
            key_arrow = s3_key_for_query(sql, "arrow")
            
            # Save the complete Arrow stream to S3
            save_arrow_stream_to_s3(S3_BUCKET, key_arrow, reader)
            
            # Update job status
            registry.update_job_status(job_id, "ready")
            logger.info(f"Query completed successfully in thread {threading.get_ident()} for job {job_id}")
    except Exception as e:
        logger.error(f"Query execution failed in thread {threading.get_ident()} for job {job_id}: {str(e)}")
        registry.update_job_status(job_id, "error")
        raise e
