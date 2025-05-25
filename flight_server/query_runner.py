import duckdb
import pyarrow as pa
import os
from typing import Generator, Optional
from .job_registry import JobRegistry
from .s3_utils import s3_key_for_query, save_arrow_stream_to_s3

def get_env_var(var_name: str, default: str) -> str:
    """Get environment variable with fallback to default value."""
    return os.getenv(var_name, default)

def get_batch_size() -> int:
    """Get batch size from environment variable or use default."""
    try:
        return int(get_env_var('FLIGHT_BATCH_SIZE', '1000'))
    except ValueError:
        return 1000

async def run_query(
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
        # Get configuration from environment variables
        db_path = get_env_var('FLIGHT_DB_PATH', ':memory:')
        registry_db = get_env_var('FLIGHT_REGISTRY_DB', '')
        s3_bucket = get_env_var('FLIGHT_S3_BUCKET', 'flight-cache')
        
        with duckdb.connect(db_path) as conn:
            if registry_db:
                conn.execute(f"ATTACH DATABASE '{registry_db}' AS registry")
            
            # Use batch processing with configurable batch size
            batch_size = get_batch_size()
            result = conn.execute(sql)
            
            # Convert DuckDB result to PyArrow table
            arrow_table = result.fetch_arrow_table()
            
            # Save result to S3
            key_arrow = s3_key_for_query(sql, "arrow")
            save_arrow_stream_to_s3(s3_bucket, key_arrow, arrow_table)
            
            # Update job status
            registry.update_job_status(job_id, "ready", row_count=arrow_table.num_rows, file_size=None)
    except Exception as e:
        registry.update_job_status(job_id, "error")
        raise e
