import duckdb
import pyarrow as pa
import os
from typing import Generator, Optional

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
    db_path: Optional[str] = None, 
    registry_db: Optional[str] = None
) -> Generator[pa.RecordBatch, None, None]:
    """
    Execute SQL query using batch processing for better performance.
    
    Args:
        sql: SQL query to execute
        db_path: Path to DuckDB database (defaults to in-memory)
        registry_db: Path to registry database
        
    Returns:
        Generator of Arrow RecordBatches
    """
    # Get configuration from environment variables
    db_path = get_env_var('FLIGHT_DB_PATH', db_path or ':memory:')
    registry_db = get_env_var('FLIGHT_REGISTRY_DB', registry_db or '')
    
    with duckdb.connect(db_path) as conn:
        if registry_db:
            conn.execute(f"ATTACH DATABASE '{registry_db}' AS registry")
        
        # Use batch processing with configurable batch size
        batch_size = get_batch_size()
        result = conn.execute(sql)
        
        # Use fetch_record_batch_chunks() for better performance
        for batch in result.fetch_record_batch_chunks(batch_size):
            yield batch
