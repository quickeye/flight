import requests
import os
from typing import Optional, Dict, Any
from pydantic import BaseModel
import pyarrow as pa
from io import BytesIO
from .env_utils import get_env_var

class QueryStatus(BaseModel):
    status: str
    format: str
    job_id: str

class FlightClient:
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or get_env_var("FLIGHT_CLIENT_BASE_URL", "http://localhost:8000")
        self.session = requests.Session()

    def submit_query(self, sql: str) -> QueryStatus:
        """
        Submit a SQL query to the server
        
        Args:
            sql: SQL query string
            
        Returns:
            QueryStatus: Status information about the submitted query
        """
        response = self.session.post(
            f"{self.base_url}/query",
            json={"sql": sql}
        )
        response.raise_for_status()
        return QueryStatus(**response.json())

    def get_query_status(self, job_id: str) -> QueryStatus:
        """
        Get the status of a previously submitted query
        
        Args:
            job_id: The job ID returned from submit_query
            
        Returns:
            QueryStatus: Current status of the query
        """
        response = self.session.get(
            f"{self.base_url}/query/{job_id}"
        )
        response.raise_for_status()
        return QueryStatus(**response.json())

    def get_query_result(self, job_id: str) -> pa.Table:
        """
        Get the results of a completed query
        
        Args:
            job_id: The job ID returned from submit_query
            
        Returns:
            pa.Table: Arrow table containing the query results
        """
        response = self.session.get(
            f"{self.base_url}/query/{job_id}/result",
            stream=True
        )
        response.raise_for_status()
        
        # Read the Arrow stream directly from the response
        reader = pa.ipc.open_stream(response.raw)
        return reader.read_all()

    def wait_for_query(self, job_id: str, timeout: int = 30) -> pa.Table:
        """
        Wait for a query to complete and return its results
        
        Args:
            job_id: The job ID returned from submit_query
            timeout: Maximum time to wait in seconds
            
        Returns:
            pa.Table: Arrow table containing the query results
        """
        import time
        start_time = time.time()
        
        while True:
            status = self.get_query_status(job_id)
            if status.status == "ready":
                return self.get_query_result(job_id)
            elif status.status == "error":
                raise Exception(f"Query failed: {job_id}")
            
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Query did not complete within {timeout} seconds")
            
            time.sleep(1)
