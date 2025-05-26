import asyncio
from flight_client.client import FlightClient
import time
from typing import List
import threading

def run_query(client: FlightClient, job_id: str):
    """Run a query using the flight client"""
    print(f"Thread {threading.get_ident()}: Starting job {job_id}")
    query = "SELECT * FROM parquet_scan('s3://flight-cache/*.parquet') LIMIT 1000000"
    status = client.submit_query(query)
    print(f"Thread {threading.get_ident()}: Query {job_id} submitted with job_id: {status.job_id}")
    results = client.wait_for_query(status.job_id, timeout=60)
    print(f"Thread {threading.get_ident()}: Query {job_id} completed")

async def main():
    # Create flight client
    client = FlightClient(base_url="http://localhost:8080")
    
    # Run 6 queries in parallel
    await asyncio.gather(
        run_query(client, "job_1"),
        run_query(client, "job_2"),
        run_query(client, "job_3"),
        run_query(client, "job_4"),
        run_query(client, "job_5"),
        run_query(client, "job_6")
    )

if __name__ == "__main__":
    asyncio.run(main())
