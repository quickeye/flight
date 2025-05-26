import asyncio
from flight_client.client import FlightClient
import time
from typing import List
import threading

async def run_query(client: FlightClient, job_id: str):
    """Run a query using the flight client"""
    print(f"Thread {threading.get_ident()}: Starting job {job_id}")
    # Use a simple arithmetic query based on the job_id
    job_num = int(job_id.split('_')[1])
    query = f"SELECT {job_num} + {job_num} as result"
    
    # Wrap synchronous client calls in asyncio.to_thread
    status = await asyncio.to_thread(client.submit_query, query)
    print(f"Thread {threading.get_ident()}: Query {job_id} submitted with job_id: {status.job_id}")
    
    results = await asyncio.to_thread(client.wait_for_query, status.job_id, timeout=5)
    print(f"Thread {threading.get_ident()}: Query {job_id} completed")
    
    # Print the results
    print(f"\nResults for query {job_id}:")
    print(results.to_pandas())
    print("-" * 40)

async def main():
    # Create flight client
    client = FlightClient(base_url="http://localhost:8080")
    
    # Run 20 queries in parallel
    jobs = [run_query(client, f"job_{i}") for i in range(1, 21)]
    await asyncio.gather(*jobs)

if __name__ == "__main__":
    asyncio.run(main())
