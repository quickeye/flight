from flight_client.client import FlightClient
import time
import pyarrow as pa
from io import BytesIO

def test_client():
    # Create a client instance
    client = FlightClient()
    
    # Submit a query
    query = "SELECT 1 + 1 as result"
    print(f"Submitting query: {query}")
    status = client.submit_query(query)
    print(f"Query submitted with job_id: {status.job_id}")
    
    # Check status periodically
    print("\nChecking query status...")
    for _ in range(10):  # Try up to 10 times
        status = client.get_query_status(status.job_id)
        print(f"Status: {status.status}")
        if status.status == "ready":
            break
        time.sleep(1)
    
    # Get the results
    if status.status == "ready":
        try:
            results = client.get_query_result(status.job_id)
            print("\nQuery results:")
            print(results.to_pandas())
        except Exception as e:
            print(f"Error getting results: {str(e)}")
    else:
        print("\nQuery did not complete successfully")

test_client()
