from flight_client.client import FlightClient

def main():
    # Create a client instance
    client = FlightClient(base_url="http://localhost:8080")
    
    # Submit a query
    query = "SELECT 1 + 1 as result"
    print(f"Submitting query: {query}")
    status = client.submit_query(query)
    print(f"Query submitted with job_id: {status.job_id}")
    
    # Wait for the query to complete and get the results
    try:
        results = client.wait_for_query(status.job_id, timeout=60)  # Increase timeout to 60 seconds
        print("\nQuery results:")
        print(results.to_pandas())
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
