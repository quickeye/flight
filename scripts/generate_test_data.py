import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import BytesIO
import boto3
from botocore.client import Config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_minio_client():
    """Initialize and return a MinIO client."""
    return boto3.client(
        's3',
        endpoint_url='http://localhost:9000',  # Default MinIO port
        aws_access_key_id='minioadmin',       # Default MinIO access key
        aws_secret_access_key='minioadmin',   # Default MinIO secret key
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def generate_sample_data(num_rows=100):
    """Generate sample data with mixed categorical and numerical columns."""
    np.random.seed(42)
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(days=x) for x in range(num_rows)]
    
    data = {
        'date': dates,
        'temperature': np.random.normal(72, 10, num_rows).round(1),
        'humidity': np.random.uniform(30, 90, num_rows).round(1),
        'pressure': np.random.normal(1013, 15, num_rows).round(2),
        'wind_speed': np.random.gamma(2, 2, num_rows).round(1),
        'rainfall': np.random.exponential(0.2, num_rows).round(2),
        'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], num_rows),
        'is_active': np.random.choice([True, False], num_rows, p=[0.7, 0.3])
    }
    
    # Add some null values
    for col in data.keys():
        if np.random.random() < 0.05:
            mask = np.random.choice([True, False], num_rows, p=[0.05, 0.95])
            if pd.api.types.is_numeric_dtype(pd.Series(data[col])):
                data[col] = np.where(mask, np.nan, data[col])
            else:
                data[col] = np.where(mask, None, data[col])
    
    return pd.DataFrame(data)

def upload_parquet_to_minio(df, bucket, key, minio_client):
    """Upload a DataFrame to MinIO as a parquet file."""
    try:
        # Convert DataFrame to parquet bytes
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        # Upload to MinIO
        minio_client.upload_fileobj(
            parquet_buffer,
            bucket,
            key,
            ExtraArgs={'ContentType': 'application/octet-stream'}
        )
        logger.info(f"Uploaded {key} to MinIO bucket {bucket}")
        return True
    except Exception as e:
        logger.error(f"Error uploading {key} to MinIO: {str(e)}")
        return False

def ensure_bucket_exists(bucket_name, minio_client):
    """Ensure the bucket exists, create if it doesn't."""
    try:
        minio_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists")
    except:
        try:
            minio_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Created bucket {bucket_name}")
        except Exception as e:
            logger.error(f"Error creating bucket {bucket_name}: {str(e)}")
            raise

def main():
    # Initialize MinIO client
    minio_client = get_minio_client()
    bucket_name = "data"  # Change this to your desired bucket name
    
    try:
        # Ensure bucket exists
        ensure_bucket_exists(bucket_name, minio_client)
        
        # Generate test data
        logger.info("Generating test data...")
        df1 = generate_sample_data(100)
        df2 = generate_sample_data(100)
        
        # Add dataset-specific columns
        df1['dataset'] = 'ds1'
        df2['dataset'] = 'ds2'
        
        # Upload to MinIO
        logger.info("Uploading datasets to MinIO...")
        upload_parquet_to_minio(df1, bucket_name, 'ds1/ds1.parquet', minio_client)
        upload_parquet_to_minio(df2, bucket_name, 'ds2/ds2.parquet', minio_client)
        
        logger.info("Data generation and upload complete!")
        logger.info(f"Sample data from ds1:\n{df1.head(3)}")
        logger.info(f"\nData types:\n{df1.dtypes}")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()