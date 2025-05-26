import hashlib
import boto3
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import io
import gzip
import json
from .env_utils import get_env_var

# S3 Configuration
S3_REGION = get_env_var("FLIGHT_S3_REGION", "us-east-1")
S3_ENDPOINT_URL = get_env_var("FLIGHT_S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = get_env_var("FLIGHT_S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = get_env_var("FLIGHT_S3_SECRET_KEY", "minioadmin")

# Create S3 client with configuration
s3_config = {
    "endpoint_url": S3_ENDPOINT_URL,
    "aws_access_key_id": S3_ACCESS_KEY,
    "aws_secret_access_key": S3_SECRET_KEY,
    "region_name": S3_REGION
}

s3 = boto3.client("s3", **s3_config)

# Create the bucket if it doesn't exist
S3_BUCKET = get_env_var("FLIGHT_S3_BUCKET", "flight-cache")
try:
    s3.create_bucket(Bucket=S3_BUCKET)
except s3.exceptions.BucketAlreadyExists:
    pass
except s3.exceptions.BucketAlreadyOwnedByYou:
    pass

def hash_query(query: str) -> str:
    return hashlib.sha256(query.encode()).hexdigest()

def s3_key_for_query(query: str, ext: str) -> str:
    return f"{S3_BUCKET}/{hash_query(query)}.{ext}"

def save_arrow_stream_to_s3(bucket: str, key: str, reader: pa.RecordBatchReader):
    # Create a streaming writer
    with pa.BufferOutputStream() as stream:
        with pa.ipc.new_stream(stream, reader.schema) as writer:
            # Write batches from the reader
            while True:
                try:
                    batch = reader.read_next_batch()
                    writer.write_batch(batch)
                except StopIteration:
                    break
        
        # Get the complete stream
        buffer = stream.getvalue()
        
        # Upload to S3
        s3.upload_fileobj(io.BytesIO(buffer.to_pybytes()), bucket, key)

def stream_arrow_from_s3(bucket: str, key: str) -> pa.Table:
    s3_obj = s3.get_object(Bucket=bucket, Key=key)
    body = s3_obj['Body']
    reader = pa_ipc.open_stream(body)
    return reader.read_all()

def save_json_gz_to_s3(bucket: str, key: str, table: pa.Table):
    buf = io.BytesIO()
    json_bytes = json.dumps(table.to_pylist()).encode("utf-8")
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(json_bytes)
    buf.seek(0)
    s3.upload_fileobj(buf, bucket, key)

def s3_key_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False
