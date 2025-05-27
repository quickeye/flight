import os
import time
import threading
import logging
import duckdb
from datetime import datetime, timezone
import tzlocal
from typing import List, Dict, Any, Optional
from pathlib import Path
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class MinIODiscoveryService:
    def __init__(
        self,
        endpoint_url: str = "http://localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        bucket_name: str = "test-data",
        region: str = "us-east-1",
        scan_interval: int = 300,
        db_path: str = "file_registry.db"
    ):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.region = region
        self.scan_interval = scan_interval
        self.db_path = db_path
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.last_updated: Optional[datetime] = None
        self.local_tz = tzlocal.get_localzone()
        
        # Initialize MinIO client
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(signature_version='s3v4', region_name=self.region)
        )
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
        
        # Initialize database
        self._init_database()

    def _ensure_bucket_exists(self):
        """Ensure the MinIO bucket exists, create if it doesn't."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Using existing bucket: {self.bucket_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"Created bucket: {self.bucket_name}")
                except Exception as create_error:
                    logger.error(f"Error creating bucket {self.bucket_name}: {str(create_error)}")
                    raise
            else:
                logger.error(f"Error checking bucket {self.bucket_name}: {str(e)}")
                raise

    def _init_database(self):
        """Initialize the DuckDB database for file tracking."""
        with duckdb.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS file_registry (
                    path VARCHAR PRIMARY KEY,
                    size_bytes BIGINT,
                    last_modified TIMESTAMP,
                    registered_at TIMESTAMP,
                    file_type VARCHAR,
                    metadata VARCHAR
                )
            """)

    def start(self):
        """Start the file discovery service."""
        if self._thread and self._thread.is_alive():
            logger.warning("File discovery service is already running")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_discovery, daemon=True)
        self._thread.start()
        logger.info("File discovery service started")

    def stop(self):
        """Stop the file discovery service."""
        if not self._thread or not self._thread.is_alive():
            return

        self._stop_event.set()
        self._thread.join(timeout=5)
        logger.info("File discovery service stopped")

    def _run_discovery(self):
        """Main discovery loop."""
        while not self._stop_event.is_set():
            try:
                self.scan_and_update()
            except Exception as e:
                logger.error(f"Error during discovery scan: {str(e)}", exc_info=True)
            
            # Wait for the next scan interval or until stopped
            self._stop_event.wait(self.scan_interval)

    def scan_and_update(self) -> int:
        """Scan the MinIO bucket for files and update the registry."""
        logger.info(f"Scanning MinIO bucket: {self.bucket_name}")
        files_found = 0
        
        start_time = datetime.now(timezone.utc)
        try:
            # List all objects in the bucket
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name):
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    try:
                        key = obj['Key']
                        if key.endswith('/'):  # Skip directories
                            continue
                            
                        file_info = {
                            'path': f"s3://{self.bucket_name}/{key}",
                            'size_bytes': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'file_type': os.path.splitext(key)[1][1:].lower() or 'unknown',
                            'metadata': str(obj.get('Metadata', {}))
                        }
                        
                        self._update_file_registry(file_info)
                        files_found += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing object {obj.get('Key')}: {str(e)}")
                        continue
                        
            self.last_updated = datetime.now(timezone.utc)
            logger.info(f"Discovery scan complete. Found {files_found} files in {(self.last_updated - start_time).total_seconds():.2f}s")
            return files_found
            
        except Exception as e:
            logger.error(f"Error scanning MinIO bucket: {str(e)}", exc_info=True)
            raise

    def _update_file_registry(self, file_info: Dict[str, Any]):
        """Update the file registry with the given file information."""
        with duckdb.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO file_registry (path, size_bytes, last_modified, registered_at, file_type, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (path) 
                DO UPDATE SET 
                    size_bytes = EXCLUDED.size_bytes,
                    last_modified = EXCLUDED.last_modified,
                    file_type = EXCLUDED.file_type,
                    metadata = EXCLUDED.metadata
            """, (
                file_info['path'],
                file_info['size_bytes'],
                file_info['last_modified'].isoformat(),
                datetime.utcnow().isoformat(),
                file_info['file_type'],
                file_info['metadata']
            ))

    def get_file_list(self, file_type: str = None) -> List[Dict[str, Any]]:
        """Get a list of registered files, optionally filtered by file type."""
        with duckdb.connect(self.db_path) as conn:
            if file_type:
                result = conn.execute("""
                    SELECT path, size_bytes, last_modified, registered_at, file_type 
                    FROM file_registry 
                    WHERE file_type = ?
                    ORDER BY last_modified DESC
                """, (file_type,)).fetchall()
            else:
                result = conn.execute("""
                    SELECT path, size_bytes, last_modified, registered_at, file_type 
                    FROM file_registry 
                    ORDER BY last_modified DESC
                """).fetchall()

            return [{
                'path': row[0],
                'size_bytes': row[1],
                'last_modified': row[2],
                'registered_at': row[3],
                'file_type': row[4]
            } for row in result]

def get_file_discovery_service() -> MinIODiscoveryService:
    """Factory function to get a file discovery service instance."""
    if not hasattr(get_file_discovery_service, '_instance'):
        get_file_discovery_service._instance = MinIODiscoveryService(
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            bucket_name=os.getenv("MINIO_BUCKET", "test-data"),
            scan_interval=int(os.getenv("FILE_DISCOVERY_INTERVAL", "300")),
            db_path=os.getenv("FILE_REGISTRY_DB", "file_registry.db")
        )
    return get_file_discovery_service._instance