import sqlite3
from datetime import datetime
from typing import Optional, Dict
from uuid import UUID
from pydantic import BaseModel
import threading
from contextlib import contextmanager
import hashlib

from .env_utils import get_env_var

class JobStatus(str):
    PENDING = "pending"
    RUNNING = "running"
    READY = "ready"
    ERROR = "error"

    def __str__(self) -> str:
        return self.value


class QueryStatusResponse(BaseModel):
    status: str
    format: str
    job_id: str
    request_id: str


class JobRegistry:
    def __init__(self):
        self.db_path = get_env_var("FLIGHT_REGISTRY_PATH", "job_registry.db")
        self._lock = threading.Lock()
        self._init_schema()

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        try:
            yield conn
        finally:
            conn.close()

    def _init_schema(self):
        """Initialize the database schema"""
        with self._lock:
            with self._get_connection() as conn:
                # Create queries table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS queries (
                        query_hash TEXT PRIMARY KEY,
                        sql TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                """)
                
                # Create jobs table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id TEXT PRIMARY KEY,
                        query_hash TEXT NOT NULL,
                        status TEXT NOT NULL,
                        format TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        completed_at TEXT,
                        row_count INTEGER,
                        file_size INTEGER,
                        s3_key TEXT NOT NULL,
                        FOREIGN KEY (query_hash) REFERENCES queries (query_hash)
                    )
                """)
                
                # Create indexes
                conn.execute("CREATE INDEX IF NOT EXISTS idx_query_hash ON jobs (query_hash)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON jobs (status)")
                conn.commit()

    def hash_query(self, sql: str) -> str:
        return hashlib.sha256(sql.encode()).hexdigest()

    def insert_query(self, sql: str) -> str:
        """
        Insert a new query into the queries table if it doesn't exist
        Returns the query_hash
        """
        query_hash = self.hash_query(sql)
        now = datetime.utcnow().isoformat()
        
        with self._lock:
            with self._get_connection() as conn:
                # Check if query already exists
                cur = conn.cursor()
                cur.execute("SELECT query_hash FROM queries WHERE query_hash = ?", (query_hash,))
                if not cur.fetchone():
                    # Insert new query
                    conn.execute("""
                        INSERT INTO queries (query_hash, sql, created_at)
                        VALUES (?, ?, ?)
                    """, (query_hash, sql, now))
                conn.commit()
                return query_hash

    def get_query(self, query_hash: str) -> Optional[Dict]:
        """Get a query by its hash"""
        with self._lock:
            with self._get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM queries WHERE query_hash = ?", (query_hash,))
                row = cur.fetchone()
                return dict(zip([d[0] for d in cur.description], row)) if row else None

    def insert_job(self, job_id: str, format: str, sql: str, s3_key: str):
        """Insert a new job into the database"""
        now = datetime.utcnow().isoformat()
        query_hash = self.hash_query(sql)
        
        with self._lock:
            with self._get_connection() as conn:
                # Make sure query exists in queries table
                conn.execute("""
                    INSERT OR IGNORE INTO queries (query_hash, sql, created_at)
                    VALUES (?, ?, ?)
                """, (query_hash, sql, now))
                
                # Insert job with the query_hash
                conn.execute("""
                    INSERT INTO jobs
                    (job_id, query_hash, status, format, created_at, s3_key)
                    VALUES (?, ?, 'pending', ?, ?, ?)
                """, (job_id, query_hash, format, now, s3_key))
                conn.commit()

    def update_job_status(self, job_id: str, status: str, row_count: Optional[int] = None, file_size: Optional[int] = None):
        """Update a job's status"""
        now = datetime.utcnow().isoformat()
        with self._lock:
            with self._get_connection() as conn:
                conn.execute("""
                    UPDATE jobs
                    SET status = ?, completed_at = ?, row_count = ?, file_size = ?
                    WHERE job_id = ?
                """, (status, now, row_count, file_size, job_id))
                conn.commit()

    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get a job by its ID"""
        with self._lock:
            with self._get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT j.*, q.sql 
                    FROM jobs j 
                    JOIN queries q ON j.query_hash = q.query_hash 
                    WHERE j.job_id = ?
                """, (job_id,))
                row = cur.fetchone()
                return dict(zip([d[0] for d in cur.description], row)) if row else None

    def get_job_by_hash(self, query_hash: str) -> Optional[Dict]:
        """Get the most recent job for a query hash"""
        with self._lock:
            with self._get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT j.*, q.sql 
                    FROM jobs j 
                    JOIN queries q ON j.query_hash = q.query_hash 
                    WHERE j.query_hash = ? 
                    ORDER BY j.created_at DESC 
                    LIMIT 1
                """, (query_hash,))
                row = cur.fetchone()
                return dict(zip([d[0] for d in cur.description], row)) if row else None

    def close(self):
        """Close the database connection"""
        # No-op since we're using connection pooling
        pass
