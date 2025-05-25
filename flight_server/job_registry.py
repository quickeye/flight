import sqlite3
from datetime import datetime
from typing import Optional

from .env_utils import get_env_var

class JobRegistry:
    def __init__(self):
        db_path = get_env_var("FLIGHT_REGISTRY_PATH", "job_registry.db")
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self):
        with self.conn:
            self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                sql TEXT NOT NULL,
                query_hash TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending', 'ready', 'error')),
                format TEXT NOT NULL CHECK (format IN ('arrow', 'json.gz')),
                created_at TEXT NOT NULL,
                completed_at TEXT,
                s3_key TEXT NOT NULL,
                row_count INTEGER,
                file_size INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_query_hash ON jobs (query_hash);
            CREATE INDEX IF NOT EXISTS idx_status ON jobs (status);
            """)

    def insert_job(self, job_id, sql, query_hash, format, s3_key):
        now = datetime.utcnow().isoformat()
        with self.conn:
            self.conn.execute("""INSERT INTO jobs
                (job_id, sql, query_hash, status, format, created_at, s3_key)
                VALUES (?, ?, ?, 'pending', ?, ?, ?)
            """, (job_id, sql, query_hash, format, now, s3_key))

    def update_job_status(self, job_id, status, row_count=None, file_size=None):
        now = datetime.utcnow().isoformat()
        with self.conn:
            self.conn.execute("""UPDATE jobs
                SET status = ?, completed_at = ?, row_count = ?, file_size = ?
                WHERE job_id = ?
            """, (status, now, row_count, file_size, job_id))

    def get_job(self, job_id):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
        row = cur.fetchone()
        return dict(zip([d[0] for d in cur.description], row)) if row else None

    def get_by_hash(self, query_hash):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE query_hash = ? ORDER BY created_at DESC LIMIT 1", (query_hash,))
        row = cur.fetchone()
        return dict(zip([d[0] for d in cur.description], row)) if row else None
