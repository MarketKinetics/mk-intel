import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from backend.config import settings

DB_PATH = settings.project_root / "data" / "mk_intel.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create tables if they don't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id         TEXT PRIMARY KEY,
                session_id     TEXT NOT NULL,
                job_type       TEXT NOT NULL,
                status         TEXT DEFAULT 'pending',
                progress       TEXT,
                started_at     TEXT,
                completed_at   TEXT,
                error          TEXT,
                celery_task_id TEXT
            )
        """)
        conn.commit()


def create_job(session_id: str, job_type: str) -> str:
    """Create a new job record. Returns job_id."""
    job_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO jobs (job_id, session_id, job_type, status, started_at)
            VALUES (?, ?, ?, 'pending', ?)
        """, (job_id, session_id, job_type,
              datetime.now(timezone.utc).isoformat()))
        conn.commit()
    return job_id


def update_job(
    job_id: str,
    status: Optional[str] = None,
    progress: Optional[str] = None,
    error: Optional[str] = None,
    celery_task_id: Optional[str] = None,
) -> None:
    """Update a job record."""
    fields = []
    values = []
    if status:
        fields.append("status = ?")
        values.append(status)
        if status in ("done", "failed"):
            fields.append("completed_at = ?")
            values.append(datetime.now(timezone.utc).isoformat())
    if progress:
        fields.append("progress = ?")
        values.append(progress)
    if error:
        fields.append("error = ?")
        values.append(error)
    if celery_task_id:
        fields.append("celery_task_id = ?")
        values.append(celery_task_id)
    if not fields:
        return
    values.append(job_id)
    with get_conn() as conn:
        conn.execute(
            f"UPDATE jobs SET {', '.join(fields)} WHERE job_id = ?",
            values
        )
        conn.commit()


def get_job(job_id: str) -> Optional[dict]:
    """Get a job record by ID."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
    return dict(row) if row else None


def get_jobs_for_session(session_id: str) -> list[dict]:
    """Get all jobs for a session."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE session_id = ? ORDER BY started_at DESC",
            (session_id,)
        ).fetchall()
    return [dict(r) for r in rows]
