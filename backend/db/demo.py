import sqlite3
import uuid
import secrets
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from backend.config import settings

DB_PATH = settings.project_root / "backend" / "db" / "mk_intel.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_demo_db() -> None:
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS demo_sessions (
                token           TEXT PRIMARY KEY,
                email           TEXT,
                ip_address      TEXT,
                access_type     TEXT DEFAULT 'demo',
                created_at      TEXT,
                last_active_at  TEXT,
                expires_at      TEXT,
                runs_used       INTEGER DEFAULT 0,
                tokens_used     INTEGER DEFAULT 0,
                quota_runs      INTEGER DEFAULT 2,
                quota_tokens    INTEGER DEFAULT 30000,
                is_active       INTEGER DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS recruiter_codes (
                code            TEXT PRIMARY KEY,
                label           TEXT,
                created_at      TEXT,
                used_at         TEXT,
                used_by_email   TEXT,
                quota_runs      INTEGER DEFAULT 3,
                quota_tokens    INTEGER DEFAULT 50000,
                is_used         INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ip_rate_limits (
                ip_address      TEXT PRIMARY KEY,
                request_count   INTEGER DEFAULT 0,
                window_start    TEXT
            )
        """)
        conn.commit()


def create_demo_session(
    email: str,
    ip_address: str,
    access_type: str = "demo",
    quota_runs: int = 2,
    quota_tokens: int = 30000,
    token_override: str = None,
) -> str:
    token      = token_override if token_override else secrets.token_urlsafe(32)
    now        = datetime.now(timezone.utc).isoformat()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO demo_sessions
            (token, email, ip_address, access_type, created_at,
             last_active_at, expires_at, quota_runs, quota_tokens)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (token, email, ip_address, access_type,
              now, now, expires_at, quota_runs, quota_tokens))
        conn.commit()
    return token


def get_demo_session(token: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM demo_sessions WHERE token = ?", (token,)
        ).fetchone()
    return dict(row) if row else None


def increment_demo_usage(token: str, tokens_used: int = 0) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute("""
            UPDATE demo_sessions
            SET runs_used = runs_used + 1,
                tokens_used = tokens_used + ?,
                last_active_at = ?
            WHERE token = ?
        """, (tokens_used, now, token))
        conn.commit()


def check_quota(token: str) -> tuple[bool, str]:
    session = get_demo_session(token)
    if not session:
        return False, "Invalid demo token"
    if not session["is_active"]:
        return False, "Demo session is inactive"
    expires = datetime.fromisoformat(session["expires_at"])
    if datetime.now(timezone.utc) > expires:
        return False, "Demo session expired"
    if session["runs_used"] >= session["quota_runs"]:
        return False, f"Demo quota exceeded ({session['quota_runs']} runs used)"
    if session["tokens_used"] >= session["quota_tokens"]:
        return False, f"Token quota exceeded ({session['quota_tokens']} tokens used)"
    return True, ""


def create_recruiter_code(label: str, quota_runs: int = 3, quota_tokens: int = 50000) -> str:
    code = secrets.token_urlsafe(16)
    now  = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO recruiter_codes (code, label, created_at, quota_runs, quota_tokens)
            VALUES (?, ?, ?, ?, ?)
        """, (code, label, now, quota_runs, quota_tokens))
        conn.commit()
    return code


def redeem_recruiter_code(code: str, email: str, ip_address: str) -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM recruiter_codes WHERE code = ? AND is_used = 0", (code,)
        ).fetchone()
        if not row:
            return None
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            UPDATE recruiter_codes SET is_used = 1, used_at = ?, used_by_email = ?
            WHERE code = ?
        """, (now, email, code))
        conn.commit()
    return create_demo_session(
        email, ip_address,
        access_type="recruiter_code",
        quota_runs=row["quota_runs"],
        quota_tokens=row["quota_tokens"],
    )


def check_ip_rate_limit(ip_address: str, max_requests: int = 3, window_hours: int = 24) -> bool:
    now    = datetime.now(timezone.utc)
    window = now - timedelta(hours=window_hours)
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM ip_rate_limits WHERE ip_address = ?", (ip_address,)
        ).fetchone()
        if not row:
            conn.execute(
                "INSERT INTO ip_rate_limits (ip_address, request_count, window_start) VALUES (?, 1, ?)",
                (ip_address, now.isoformat())
            )
            conn.commit()
            return True
        window_start = datetime.fromisoformat(row["window_start"])
        if window_start < window:
            conn.execute(
                "UPDATE ip_rate_limits SET request_count = 1, window_start = ? WHERE ip_address = ?",
                (now.isoformat(), ip_address)
            )
            conn.commit()
            return True
        if row["request_count"] >= max_requests:
            return False
        conn.execute(
            "UPDATE ip_rate_limits SET request_count = request_count + 1 WHERE ip_address = ?",
            (ip_address,)
        )
        conn.commit()
        return True
