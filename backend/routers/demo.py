import hashlib
import os
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from backend.db.demo import (
    init_demo_db, create_demo_session, get_demo_session,
    check_quota, check_ip_rate_limit, create_recruiter_code,
    redeem_recruiter_code
)

router = APIRouter(prefix="/demo", tags=["demo"])
admin_router = APIRouter(prefix="/admin", tags=["admin"])

from backend.config import settings
ADMIN_KEY = settings.admin_key


class DemoRequest(BaseModel):
    fingerprint: str
    recruiter_code: Optional[str] = None
    email: Optional[str] = None


class RecruiterCodeRequest(BaseModel):
    label: str
    quota_runs: int = 3
    quota_tokens: int = 50000
    admin_key: str


def _hash_identity(fingerprint: str, ip: str) -> str:
    """Hash fingerprint + IP into a stable identity key."""
    raw = f"{fingerprint}:{ip}"
    return hashlib.sha256(raw.encode()).hexdigest()


@router.post("/request")
def request_demo(body: DemoRequest, request: Request):
    ip = request.client.host

    # IP rate limit check
    if not check_ip_rate_limit(ip):
        raise HTTPException(
            status_code=429,
            detail="Too many demo requests from this IP. Try again in 24 hours."
        )

    # Recruiter code flow
    if body.recruiter_code:
        if not body.email:
            raise HTTPException(status_code=400, detail="Email required for recruiter code redemption")
        token = redeem_recruiter_code(body.recruiter_code, body.email, ip)
        if not token:
            raise HTTPException(status_code=400, detail="Invalid or already used recruiter code")
        session = get_demo_session(token)
        return {
            "token":       token,
            "access_type": "recruiter_code",
            "quota_runs":  session["quota_runs"],
            "runs_used":   session["runs_used"],
            "expires_at":  session["expires_at"],
            "notice":      "By using this platform you agree that uploaded data may be retained privately to improve platform accuracy. Do not upload data containing sensitive personal information.",
        }

    # Standard fingerprint flow
    identity = _hash_identity(body.fingerprint, ip)

    # Check if identity already has a session
    from backend.db.demo import get_conn
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM demo_sessions WHERE token = ? AND is_active = 1",
            (identity,)
        ).fetchone()

    if row:
        row = dict(row)
        allowed, reason = check_quota(identity)
        if not allowed:
            raise HTTPException(status_code=402, detail=f"{reason}. To continue, use your own Anthropic API key (BYOK mode).")
        return {
            "token":       identity,
            "access_type": "demo",
            "quota_runs":  row["quota_runs"],
            "runs_used":   row["runs_used"],
            "expires_at":  row["expires_at"],
            "notice":      "By using this platform you agree that uploaded data may be retained privately to improve platform accuracy.",
        }

    # New session
    token = create_demo_session(
        email      = body.email or "",
        ip_address = ip,
        access_type = "demo",
        quota_runs  = 1,
        quota_tokens = 200000,
        token_override = identity,
    )
    session = get_demo_session(token)
    return {
        "token":       token,
        "access_type": "demo",
        "quota_runs":  session["quota_runs"],
        "runs_used":   session["runs_used"],
        "expires_at":  session["expires_at"],
        "notice":      "By using this platform you agree that uploaded data may be retained privately to improve platform accuracy. Do not upload data containing sensitive personal information.",
    }


@router.get("/status/{token}")
def demo_status(token: str):
    session = get_demo_session(token)
    if not session:
        raise HTTPException(status_code=404, detail="Demo session not found")
    allowed, reason = check_quota(token)
    return {
        "token":        token,
        "access_type":  session["access_type"],
        "runs_used":    session["runs_used"],
        "quota_runs":   session["quota_runs"],
        "tokens_used":  session["tokens_used"],
        "quota_tokens": session["quota_tokens"],
        "runs_remaining": max(0, session["quota_runs"] - session["runs_used"]),
        "allowed":      allowed,
        "reason":       reason if not allowed else None,
        "expires_at":   session["expires_at"],
    }


@router.delete("/data/{token}")
def delete_demo_data(token: str):
    """User-requested data deletion."""
    session = get_demo_session(token)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    from backend.db.demo import get_conn
    with get_conn() as conn:
        conn.execute("UPDATE demo_sessions SET is_active = 0 WHERE token = ?", (token,))
        conn.commit()
    return {"deleted": True, "token": token}


@admin_router.post("/recruiter-code")
def generate_recruiter_code(body: RecruiterCodeRequest):
    if body.admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Invalid admin key")
    code = create_recruiter_code(
        label       = body.label,
        quota_runs  = body.quota_runs,
        quota_tokens = body.quota_tokens,
    )
    return {"code": code, "label": body.label,
            "quota_runs": body.quota_runs, "quota_tokens": body.quota_tokens}
