import io
import json
import zipfile
from pathlib import Path
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from backend.config import settings
from backend.db.jobs import get_conn as get_jobs_conn

router = APIRouter(prefix="/admin", tags=["admin"])

SESSIONS_DIR = settings.project_root / "data" / "sessions"
COMPANY_DATA_DIR = settings.project_root / "data" / "company_data"


def _check_admin_key(admin_key: str) -> None:
    if admin_key != settings.admin_key:
        raise HTTPException(status_code=403, detail="Invalid admin key")


def _load_session_meta(session_id: str) -> dict:
    """Load session metadata from JSON file."""
    path = SESSIONS_DIR / f"{session_id}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        company = data.get("company") or {}
        objective = data.get("objective") or {}
        sobjs = data.get("sobjs") or []
        return {
            "session_id":   session_id,
            "company_name": company.get("name", "Unknown"),
            "industry":     company.get("industry", ""),
            "customer_type": company.get("customer_type", ""),
            "objective":    objective.get("statement", ""),
            "sobj_count":   len(sobjs),
            "status":       data.get("status", "unknown"),
            "session_mode": data.get("session_mode", ""),
            "created_at":   data.get("created_at", ""),
            "updated_at":   data.get("updated_at", ""),
        }
    except Exception:
        return None


def _find_company_dir(session_id: str) -> Path:
    """Find the company_data directory for a session."""
    if not COMPANY_DATA_DIR.exists():
        return None
    for d in COMPANY_DATA_DIR.iterdir():
        if d.is_dir() and session_id[:8] in d.name:
            return d
    return None


def _build_session_zip(session_id: str) -> io.BytesIO:
    """Build a ZIP file containing all inputs and outputs for a session."""
    buf = io.BytesIO()

    session_path = SESSIONS_DIR / f"{session_id}.json"
    company_dir = _find_company_dir(session_id)

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:

        # Session metadata
        if session_path.exists():
            zf.write(session_path, f"session_{session_id[:8]}/session.json")

        if company_dir:
            # --- INPUTS ---

            # Raw uploaded file
            raw_dir = company_dir / "raw"
            if raw_dir.exists():
                for f in raw_dir.iterdir():
                    if f.is_file():
                        zf.write(f, f"session_{session_id[:8]}/inputs/raw/{f.name}")

            # Column mapping
            col_map = company_dir / "normalized" / "column_mapping.json"
            if col_map.exists():
                zf.write(col_map, f"session_{session_id[:8]}/inputs/column_mapping.json")

            # Coverage report
            coverage = company_dir / "normalized" / ".coverage_computed"
            if coverage.exists():
                zf.write(coverage, f"session_{session_id[:8]}/inputs/coverage_computed.txt")

            # Normalized records (parquet)
            norm = company_dir / "normalized" / "normalized_records.parquet"
            if norm.exists():
                zf.write(norm, f"session_{session_id[:8]}/inputs/normalized_records.parquet")

            # --- PIPELINE ---

            # TA cards
            ta_csv = company_dir / "enriched" / "ta_cards.csv"
            if ta_csv.exists():
                zf.write(ta_csv, f"session_{session_id[:8]}/pipeline/ta_cards.csv")

            ta_parquet = company_dir / "enriched" / "ta_cards.parquet"
            if ta_parquet.exists():
                zf.write(ta_parquet, f"session_{session_id[:8]}/pipeline/ta_cards.parquet")

            # Refined TA profiles
            refined = company_dir / "enriched" / "refined_ta_profiles.json"
            if refined.exists():
                zf.write(refined, f"session_{session_id[:8]}/pipeline/refined_ta_profiles.json")

            # TAR candidates
            candidates = company_dir / "enriched" / "tar_candidates.json"
            if candidates.exists():
                zf.write(candidates, f"session_{session_id[:8]}/pipeline/tar_candidates.json")

            # Custom archetypes (Case C)
            archetypes = company_dir / "enriched" / "custom_archetypes.json"
            if archetypes.exists():
                zf.write(archetypes, f"session_{session_id[:8]}/pipeline/custom_archetypes.json")

            # --- OUTPUTS ---

            # Scored rankings
            rankings = company_dir / "enriched" / "scored_rankings.json"
            if rankings.exists():
                zf.write(rankings, f"session_{session_id[:8]}/outputs/scored_rankings.json")

            # TARs
            tars_dir = company_dir / "enriched" / "tars"
            if tars_dir.exists():
                for f in sorted(tars_dir.glob("*.json")):
                    zf.write(f, f"session_{session_id[:8]}/outputs/tars/{f.name}")

        # Jobs log from SQLite
        try:
            with get_jobs_conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE session_id = ? ORDER BY started_at",
                    (session_id,)
                ).fetchall()
            jobs_data = [dict(r) for r in rows]
            zf.writestr(
                f"session_{session_id[:8]}/jobs_log.json",
                json.dumps(jobs_data, indent=2)
            )
        except Exception:
            pass

    buf.seek(0)
    return buf


# ── Admin endpoints ───────────────────────────────────────────────────────────

@router.get("/sessions")
def list_all_sessions(admin_key: str):
    """List all sessions with metadata. Admin only."""
    _check_admin_key(admin_key)

    if not SESSIONS_DIR.exists():
        return []

    sessions = []
    for path in sorted(SESSIONS_DIR.glob("*.json"), reverse=True):
        session_id = path.stem
        meta = _load_session_meta(session_id)
        if meta:
            # Check if TARs were generated
            company_dir = _find_company_dir(session_id)
            tar_count = 0
            if company_dir:
                tars_dir = company_dir / "enriched" / "tars"
                if tars_dir.exists():
                    tar_count = len(list(tars_dir.glob("*.json")))
            meta["tar_count"] = tar_count
            meta["has_data"] = company_dir is not None
            sessions.append(meta)

    return sessions


@router.get("/sessions/{session_id}/export")
def admin_export_session(session_id: str, admin_key: str):
    """Export all data for a session as ZIP. Admin only."""
    _check_admin_key(admin_key)

    session_path = SESSIONS_DIR / f"{session_id}.json"
    if not session_path.exists():
        raise HTTPException(status_code=404, detail="Session not found")

    meta = _load_session_meta(session_id)
    company_slug = (meta.get("company_name", "session") or "session").lower().replace(" ", "_")
    filename = f"mk_intel_{company_slug}_{session_id[:8]}.zip"

    buf = _build_session_zip(session_id)

    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
