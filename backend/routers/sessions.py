import json
import sys
from pathlib import Path
from fastapi import APIRouter, HTTPException
from backend.config import settings
from backend.models.requests import (
    CompanyProfileRequest, ObjectiveRequest,
    SOBJRequest, SOBJStatusUpdate
)
from backend.models.responses import SessionResponse, SOBJResponse

# Add mk-intel to path
sys.path.insert(0, str(settings.project_root))
sys.path.insert(0, str(settings.project_root / "ingestion"))

from mk_intel_session import (
    MKSession, SessionStatus, CompanyProfile,
    Objective, SupportingObjective
)

router = APIRouter(prefix="/sessions", tags=["sessions"])

SESSIONS_DIR = settings.project_root / "data" / "sessions"


def _load_session(session_id: str) -> MKSession:
    path = SESSIONS_DIR / f"{session_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Session not found")
    return MKSession.load(str(path))


def _save_session(session: MKSession) -> None:
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    session.save(str(SESSIONS_DIR))


def _to_response(session: MKSession) -> SessionResponse:
    return SessionResponse(
        session_id     = session.session_id,
        status         = session.status.value,
        session_mode   = session.session_mode,
        company_name   = session.company.name if session.company else None,
        obj_statement  = session.objective.statement if session.objective else None,
        sobj_count     = len(session.sobjs),
        sobjs_approved = len(session.get_approved_sobjs()),
        created_at     = session.created_at,
        updated_at     = session.updated_at,
    )


@router.post("", response_model=SessionResponse)
def create_session():
    session = MKSession.new(session_mode="developer")
    _save_session(session)
    return _to_response(session)


@router.get("/{session_id}", response_model=SessionResponse)
def get_session(session_id: str):
    return _to_response(_load_session(session_id))


@router.delete("/{session_id}")
def delete_session(session_id: str):
    path = SESSIONS_DIR / f"{session_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Session not found")
    path.unlink()
    return {"deleted": session_id}


@router.post("/{session_id}/company", response_model=SessionResponse)
def set_company(session_id: str, body: CompanyProfileRequest):
    session = _load_session(session_id)
    session.company = CompanyProfile(
        name              = body.name,
        url               = body.url,
        description_input = body.description_input,
        industry          = body.industry,
        customer_type     = body.customer_type,
        source            = "user_input",
        confidence        = "high",
    )
    if session.status == SessionStatus.CREATED:
        session.advance(SessionStatus.COMPANY_IDENTIFIED)
    _save_session(session)
    return _to_response(session)


@router.post("/{session_id}/objective", response_model=SessionResponse)
def set_objective(session_id: str, body: ObjectiveRequest):
    session = _load_session(session_id)
    session.objective = Objective(
        id           = body.id,
        statement    = body.statement,
        verb         = body.verb,
        object       = body.object,
        is_validated = True,
    )
    if session.status == SessionStatus.COMPANY_IDENTIFIED:
        session.advance(SessionStatus.OBJ_SET)
    _save_session(session)
    return _to_response(session)


@router.post("/{session_id}/sobjs", response_model=SOBJResponse)
def add_sobj(session_id: str, body: SOBJRequest):
    session = _load_session(session_id)
    sobj = SupportingObjective(
        id        = body.id,
        statement = body.statement,
        direction = body.direction,
        status    = "pending",
    )
    session.sobjs.append(sobj)
    _save_session(session)
    return SOBJResponse(
        id        = sobj.id,
        statement = sobj.statement,
        direction = sobj.direction,
        status    = sobj.status,
        version   = sobj.version,
    )


@router.patch("/{session_id}/sobjs/{sobj_id}", response_model=SOBJResponse)
def update_sobj(session_id: str, sobj_id: str, body: SOBJStatusUpdate):
    session = _load_session(session_id)
    sobj = next((s for s in session.sobjs if s.id == sobj_id), None)
    if not sobj:
        raise HTTPException(status_code=404, detail="SOBJ not found")
    sobj.status         = body.status
    sobj.amendment_note = body.amendment_note
    if session.all_sobjs_approved() and session.status == SessionStatus.OBJ_SET:
        session.advance(SessionStatus.SOBJS_APPROVED)
    _save_session(session)
    return SOBJResponse(
        id        = sobj.id,
        statement = sobj.statement,
        direction = sobj.direction,
        status    = sobj.status,
        version   = sobj.version,
    )


@router.get("/{session_id}/column-mapping")
def get_column_mapping(session_id: str):
    """Get column mapping with sample values for review."""
    import json
    import pandas as pd
    from backend.config import settings

    session = _load_session(session_id)
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    company_dir = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}"
    )

    # Find company dir by session prefix if exact path doesn't exist
    if not company_dir.exists():
        base = settings.project_root / "data" / "company_data"
        for d in base.iterdir():
            if d.is_dir() and session_id[:8] in d.name:
                company_dir = d
                break

    mapping_path = company_dir / "normalized" / "column_mapping.json"
    if not mapping_path.exists():
        raise HTTPException(status_code=404, detail="Column mapping not found — run ingestion first")

    mapping_data = json.loads(mapping_path.read_text())

    # Load sample values from raw file
    samples = {}
    raw_dir = company_dir / "raw"
    if raw_dir.exists():
        raw_files = list(raw_dir.iterdir())
        if raw_files:
            try:
                sys.path.insert(0, str(settings.project_root))
                sys.path.insert(0, str(settings.project_root / "ingestion"))
                from readers import read_file
                df = read_file(raw_files[0])
                for col in df.columns:
                    vals = df[col].dropna().head(5).tolist()
                    samples[col] = [str(v) for v in vals]
            except Exception as e:
                print(f"[column-mapping] Could not load samples: {e}")

    # Get canonical fields list from normalizer
    try:
        from normalizer import CANONICAL_FIELDS
        canonical_fields = sorted(list(CANONICAL_FIELDS))
    except Exception:
        canonical_fields = []

    return {
        "session_id":       session_id,
        "mapping":          mapping_data,
        "samples":          samples,
        "canonical_fields": canonical_fields,
    }


@router.patch("/{session_id}/column-mapping")
def update_column_mapping(session_id: str, body: dict):
    """Save user-amended column mapping back to disk."""
    import json
    from backend.config import settings

    session = _load_session(session_id)
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    company_dir = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}"
    )

    if not company_dir.exists():
        base = settings.project_root / "data" / "company_data"
        for d in base.iterdir():
            if d.is_dir() and session_id[:8] in d.name:
                company_dir = d
                break

    mapping_path = company_dir / "normalized" / "column_mapping.json"
    if not mapping_path.exists():
        raise HTTPException(status_code=404, detail="Column mapping not found")

    # Load existing mapping and apply amendments
    mapping_data = json.loads(mapping_path.read_text())
    amendments = body.get("amendments", {})

    for col, canonical_field in amendments.items():
        if col in mapping_data["mappings"]:
            if canonical_field is None or canonical_field == "":
                mapping_data["mappings"][col]["canonical_field"] = None
                mapping_data["mappings"][col]["method"] = "user_skipped"
                mapping_data["mappings"][col]["confidence"] = "user"
            else:
                mapping_data["mappings"][col]["canonical_field"] = canonical_field
                mapping_data["mappings"][col]["method"] = "user_amended"
                mapping_data["mappings"][col]["confidence"] = "user"

    mapping_data["user_amended"] = True
    mapping_data["amended_at"] = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()

    mapping_path.write_text(json.dumps(mapping_data, indent=2))
    return {"status": "ok", "amended_fields": list(amendments.keys())}


@router.get("/{session_id}/export")
def export_session(session_id: str):
    """Export all session data as a ZIP file for the session owner."""
    import io
    import zipfile
    import json
    from fastapi.responses import StreamingResponse
    from backend.routers.admin import _build_session_zip, _load_session_meta

    path = SESSIONS_DIR / f"{session_id}.json"
    if not path.exists():
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


@router.get("/{session_id}/sobjs", response_model=list[SOBJResponse])
def list_sobjs(session_id: str):
    session = _load_session(session_id)
    return [
        SOBJResponse(
            id        = s.id,
            statement = s.statement,
            direction = s.direction,
            status    = s.status,
            version   = s.version,
        )
        for s in session.sobjs
    ]
