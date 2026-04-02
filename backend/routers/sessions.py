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
