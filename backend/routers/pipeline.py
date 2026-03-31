import sys
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File
from backend.config import settings
from backend.db.jobs import create_job, get_job, get_jobs_for_session
from backend.models.responses import JobStatusResponse
from backend.tasks.pipeline import run_ingestion

sys.path.insert(0, str(settings.project_root / "mk-intel"))
sys.path.insert(0, str(settings.project_root / "mk-intel" / "ingestion"))

from mk_intel_session import MKSession

router = APIRouter(prefix="/sessions", tags=["pipeline"])

SESSIONS_DIR = settings.project_root / "data" / "sessions"


def _load_session(session_id: str) -> MKSession:
    path = SESSIONS_DIR / f"{session_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Session not found")
    return MKSession.load(str(path))


@router.post("/{session_id}/ingest")
async def ingest(session_id: str, file: UploadFile = File(...)):
    session = _load_session(session_id)

    # Save uploaded file
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    raw_dir = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session.session_id[:8]}" / "raw"
    )
    raw_dir.mkdir(parents=True, exist_ok=True)
    file_path = raw_dir / file.filename

    content = await file.read()
    file_path.write_bytes(content)

    # Create job record
    job_id = create_job(session_id, "ingest")

    # Enqueue Celery task
    task = run_ingestion.delay(
        session_id = session_id,
        file_path  = str(file_path),
        job_id     = job_id,
    )

    # Store celery task id
    from backend.db.jobs import update_job
    update_job(job_id, status="pending", celery_task_id=task.id)

    return {"job_id": job_id, "status": "pending", "file": file.filename}


@router.get("/{session_id}/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(session_id: str, job_id: str):
    job = get_job(job_id)
    if not job or job["session_id"] != session_id:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatusResponse(**job)


@router.get("/{session_id}/jobs", response_model=list[JobStatusResponse])
def list_jobs(session_id: str):
    return [JobStatusResponse(**j) for j in get_jobs_for_session(session_id)]


@router.get("/{session_id}/ta-cards")
def get_ta_cards(session_id: str):
    import json
    import pandas as pd
    from fastapi.responses import JSONResponse

    session = _load_session(session_id)
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")

    enriched_dir = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}" / "enriched"
    )
    ta_cards_path = enriched_dir / "ta_cards.parquet"

    if not ta_cards_path.exists():
        raise HTTPException(status_code=404, detail="TA cards not found — run ingestion first")

    df = pd.read_parquet(ta_cards_path)
    json_str = df.to_json(orient="records", default_handler=str)
    return JSONResponse(content=json.loads(json_str))



@router.post("/{session_id}/prefilter")
def prefilter(session_id: str):
    session = _load_session(session_id)

    # Check ta-cards exist
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    enriched_dir = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}" / "enriched"
    )
    if not (enriched_dir / "ta_cards.parquet").exists():
        raise HTTPException(status_code=400, detail="Run ingestion first")

    job_id = create_job(session_id, "prefilter")
    from backend.tasks.pipeline import run_prefilter
    task = run_prefilter.delay(session_id=session_id, job_id=job_id)
    from backend.db.jobs import update_job
    update_job(job_id, celery_task_id=task.id)
    return {"job_id": job_id, "status": "pending"}


@router.get("/{session_id}/candidates")
def get_candidates(session_id: str):
    import json
    session = _load_session(session_id)
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    enriched_dir = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}" / "enriched"
    )
    candidates_path = enriched_dir / "tar_candidates.json"
    if not candidates_path.exists():
        raise HTTPException(status_code=404, detail="Candidates not found — run prefilter first")
    return json.loads(candidates_path.read_text())


@router.post("/{session_id}/generate")
def generate(session_id: str):
    session = _load_session(session_id)
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    enriched_dir = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}" / "enriched"
    )
    if not (enriched_dir / "tar_candidates.json").exists():
        raise HTTPException(status_code=400, detail="Run prefilter first")

    job_id = create_job(session_id, "generate")
    from backend.tasks.pipeline import run_tar_generation
    task = run_tar_generation.delay(session_id=session_id, job_id=job_id)
    from backend.db.jobs import update_job
    update_job(job_id, celery_task_id=task.id)
    return {"job_id": job_id, "status": "pending"}


@router.get("/{session_id}/tars")
def list_tars(session_id: str):
    import json
    session = _load_session(session_id)
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    tars_dir = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}" / "enriched" / "tars"
    )
    if not tars_dir.exists():
        raise HTTPException(status_code=404, detail="No TARs found — run generate first")
    tars = []
    for f in sorted(tars_dir.glob("*.json")):
        data = json.loads(f.read_text())
        tars.append({
            "tar_id":        data.get("tar_id"),
            "ta_id":         data.get("ta_id"),
            "sobj_id":       data.get("sobj_id"),
            "gate_passed":   data.get("gate_passed"),
            "confidence":    data.get("confidence_case"),
        })
    return tars


@router.get("/{session_id}/tars/{tar_id}")
def get_tar(session_id: str, tar_id: str):
    import json
    session = _load_session(session_id)
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    tar_path = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}" / "enriched" / "tars" / f"{tar_id}.json"
    )
    if not tar_path.exists():
        raise HTTPException(status_code=404, detail="TAR not found")
    return json.loads(tar_path.read_text())


@router.get("/{session_id}/rankings")
def get_rankings(session_id: str):
    import json
    session = _load_session(session_id)
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    rankings_path = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}" / "enriched" / "scored_rankings.json"
    )
    if not rankings_path.exists():
        raise HTTPException(status_code=404, detail="Rankings not found — run generate first")
    return json.loads(rankings_path.read_text())
