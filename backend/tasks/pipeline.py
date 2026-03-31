import sys
from backend.celery_app import celery_app
from backend.db.jobs import update_job
from backend.config import settings

sys.path.insert(0, str(settings.project_root / "mk-intel"))
sys.path.insert(0, str(settings.project_root / "mk-intel" / "ingestion"))


@celery_app.task(bind=True)
def add(self, x: int, y: int) -> int:
    """Toy task — proves Celery wiring works."""
    return x + y


@celery_app.task(bind=True)
def run_ingestion(self, session_id: str, file_path: str, job_id: str):
    """
    Run the full MK Intel ingestion pipeline for a session.
    Writes progress to the jobs table at each step.
    """
    from pathlib import Path
    from mk_intel_session import MKSession
    from mk_data_ingestor import MKDataIngestor

    SESSIONS_DIR = settings.project_root / "data" / "sessions"
    DATA_DIR     = settings.project_root / "data"
    ZCTA_PATH    = DATA_DIR / "reference" / "zcta_enrichment.parquet"

    try:
        update_job(job_id, status="running", progress="Loading session...")

        session = MKSession.load(str(SESSIONS_DIR / f"{session_id}.json"))

        update_job(job_id, progress="Step 1/6 — Normalizing data...")

        ingestor = MKDataIngestor(
            session           = session,
            company_data_root = DATA_DIR / "company_data",
            compliance_mode   = "standard",
            sector            = None,
            zcta_path         = ZCTA_PATH if ZCTA_PATH.exists() else None,
        )

        update_job(job_id, progress="Step 2/6 — Running ingestion pipeline...")
        ingestor.ingest(Path(file_path))

        update_job(job_id, progress="Step 3/6 — Saving session...")
        session.save(str(SESSIONS_DIR))

        update_job(job_id, status="done", progress="Ingestion complete.")

    except Exception as e:
        update_job(job_id, status="failed", error=str(e))
        raise
