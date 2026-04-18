import sys
from dotenv import load_dotenv
load_dotenv()
from backend.celery_app import celery_app
from backend.db.jobs import update_job, init_db
init_db()
from backend.config import settings

sys.path.insert(0, str(settings.project_root))
sys.path.insert(0, str(settings.project_root / "ingestion"))


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
    ZCTA_PATH    = Path("/app/bta_data/zcta_enrichment.parquet")

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


@celery_app.task(bind=True)
def run_prefilter(self, session_id: str, job_id: str):
    """Run TAR pre-filter pipeline."""
    import json
    import pandas as pd
    from pathlib import Path
    from mk_intel_session import MKSession
    from mk_tar_prefilter import MKTARGenerator as PreFilter, build_company_context

    SESSIONS_DIR = settings.project_root / "data" / "sessions"
    DATA_DIR     = settings.project_root / "data"

    try:
        update_job(job_id, status="running", progress="Loading session...")
        session = MKSession.load(str(SESSIONS_DIR / f"{session_id}.json"))

        company_name = session.company.name if session.company else "unknown"
        slug = company_name.lower().replace(" ", "_")
        company_dir  = DATA_DIR / "company_data" / f"{slug}_{session_id[:8]}"
        enriched_dir = company_dir / "enriched"
        normalized_dir = company_dir / "normalized"

        # ── Re-normalization if mapping was amended ───────────────────────────
        # The ingest step runs normalization before the user reviews the mapping.
        # If the user amended any field mappings in the review screen, we must
        # re-run normalization with the corrected mapping before prefilter runs.
        mapping_path = normalized_dir / "column_mapping.json"
        if mapping_path.exists():
            mapping_data = json.loads(mapping_path.read_text())
            if mapping_data.get("user_amended"):
                update_job(job_id, progress="Applying amended column mapping — re-normalizing...")
                from pathlib import Path as P
                from mk_data_ingestor import MKDataIngestor
                from mk_intel_session import MKSession as S
                ZCTA_PATH = P("/app/bta_data/zcta_enrichment.parquet")

                # Find the raw uploaded file
                raw_dir = company_dir / "raw"
                raw_files = list(raw_dir.iterdir()) if raw_dir.exists() else []
                if raw_files:
                    raw_file = raw_files[0]
                    ingestor = MKDataIngestor(
                        session           = session,
                        company_data_root = DATA_DIR / "company_data",
                        compliance_mode   = "standard",
                        sector            = None,
                        zcta_path         = ZCTA_PATH if ZCTA_PATH.exists() else None,
                    )
                    # Force re-run normalization and coverage only
                    # (clustering and BTA matching already done — re-run from scratch
                    # to pick up amended mappings, then rebuild TA cards)
                    ingestor.load_and_normalize(raw_file, force=True)
                    ingestor.compute_coverage(force=True)

                    # Re-detect analysis mode after re-normalization
                    # session.analysis_mode may be stale from the saved JSON
                    if ingestor._df_norm is not None and "bta_eligible" in ingestor._df_norm.columns:
                        bta_eligible_count = int(ingestor._df_norm["bta_eligible"].sum())
                    else:
                        bta_eligible_count = 0
                    session.analysis_mode = "behavioral" if bta_eligible_count == 0 else "bta"

                    ingestor.cluster(force=True)

                    if session.analysis_mode == "behavioral":
                        ingestor.build_behavioral_ta_cards(force=True)
                    else:
                        ingestor.match_btas(force=True)
                        ingestor.enrich_zip(force=True)
                        ingestor.build_ta_cards(force=True)

                    ingestor.save()
                    session.save(str(SESSIONS_DIR))
                    update_job(job_id, progress="Re-normalization complete.")
                else:
                    update_job(job_id, progress="Warning: no raw file found — skipping re-normalization.")

        update_job(job_id, progress="Loading TA cards...")
        df = pd.read_parquet(enriched_dir / "ta_cards.parquet")
        ta_cards = df.to_dict(orient="records")

        update_job(job_id, progress="Running profile refinement...")
        generator = PreFilter(
            session         = session,
            compliance_mode = "standard",
            sector          = None,
        )
        company_context = build_company_context(session)
        refined = generator.refine_profiles(ta_cards, company_context)

        update_job(job_id, progress="Running pre-filter...")
        sobjs = session.get_approved_sobjs()
        candidates = generator.prefilter(refined, sobjs)

        update_job(job_id, progress="Saving candidates...")
        generator._save_outputs(refined, candidates, enriched_dir)

        update_job(job_id, status="done",
                   progress=f"Pre-filter complete. {len(candidates)} candidates.")

    except Exception as e:
        update_job(job_id, status="failed", error=str(e))
        raise


@celery_app.task(bind=True)
def run_tar_generation(self, session_id: str, job_id: str, demo_token: str = None, byok_key: str = None):
    """Run TAR generation and scoring pipeline."""
    import json
    from pathlib import Path
    from mk_intel_session import MKSession
    from mk_tar_generator import MKTARGenerator, tar_to_ta_input
    from mk_ta_scoring_algorithm import rank_tas_for_sobj

    SESSIONS_DIR = settings.project_root / "data" / "sessions"
    DATA_DIR     = settings.project_root / "data"

    try:
        update_job(job_id, status="running", progress="Loading session...")
        session = MKSession.load(str(SESSIONS_DIR / f"{session_id}.json"))

        company_name = session.company.name if session.company else "unknown"
        slug = company_name.lower().replace(" ", "_")
        enriched_dir = DATA_DIR / "company_data" / f"{slug}_{session_id[:8]}" / "enriched"
        tars_dir     = enriched_dir / "tars"

        update_job(job_id, progress="Loading TAR candidates...")
        candidates_raw = json.loads((enriched_dir / "tar_candidates.json").read_text())
        profiles_raw   = json.loads((enriched_dir / "refined_ta_profiles.json").read_text())

        # Reconstruct candidate proxy objects
        class CandidateProxy:
            def __init__(self, d):
                self.tar_id          = d["tar_id"]
                self.ta_id           = d["ta_id"]
                self.sobj_id         = d["sobj_id"]
                self.sobj_statement  = d["sobj_statement"]
                self.sobj_direction  = d["sobj_direction"]
                self.confidence_case = d["confidence_case"]
                rp = d["refined_profile"]
                self.refined_profile = type("RP", (), {
                    "profile":         rp["profile"],
                    "refinement_case": rp["refinement_case"],
                    "company_context": rp.get("company_context", ""),
                })()

        candidates = [CandidateProxy(c) for c in candidates_raw]

        update_job(job_id, progress=f"Generating {len(candidates)} TARs...")
        # Set BYOK key on session if provided
        if byok_key:
            session.api_key = byok_key
            session.session_mode = "byok"
        generator = MKTARGenerator(
            session         = session,
            compliance_mode = "standard",
            sector          = None,
        )
        tar_documents = generator.generate(candidates, output_dir=tars_dir)

        update_job(job_id, progress="Scoring and ranking TARs...")
        ta_inputs_by_sobj = {}
        for doc in tar_documents:
            ta_input = tar_to_ta_input(doc)
            if ta_input is None:
                continue
            ta_inputs_by_sobj.setdefault(doc.sobj_id, []).append(ta_input)

        scored_output = {}
        for sobj_id, inputs in ta_inputs_by_sobj.items():
            ranked = rank_tas_for_sobj(inputs)
            scored_output[sobj_id] = [
                {
                    "tar_id":           r.ta_id,
                    "sobj_id":          r.sobj_id,
                    "rank":             r.rank,
                    "final_score":      r.final_score,
                    "composite_score":  r.composite_score,
                    "size_modifier":    r.size_modifier,
                    "dimension_scores": r.dimension_scores,
                    "recommendation":   r.recommendation,
                    "gate_passed":      r.gate_result.passed,
                    "gate_reason":      r.gate_result.reason,
                }
                for r in ranked
            ]

        (enriched_dir / "scored_rankings.json").write_text(
            json.dumps(scored_output, indent=2, default=str)
        )

        passed = sum(1 for d in tar_documents if d.gate_passed)
        update_job(job_id, status="done",
                   progress=f"Done. {passed}/{len(tar_documents)} TARs passed gate.")

        # Increment demo quota counter
        if demo_token:
            from backend.db.demo import increment_demo_usage
            increment_demo_usage(demo_token, tokens_used=0)

    except Exception as e:
        update_job(job_id, status="failed", error=str(e))
        raise
