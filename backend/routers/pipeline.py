import sys
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Request
from backend.config import settings
from backend.db.jobs import create_job, get_job, get_jobs_for_session
from backend.models.responses import JobStatusResponse
from backend.tasks.pipeline import run_ingestion

sys.path.insert(0, str(settings.project_root))
sys.path.insert(0, str(settings.project_root / "ingestion"))

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
def generate(session_id: str, request: Request):
    session = _load_session(session_id)
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    enriched_dir = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}" / "enriched"
    )
    if not (enriched_dir / "tar_candidates.json").exists():
        raise HTTPException(status_code=400, detail="Run prefilter first")

    # Demo quota check
    demo_token = request.headers.get("X-Demo-Token")
    if demo_token:
        from backend.db.demo import check_quota
        allowed, reason = check_quota(demo_token)
        if not allowed:
            raise HTTPException(status_code=402, detail=f"{reason}. To continue, use your own Anthropic API key.")

    job_id = create_job(session_id, "generate")
    from backend.tasks.pipeline import run_tar_generation
    task = run_tar_generation.delay(
        session_id=session_id,
        job_id=job_id,
        demo_token=demo_token,
    )
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


def _build_summary_prompt(tar: dict) -> str:
    eff   = tar.get("effectiveness", {})
    susc  = tar.get("susceptibility", {})
    narr  = tar.get("narrative_and_actions", {})
    vuln  = tar.get("vulnerabilities", {})
    acc   = tar.get("accessibility", [])
    hdr   = tar.get("header", {})
    ta    = hdr.get("target_audience", {})
    sobj  = hdr.get("supporting_objective", {})
    assess = tar.get("assessment", {})
    trace = tar.get("traceability", {})
    conf  = trace.get("confidence", {})

    top_actions = narr.get("recommended_actions", [])[:3]
    top_motives = vuln.get("motives", [])[:3]
    top_channels = [
        c for c in (acc if isinstance(acc, list) else [])
        if not c.get("violates_restrictions")
    ][:3]
    size_est = ta.get("audience_size_estimate", {})

    return f"""You are a senior marketing strategist. Generate a concise executive summary
of the following Target Audience Report. The summary should be readable in under 2 minutes
and actionable for a senior decision-maker.

TAR CONTEXT:
- Audience: {ta.get("definition")}
- SOBJ: {sobj.get("statement")}
- Effectiveness rating: {eff.get("rating")}/5
- Susceptibility rating: {susc.get("rating")}/5
- Audience size: {size_est.get("value")} {size_est.get("unit")}
- Confidence: {conf.get("level")} — {str(conf.get("rationale", ""))[:200]}

TOP MOTIVES:
{chr(10).join(f"- {m.get('id')}: {m.get('description')} [{m.get('priority')}]" for m in top_motives)}

MAIN ARGUMENT:
IF: {narr.get("main_argument", {}).get("premise", "")}
THEN: {narr.get("main_argument", {}).get("consequence", "")}

TOP 3 RECOMMENDED ACTIONS:
{chr(10).join(f"- {a.get('action_id')}: {str(a.get('description', ''))[:120]}" for a in top_actions)}

TOP CHANNELS:
{chr(10).join(f"- {c.get('channel_name')} (reach {c.get('reach_quality')}/5)" for c in top_channels)}

KEY RISKS:
{chr(10).join(f"- {str(r.get('description', ''))[:100]}" for r in eff.get('restrictions', [])[:3])}

BASELINE BEHAVIOR: {str(assess.get("baseline_behavior", ""))[:200]}
TARGET BEHAVIOR: {str(assess.get("target_behavior", ""))[:200]}

Return a JSON object with this exact structure:
{{
    "audience_name":    "short human-friendly name for this audience in this campaign context, 3-5 words, e.g. Loyal Mid-Career Subscribers or Value-Conscious Homeowners. Never use BTA archetype name or cluster ID.",
    "audience":         "one-line audience description with key stats",
    "objective":        "SOBJ statement",
    "verdict":          "FIRST PRIORITY|HIGH PRIORITY|MEDIUM PRIORITY|LOWER PRIORITY — one sentence rationale",
    "why_this_audience": "2-3 sentences on why this segment matters for this objective",
    "the_case":         "2-3 sentences summarizing the persuasion logic",
    "top_actions": [
        {{"action": "action description", "channel": "channel name", "timing": "when"}}
    ],
    "key_risk":         "single most important risk to campaign success",
    "confidence":       "high|medium|low",
    "confidence_note":  "one sentence on confidence basis"
}}
Return ONLY the JSON object."""


@router.get("/{session_id}/tars/{tar_id}/summary")
def get_tar_summary(session_id: str, tar_id: str):
    import json
    import sys
    from backend.config import settings
    from dotenv import load_dotenv
    load_dotenv()

    sys.path.insert(0, str(settings.project_root))
    sys.path.insert(0, str(settings.project_root / "ingestion"))

    session = _load_session(session_id)
    company_name = session.company.name if session.company else "unknown"
    slug = company_name.lower().replace(" ", "_")
    tar_path = (
        settings.project_root / "data" / "company_data" /
        f"{slug}_{session_id[:8]}" / "enriched" / "tars" / f"{tar_id}.json"
    )
    if not tar_path.exists():
        raise HTTPException(status_code=404, detail="TAR not found")

    tar = json.loads(tar_path.read_text())

    # Build summary prompt
    eff  = tar.get("effectiveness", {})
    susc = tar.get("susceptibility", {})
    narr = tar.get("narrative_and_actions", {})
    vuln = tar.get("vulnerabilities", {})
    acc  = tar.get("accessibility", [])
    hdr  = tar.get("header", {})
    ta   = hdr.get("target_audience", {})
    sobj = hdr.get("supporting_objective", {})
    assess = tar.get("assessment", {})

    top_actions = narr.get("recommended_actions", [])[:3]
    top_motives = vuln.get("motives", [])[:3]
    top_channels = [
        c for c in (acc if isinstance(acc, list) else [])
        if not c.get("violates_restrictions")
    ][:3]

    size_est = ta.get("audience_size_estimate", {})
    trace = tar.get("traceability", {})
    conf = trace.get("confidence", {})

    prompt = f"""You are a senior marketing strategist. Generate a concise executive summary
of the following Target Audience Report. The summary should be readable in under 2 minutes
and actionable for a senior decision-maker.

TAR CONTEXT:
- Audience: {ta.get('definition')}
- SOBJ: {sobj.get('statement')}
- Effectiveness rating: {eff.get('rating')}/5
- Susceptibility rating: {susc.get('rating')}/5
- Audience size: {size_est.get('value')} {size_est.get('unit')}
- Confidence: {conf.get('level')} — {conf.get('rationale', '')[:200]}

TOP MOTIVES:
{chr(10).join(f"- {m.get('id')}: {m.get('description')} [{m.get('priority')}]" for m in top_motives)}

MAIN ARGUMENT:
IF: {narr.get('main_argument', {}).get('premise', '')}
THEN: {narr.get('main_argument', {}).get('consequence', '')}

TOP 3 RECOMMENDED ACTIONS:
{chr(10).join(f"- {a.get('action_id')}: {a.get('description', '')[:120]}" for a in top_actions)}

TOP CHANNELS:
{chr(10).join(f"- {c.get('channel_name')} (reach {c.get('reach_quality')}/5)" for c in top_channels)}

KEY RISKS (from restrictions):
{chr(10).join(f"- {r.get('description', '')[:100]}" for r in eff.get('restrictions', [])[:3])}

BASELINE BEHAVIOR: {assess.get('baseline_behavior', '')[:200]}
TARGET BEHAVIOR: {assess.get('target_behavior', '')[:200]}

Return a JSON object with this exact structure:
{{
    "audience_name":    "short human-friendly name for this audience in this campaign context, 3-5 words, e.g. Loyal Mid-Career Subscribers or Value-Conscious Homeowners. Never use BTA archetype name or cluster ID.",
    "audience":         "one-line audience description with key stats",
    "objective":        "SOBJ statement",
    "verdict":          "FIRST PRIORITY|HIGH PRIORITY|MEDIUM PRIORITY|LOWER PRIORITY — one sentence rationale",
    "why_this_audience": "2-3 sentences on why this segment matters for this objective",
    "the_case":         "2-3 sentences summarizing the persuasion logic",
    "top_actions": [
        {{"action": "action description", "channel": "channel name", "timing": "when"}}
    ],
    "key_risk":         "single most important risk to campaign success",
    "confidence":       "high|medium|low",
    "confidence_note":  "one sentence on confidence basis"
}}
Return ONLY the JSON object."""

    # Call Haiku synchronously — fast enough, no background job needed
    import anthropic
    import os
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model      = "claude-haiku-4-5-20251001",
        max_tokens = 1500,
        messages   = [{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip().replace("```json", "").replace("```", "").strip()
    try:
        summary = json.loads(raw)
    except json.JSONDecodeError:
        summary = {"raw": raw, "parse_error": True}

    # Persist audience_name back to TAR JSON if generated
    if not summary.get("parse_error") and summary.get("audience_name"):
        try:
            tar["audience_name"] = summary["audience_name"]
            tar_path.write_text(json.dumps(tar, indent=2, default=str))
        except Exception:
            pass

    return {
        "tar_id":   tar_id,
        "ta_id":    tar.get("ta_id"),
        "sobj_id":  tar.get("sobj_id"),
        "summary":  summary,
    }



@router.get("/{session_id}/tars/{tar_id}/summary.html")
def get_tar_summary_html(session_id: str, tar_id: str):
    from fastapi.responses import HTMLResponse

    summary_response = get_tar_summary(session_id, tar_id)
    s = summary_response["summary"]
    tar_id_val = summary_response["tar_id"]
    ta_id_val  = summary_response["ta_id"]
    sobj_id    = summary_response["sobj_id"]

    verdict = s.get("verdict", "")
    verdict_class = (
        "first"  if "FIRST"  in verdict else
        "high"   if "HIGH"   in verdict else
        "medium" if "MEDIUM" in verdict else "lower"
    )

    actions_rows = ""
    for i, a in enumerate(s.get("top_actions", []), 1):
        desc    = a.get("action", "")
        channel = a.get("channel", "")
        timing  = a.get("timing", "")
        actions_rows += (
            f'''<div class="action">'''
            f'''<span class="action-num">{i}</span>'''
            f'''<div class="action-body">'''
            f'''<div class="action-desc">{desc}</div>'''
            f'''<div class="action-meta">'''
            f'''<span class="tag">{channel}</span>'''
            f'''<span class="tag timing">{timing}</span>'''
            f'''</div></div></div>'''
        )

    audience_name  = s.get("audience_name", "")
    audience       = s.get("audience", "")
    objective      = s.get("objective", "")
    why_audience   = s.get("why_this_audience", "")
    the_case       = s.get("the_case", "")
    key_risk       = s.get("key_risk", "")
    confidence     = s.get("confidence", "low")
    conf_note      = s.get("confidence_note", "")

    css = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #f5f5f5; color: #1a1a1a; padding: 32px 16px; }
.card { max-width: 800px; margin: 0 auto; background: white;
        border-radius: 12px; box-shadow: 0 2px 16px rgba(0,0,0,0.08); overflow: hidden; }
.header { background: #1a1a2e; color: white; padding: 32px; }
.header .label { font-size: 11px; text-transform: uppercase;
                 letter-spacing: 1.5px; color: #8888aa; margin-bottom: 8px; }
.header h1 { font-size: 22px; font-weight: 600; margin-bottom: 6px; }
.header .sobj-sub { font-size: 13px; color: #7777aa; margin-bottom: 4px; }
.header .sobj { font-size: 14px; color: #aaaacc; }
.verdict-bar { padding: 16px 32px; font-size: 14px; font-weight: 600; }
.verdict-bar.first  { background: #e6f4ea; color: #1e7e34; border-left: 4px solid #28a745; }
.verdict-bar.high   { background: #e8f0fe; color: #1a56db; border-left: 4px solid #1a56db; }
.verdict-bar.medium { background: #fff8e1; color: #b45309; border-left: 4px solid #f59e0b; }
.verdict-bar.lower  { background: #fdf2f2; color: #b91c1c; border-left: 4px solid #ef4444; }
.body { padding: 32px; }
.section { margin-bottom: 28px; }
.section h2 { font-size: 11px; text-transform: uppercase; letter-spacing: 1.5px;
              color: #888; margin-bottom: 10px; }
.section p { font-size: 15px; line-height: 1.6; color: #333; }
.meta-row { display: flex; gap: 24px; margin-bottom: 28px;
            padding: 16px; background: #f8f9fa; border-radius: 8px; }
.meta-item { flex: 1; }
.meta-item .label { font-size: 11px; text-transform: uppercase;
                    letter-spacing: 1px; color: #888; margin-bottom: 4px; }
.meta-item .value { font-size: 15px; font-weight: 600; color: #1a1a1a; }
.action { display: flex; gap: 16px; margin-bottom: 12px;
          padding: 14px; background: #f8f9fa; border-radius: 8px; align-items: flex-start; }
.action-num { background: #1a1a2e; color: white; width: 26px; height: 26px;
              border-radius: 50%; display: flex; align-items: center; justify-content: center;
              font-size: 12px; font-weight: 600; flex-shrink: 0; margin-top: 2px; }
.action-desc { font-size: 14px; line-height: 1.5; color: #333; margin-bottom: 6px; }
.action-meta { display: flex; gap: 8px; flex-wrap: wrap; }
.tag { font-size: 11px; padding: 3px 10px; border-radius: 12px;
       background: #e8f0fe; color: #1a56db; font-weight: 500; }
.tag.timing { background: #e6f4ea; color: #1e7e34; }
.risk-box { background: #fff8e1; border-left: 4px solid #f59e0b;
            padding: 14px 16px; border-radius: 0 8px 8px 0; }
.risk-box p { font-size: 14px; line-height: 1.5; color: #333; }
.confidence { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; }
.conf-dot { width: 8px; height: 8px; border-radius: 50%; }
.conf-high .conf-dot { background: #28a745; }
.conf-medium .conf-dot { background: #f59e0b; }
.conf-low .conf-dot { background: #ef4444; }
.footer { padding: 16px 32px; background: #f8f9fa;
          font-size: 12px; color: #888; border-top: 1px solid #eee; }
"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TAR Summary — {ta_id_val}</title>
<style>{css}</style>
</head>
<body>
<div class="card">
  <div class="header">
    <div class="label">Executive Summary &middot; {tar_id_val}</div>
    <h1>{audience_name}</h1>
    <div class="sobj-sub">{audience}</div>
    <div class="sobj">{objective}</div>
  </div>
  <div class="verdict-bar {verdict_class}">{verdict}</div>
  <div class="body">
    <div class="meta-row">
      <div class="meta-item">
        <div class="label">Audience ID</div>
        <div class="value">{ta_id_val}</div>
      </div>
      <div class="meta-item">
        <div class="label">SOBJ</div>
        <div class="value">{sobj_id}</div>
      </div>
      <div class="meta-item">
        <div class="label">Confidence</div>
        <div class="confidence conf-{confidence}">
          <span class="conf-dot"></span>{confidence.upper()} &mdash; {conf_note}
        </div>
      </div>
    </div>
    <div class="section">
      <h2>Why this audience</h2>
      <p>{why_audience}</p>
    </div>
    <div class="section">
      <h2>The case</h2>
      <p>{the_case}</p>
    </div>
    <div class="section">
      <h2>Top recommended actions</h2>
      {actions_rows}
    </div>
    <div class="section">
      <h2>Key risk</h2>
      <div class="risk-box"><p>{key_risk}</p></div>
    </div>
  </div>
  <div class="footer">Generated by MK Intel &middot; {tar_id_val} &middot; Draft</div>
</div>
</body>
</html>"""

    return HTMLResponse(content=html)


@router.post("/admin/setup-store")
def setup_segment_store(admin_key: str):
    """One-time setup: load BTA segments into ChromaDB."""
    from backend.config import settings
    if admin_key != settings.admin_key:
        raise HTTPException(status_code=403, detail="Invalid admin key")
    import sys
    sys.path.insert(0, str(settings.project_root / "store"))
    from segment_store import load_segments, store_info
    n = load_segments(jsonl_path=Path("/app/bta_data/mk_bta_rag_corpus.jsonl"))
    return {"loaded": n, "store_info": store_info()}
