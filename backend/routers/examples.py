import json
from pathlib import Path
from fastapi import APIRouter, HTTPException
from backend.config import settings

router = APIRouter(prefix="/examples", tags=["examples"])

EXAMPLES = {
    "globalcart": {
        "slug":        "globalcart",
        "name":        "GlobalCart",
        "sector":      "E-Commerce",
        "description": "Subscription e-commerce platform. 50,000 customers, 2 SOBJs: renew subscription and reactivate cancelled accounts. No ZIP enrichment — clean Case A baseline.",
        "sobj_count":  2,
        "ta_count":    7,
        "tar_count":   4,
        "zip_enrichment": False,
        "session_slug": "afd8c333",
    },
    "cloudsync": {
        "slug":        "cloudsync",
        "name":        "CloudSync",
        "sector":      "SaaS",
        "description": "B2B SaaS platform. 1,500 customers with ZIP enrichment — demonstrates Cases A, B1, B2. SOBJs: reduce cancellations and upgrade plan tier.",
        "sobj_count":  2,
        "ta_count":    6,
        "tar_count":   4,
        "zip_enrichment": True,
        "session_slug": "fcb77ed8",
    },
}


def _get_example_dir(slug: str) -> Path:
    meta = EXAMPLES.get(slug)
    if not meta:
        raise HTTPException(status_code=404, detail="Example not found")
    # Try direct slug first, then search by session_id prefix
    direct = settings.project_root / "data" / "company_data" / meta["session_slug"]
    if direct.exists():
        return direct
    # Search for directory containing the session_id prefix
    session_id = meta["session_slug"]
    base = settings.project_root / "data" / "company_data"
    if base.exists():
        for d in base.iterdir():
            if d.is_dir() and session_id in d.name:
                return d
    raise HTTPException(status_code=404, detail=f"Example data not found on disk: {meta['session_slug']}")


@router.get("")
def list_examples():
    """List all pre-generated example datasets."""
    return list(EXAMPLES.values())


@router.get("/{slug}")
def get_example(slug: str):
    """Get example metadata + available TARs."""
    meta = EXAMPLES.get(slug)
    if not meta:
        raise HTTPException(status_code=404, detail="Example not found")
    enriched_dir = _get_example_dir(slug) / "enriched"

    rankings = {}
    rankings_path = enriched_dir / "scored_rankings.json"
    if rankings_path.exists():
        rankings = json.loads(rankings_path.read_text())

    tars = []
    tars_dir = enriched_dir / "tars"
    if tars_dir.exists():
        for f in sorted(tars_dir.glob("*.json")):
            data = json.loads(f.read_text())
            tars.append({
                "tar_id":       data.get("tar_id"),
                "ta_id":        data.get("ta_id"),
                "sobj_id":      data.get("sobj_id"),
                "gate_passed":  data.get("gate_passed"),
                "confidence":   data.get("confidence_case"),
                "audience_name": data.get("audience_name"),
            })

    return {
        **meta,
        "tars":     tars,
        "rankings": rankings,
    }


@router.get("/{slug}/tars/{tar_id}")
def get_example_tar(slug: str, tar_id: str):
    """Get full TAR JSON for an example."""
    enriched_dir = _get_example_dir(slug) / "enriched"
    tar_path = enriched_dir / "tars" / f"{tar_id}.json"
    if not tar_path.exists():
        raise HTTPException(status_code=404, detail="TAR not found")
    return json.loads(tar_path.read_text())


@router.get("/{slug}/tars/{tar_id}/summary")
def get_example_tar_summary(slug: str, tar_id: str):
    """Get executive summary for an example TAR — generates and persists audience_name."""
    meta = EXAMPLES.get(slug)
    if not meta:
        raise HTTPException(status_code=404, detail="Example not found")

    import os
    from dotenv import load_dotenv
    load_dotenv()

    enriched_dir = _get_example_dir(slug) / "enriched"
    tar_path = enriched_dir / "tars" / f"{tar_id}.json"
    if not tar_path.exists():
        raise HTTPException(status_code=404, detail="TAR not found")

    tar = json.loads(tar_path.read_text())

    import anthropic
    from backend.routers.pipeline import _build_summary_prompt
    prompt = _build_summary_prompt(tar)
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

    # Persist audience_name back to TAR JSON
    if not summary.get("parse_error") and summary.get("audience_name"):
        try:
            tar["audience_name"] = summary["audience_name"]
            tar_path.write_text(json.dumps(tar, indent=2, default=str))
        except Exception:
            pass

    return {
        "tar_id":  tar_id,
        "ta_id":   tar.get("ta_id"),
        "sobj_id": tar.get("sobj_id"),
        "summary": summary,
    }


@router.get("/{slug}/tars/{tar_id}/summary.html")
def get_example_tar_summary_html(slug: str, tar_id: str):
    """Get formatted HTML executive summary for an example TAR."""
    from fastapi.responses import HTMLResponse
    summary_data = get_example_tar_summary(slug, tar_id)
    from backend.routers.pipeline import _render_summary_html
    return HTMLResponse(content=_render_summary_html(
        summary_data["summary"],
        summary_data["tar_id"],
        summary_data["ta_id"],
        summary_data["sobj_id"],
    ))


@router.get("/{slug}/tars/export/{sobj_id}")
def export_example_tars_html(slug: str, sobj_id: str):
    """Export all TARs for a given SOBJ as a single print-ready HTML file."""
    from fastapi.responses import HTMLResponse
    from backend.routers.pipeline import _render_export_html

    meta = EXAMPLES.get(slug)
    if not meta:
        raise HTTPException(status_code=404, detail="Example not found")

    enriched_dir = _get_example_dir(slug) / "enriched"
    tars_dir = enriched_dir / "tars"
    if not tars_dir.exists():
        raise HTTPException(status_code=404, detail="No TARs found")

    # Load rankings for sort order
    rankings = {}
    rankings_path = enriched_dir / "scored_rankings.json"
    if rankings_path.exists():
        raw_rankings = json.loads(rankings_path.read_text())
        for r in raw_rankings.get(sobj_id, []):
            rankings[r["tar_id"]] = r.get("rank", 999)

    tars = []
    for f in sorted(tars_dir.glob("*.json")):
        data = json.loads(f.read_text())
        if data.get("sobj_id") == sobj_id and data.get("gate_passed"):
            tars.append(data)

    if not tars:
        raise HTTPException(status_code=404, detail=f"No TARs found for {sobj_id}")

    # Sort by rank
    tars.sort(key=lambda t: rankings.get(t.get("ta_id", ""), 999))

    html = _render_export_html(tars, meta["name"], sobj_id)
    return HTMLResponse(content=html, headers={
        "Content-Disposition": f"inline; filename=mk-intel-{slug}-{sobj_id}.html"
    })
