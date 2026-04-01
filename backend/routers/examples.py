import json
from pathlib import Path
from fastapi import APIRouter, HTTPException
from backend.config import settings

router = APIRouter(prefix="/examples", tags=["examples"])

# Pre-generated example sessions — slug: display metadata
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
        "session_slug": "globalcart_demo_20e909ae",
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
        "session_slug": "cloudsync_demo_94e9a435",
    },
}


def _get_example_dir(slug: str) -> Path:
    meta = EXAMPLES.get(slug)
    if not meta:
        raise HTTPException(status_code=404, detail="Example not found")
    path = settings.project_root / "data" / "company_data" / meta["session_slug"]
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Example data not found on disk: {meta['session_slug']}")
    return path


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

    # Load rankings
    rankings = {}
    rankings_path = enriched_dir / "scored_rankings.json"
    if rankings_path.exists():
        rankings = json.loads(rankings_path.read_text())

    # List TARs
    tars = []
    tars_dir = enriched_dir / "tars"
    if tars_dir.exists():
        for f in sorted(tars_dir.glob("*.json")):
            data = json.loads(f.read_text())
            tars.append({
                "tar_id":      data.get("tar_id"),
                "ta_id":       data.get("ta_id"),
                "sobj_id":     data.get("sobj_id"),
                "gate_passed": data.get("gate_passed"),
                "confidence":  data.get("confidence_case"),
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
    """Get executive summary for an example TAR."""
    from backend.routers.pipeline import get_tar_summary
    # Reuse the summary logic by temporarily mapping to session
    meta = EXAMPLES.get(slug)
    if not meta:
        raise HTTPException(status_code=404, detail="Example not found")

    import json, os
    from dotenv import load_dotenv
    load_dotenv()

    enriched_dir = _get_example_dir(slug) / "enriched"
    tar_path = enriched_dir / "tars" / f"{tar_id}.json"
    if not tar_path.exists():
        raise HTTPException(status_code=404, detail="TAR not found")

    tar = json.loads(tar_path.read_text())

    # Call summary generation directly
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

    return {"tar_id": tar_id, "ta_id": tar.get("ta_id"),
            "sobj_id": tar.get("sobj_id"), "summary": summary}


@router.get("/{slug}/tars/{tar_id}/summary.html")
def get_example_tar_summary_html(slug: str, tar_id: str):
    """Get formatted HTML executive summary for an example TAR."""
    from fastapi.responses import HTMLResponse
    from backend.routers.pipeline import get_tar_summary_html
    # Build a minimal shim — reuse HTML generation
    summary_data = get_example_tar_summary(slug, tar_id)
    from backend.routers.pipeline import _render_summary_html
    return HTMLResponse(content=_render_summary_html(
        summary_data["summary"],
        summary_data["tar_id"],
        summary_data["ta_id"],
        summary_data["sobj_id"],
    ))
