# MK Intel — Project Handoff Document
## For Claude: Read this first, then request the files listed at the bottom

---

## What is MK Intel

MK Intel is an AI-first Target Audience Analysis platform built as a portfolio piece for AI engineering roles. It takes a company's customer data, maps customers to societal archetypes derived from U.S. census data, and produces ranked, evidence-based Target Audience Reports (TARs) — structured analytical documents that tell a marketing team which audience segments to target for a specific campaign objective, and exactly how to do it.

**Stack:** Python, FastAPI, Celery, Redis, ChromaDB, SQLite, Anthropic API (Claude Haiku)

**Project root:** `/Users/marcomagnolo/Projects/Market_Kinetics/`

**GitHub:** github.com/MarketKinetics/mk-intel (public, live — Railway deployment pending)

**MK Intel is Module 1 of the Market Kinetics platform:**
- MK Intel — Target audience analysis + report generation ✅ Active
- MK Product — Communication product studio (product worksheet → multimedia campaign drafts) 📋 Planned
- MK Campaign — Campaign execution and channel orchestration 📋 Planned
- MK Engage — Meeting simulator (personality traits + objectives → interactive rehearsal testbed) 📋 Planned

---

## Architecture overview

```
Societal baseline (built once, reused)
    ACS PUMS + GSS + Pew → K-Prototypes clustering → 7 BTAs → ChromaDB

Per-company pipeline (runs per session)
    Upload CSV → normalize → cluster → BTA match → ZIP enrich → TA cards
         ↓
    TAR pre-filter (profile refinement A/B1/B2/C + SOBJ scoring)
         ↓
    TAR generation (8 sequential LLM sections per candidate)
         ↓
    Scoring algorithm (4 dimensions → ranked output)
         ↓
    Executive summary (JSON + HTML, on-demand)
```

---

## Key terminology

| Term | Meaning |
|---|---|
| BTA | Baseline Target Audience — one of 7 societal archetypes from ACS/GSS/Pew |
| CS | Company Segment — business-specific cluster mapped to a BTA (e.g. CS01_BTA_06) |
| TAR | Target Audience Report — full structured analytical doc per (CS × SOBJ) |
| OBJ | Campaign objective (e.g. "Reduce subscription churn") |
| SOBJ | Supporting objective (e.g. "TA renews subscription at next billing cycle") |
| ZIP enrichment | Cross-check BTA assignments against ZIP-level demographics — Cases A/B1/B2/C |
| Case A | Full ZIP alignment → high confidence |
| Case B1 | Income diverges, age+race match → adjust income descriptors only |
| Case B2 | Race diverges, age+income match → adjust cultural/media layer only |
| Case C | Full conflict → LLM custom archetype, confidence penalty |

---

## The 7 BTAs (societal archetypes)

| ID | Name | Pop share |
|---|---|---|
| BTA_00 | Diverse Mid-Life Workers | 17.2% |
| BTA_01 | Older Non-Partnered Adults | 16.2% |
| BTA_02 | Young Hispanic Working Adults | 5.4% |
| BTA_03 | Retired Renters | 12.5% |
| BTA_04 | Mid-Career Homeowners | 15.1% |
| BTA_05 | Young Non-Owning Singles | 14.4% |
| BTA_06 | Established Mid-Career Homeowners | 19.1% |

---

## File structure

```
Market_Kinetics/
├── mk-intel/                          ← core platform scripts
│   ├── mk_intel_session.py            ← session model (MKSession, SessionStatus)
│   ├── mk_tar_prefilter.py            ← Stage 1: profile refinement + pre-filter
│   ├── mk_tar_generator.py            ← Stage 2: TAR generation + scoring adapter
│   ├── mk_ta_scoring_algorithm.py     ← scoring algorithm (4 dimensions)
│   ├── utils.py                       ← API key management, log_api_usage
│   ├── docs/
│   │   ├── ARCHITECTURE.md            ← public GitHub narrative
│   │   ├── mk_tar_schema_v2.json      ← TAR schema
│   │   └── mk_canonical_behavioral_schema_v1.json
│   └── ingestion/
│       ├── mk_data_ingestor.py        ← full ingestion pipeline
│       ├── zip_enrichment.py          ← ZIP enrichment + BTA confidence
│       ├── synthetic_data_generator.py
│       ├── normalizer.py
│       ├── coverage.py
│       ├── readers.py
│       └── utils.py
├── backend/                           ← FastAPI backend
│   ├── main.py                        ← FastAPI app entry point
│   ├── config.py                      ← Pydantic settings (admin_key added)
│   ├── celery_app.py                  ← Celery + Redis config
│   ├── routers/
│   │   ├── sessions.py                ← session + company + OBJ endpoints
│   │   ├── pipeline.py                ← ingest + prefilter + generate + summary + HTML
│   │   ├── demo.py                    ← auth endpoints (fingerprint + recruiter code)
│   │   └── examples.py                ← pre-generated example datasets
│   ├── tasks/
│   │   └── pipeline.py                ← Celery tasks (run_ingestion, run_prefilter, run_tar_generation)
│   ├── db/
│   │   ├── jobs.py                    ← app-owned job state (SQLite)
│   │   └── demo.py                    ← demo auth + quota (SQLite) ✅ Complete
│   └── models/
│       ├── requests.py
│       └── responses.py
├── notebooks/                         ← all 15 notebooks
│   ├── 01-11 (b-versions for 05/06)   ← societal baseline methodology
│   ├── 12_ingestion_demo_ecommerce.ipynb
│   ├── 13_ingestion_demo_zip_enrichment.ipynb
│   ├── 14_TAR_prefilter.ipynb
│   └── 15_TAR_generation_and_scoring.ipynb
├── data/
│   ├── demo/                          ← synthetic demo CSVs (in git)
│   │   ├── globalcart_ecommerce.csv
│   │   └── cloudsync_saas.csv
│   ├── societal_processed/bta_cards/
│   │   ├── mk_bta_baseline.parquet    ← 7 BTA cards
│   │   └── mk_bta_rag_corpus.jsonl    ← RAG corpus for ChromaDB
│   ├── reference/
│   │   └── zcta_enrichment.parquet    ← ZIP → income/race lookup
│   ├── sessions/                      ← saved session JSON files
│   └── company_data/                  ← per-session ingestion outputs
├── store/
│   └── segment_store.py               ← ChromaDB segment store
├── tests/
│   ├── test_segment_store.py
│   └── test_data_ingestor.py          ← 170/170 passing
├── docs/
│   └── MK_Intel_Architecture_Decisions.md  ← internal, gitignored
├── ROADMAP.md
├── FASTApi_POAM.md
├── Procfile                           ← web + worker for Railway
├── railway.json
├── nixpacks.toml
├── runtime.txt                        ← python-3.11
├── README.md                          ← public, live on GitHub
└── .env                               ← ANTHROPIC_API_KEY, REDIS_URL, ADMIN_KEY (gitignored)
```

---

## Completed phases

### P0 — Societal baseline (NB01-11)
- ACS PUMS + GSS + Pew data pipeline
- K-Prototypes clustering → 7 BTAs
- BTA cards with structural + psych + media signals
- ChromaDB segment store
- Scoring algorithm (`mk_ta_scoring_algorithm.py`)
- TAR schema v2 (`mk_tar_schema_v2.json`)
- Session model (`mk_intel_session.py`)

### P1.2 — Business data ingestion (NB12-13)
- Canonical behavioral schema
- Multi-format readers, normalizer, coverage scoring
- `MKDataIngestor.ingest()` — single call, all steps automated
- ZIP enrichment integrated as Step 4.5
- Synthetic data generator (GlobalCart + CloudSync demos)
- Naming conventions: TA_XX → CS{cluster}_{bta}, TAAW → TAR

### P3/P4 — TAR generation + scoring (NB14-15)
- `mk_tar_prefilter.py` — profile refinement (A/B1/B2/C) + SOBJ rule-based pre-filter
- `mk_tar_generator.py` — 8-section sequential TAR generation, JSON repair, source tagging
- `tar_to_ta_input()` — adapter from TAR JSON to scoring algorithm input
- Full pipeline tested on GlobalCart and CloudSync sessions

### P2 — FastAPI backend ✅ COMPLETE
- B0: FastAPI + Celery + Redis skeleton, /health endpoint
- B1: Session model Pass 2 (TAAWS → TARS naming), job DB
- B2: Session endpoints (create, company, objective, SOBJs)
- B3: File upload + ingestion Celery task + ta-cards endpoint
- B4: Prefilter + TAR generation + scoring + rankings endpoints
- B4+: Executive summary JSON + HTML with LLM-generated audience_name
- B5: Demo auth (fingerprint + IP hash, recruiter codes, quota enforcement)
- B5+: Examples endpoints (pre-generated GlobalCart + CloudSync)
- B6: Railway deployment config (Procfile, nixpacks.toml, railway.json) — **deploy pending**

### GitHub ✅ LIVE
- Org: github.com/MarketKinetics
- Repo: github.com/MarketKinetics/mk-intel (public)
- Clean structure: backend/, mk-intel/, notebooks/ (all 15), store/, tests/
- README.md live and rendering
- Synthetic demo CSVs in data/demo/
- Internal docs gitignored (MK_Intel_Architecture_Decisions.md, scoring PDFs)

---

## Demo auth system (B5) — fully implemented

**Standard user (fingerprint + IP hash):**
- Zero friction — no email required
- 1 live pipeline run (full TAR generation)
- Pre-generated examples: unlimited viewing
- 30-day session expiry
- 200k token safety ceiling

**Recruiter code user:**
- Email + code required
- 2 live pipeline runs
- Same example access
- Codes generated via POST /admin/recruiter-code (ADMIN_KEY protected)

**Key endpoints:**
```
POST /demo/request          → fingerprint → issue token (or redeem recruiter code)
GET  /demo/status/{token}   → remaining quota
DELETE /demo/data/{token}   → user-requested data deletion
POST /admin/recruiter-code  → generate recruiter code (ADMIN_KEY required)
GET  /examples              → list pre-generated datasets
GET  /examples/{slug}       → example metadata + TARs + rankings
GET  /examples/{slug}/tars/{tar_id}              → full TAR JSON
GET  /examples/{slug}/tars/{tar_id}/summary.html → HTML executive summary
```

**Quota enforcement:**
- `X-Demo-Token` header sent with POST /sessions/{id}/generate
- 402 returned with BYOK message when quota exceeded
- `increment_demo_usage()` called by Celery task on completion

---

## All API endpoints

```
GET  /health
POST /sessions
GET  /sessions/{id}
DELETE /sessions/{id}
POST /sessions/{id}/company
POST /sessions/{id}/objective
POST /sessions/{id}/sobjs
PATCH /sessions/{id}/sobjs/{sobj_id}
GET  /sessions/{id}/sobjs
POST /sessions/{id}/ingest
GET  /sessions/{id}/jobs/{job_id}
GET  /sessions/{id}/jobs
GET  /sessions/{id}/ta-cards
POST /sessions/{id}/prefilter
GET  /sessions/{id}/candidates
POST /sessions/{id}/generate           ← X-Demo-Token header for quota check
GET  /sessions/{id}/tars
GET  /sessions/{id}/tars/{tar_id}
GET  /sessions/{id}/tars/{tar_id}/summary
GET  /sessions/{id}/tars/{tar_id}/summary.html
GET  /sessions/{id}/rankings
POST /demo/request
GET  /demo/status/{token}
DELETE /demo/data/{token}
POST /admin/recruiter-code
GET  /examples
GET  /examples/{slug}
GET  /examples/{slug}/tars/{tar_id}
GET  /examples/{slug}/tars/{tar_id}/summary
GET  /examples/{slug}/tars/{tar_id}/summary.html
```

---

## Remaining phases

### B6 — Railway deployment (NEXT)
- Go to railway.app → New Project → Deploy from GitHub → MarketKinetics/mk-intel
- Add Redis add-on (native Railway service, injects REDIS_URL automatically)
- Add persistent volume → mount at /data → set PROJECT_ROOT=/data in env vars
- Set env vars: ANTHROPIC_API_KEY, ADMIN_KEY, PROJECT_ROOT=/data
- Railway reads Procfile automatically (web + worker processes)
- Smoke test: /health → api=ok, redis=ok, worker=ok

**Critical Railway note:** API and Celery worker run in same service via Procfile.
This means they share the filesystem — uploaded files written by API are
immediately visible to the worker. No cross-service handoff problem.

**Data on Railway:** The BTA baseline parquet, ChromaDB, and reference data
need to be on the persistent volume. Options:
1. Include reference data in git (small files — zcta_enrichment.parquet ~5MB)
2. Run a one-time setup script on Railway to download/generate them
3. Include them in the Docker image

The `data/reference/zcta_enrichment.parquet` is already gitignore-exempted (!data/reference/).
The BTA baseline parquet and ChromaDB are NOT in git — need a strategy for Railway.

### P5 — React frontend
Key screens:
1. Landing page — example gallery + "Try with your own data" CTA + fingerprint auth
2. Session setup flow (company, OBJ, SOBJs)
3. File upload + ingestion progress polling
4. TA cards view
5. TAR ranked output view
6. TAR executive summary card (audience_name prominent, verdict badge, top 3 actions, key risk)
7. Full TAR detail view (all 8 sections, source tags visible)
8. Quota display + BYOK prompt when exhausted

### P6 — Additional demo datasets (3-5 total)
Currently: GlobalCart (e-commerce) + CloudSync (SaaS)
Need 1-3 more covering:
- Banking/financial services (compliance mode demo)
- Retail/local business (Case C custom archetype demo)
- Media/subscription (high churn, multiple SOBJs)

---

## Key architectural decisions

| Decision | Choice | Reason |
|---|---|---|
| Clustering | K-Prototypes | Mixed categorical + numeric |
| Targeting unit | Individual (not household) | B2C targeting |
| Income fields | individual income for matching, HH income as descriptor | ACS PUMS distinction |
| BTA confidence | A/B1/B2/C ZIP validation | Graduated confidence |
| Pre-filter | Rule-based + LLM fallback | Cost-efficient, data-agnostic |
| TAR generation | Sequential 8 LLM calls | Internal cross-referencing integrity |
| Source tagging | company_data / bta_baseline / zip_inference / llm_inference | Full auditability |
| Gate enforcement | Deterministic Python (rating > 2), not LLM-decided | Reliability |
| JSON repair | Backwards-scanning parser | Handles LLM truncation |
| Job state | App-owned SQLite jobs table | Clean UI status, not raw Celery state |
| API+worker | Single Railway service via Procfile | Shared filesystem, no handoff problem |
| Audience naming | LLM-generated human-friendly name per TAR | Better than archetype ID |
| Demo auth | Fingerprint + IP hash (no email) | Zero friction for standard users |
| Recruiter access | Email + code → higher quota token | Personal, trackable |
| Data retention | Raw CSVs retained privately, aggregates only for internal use | Legal + analytics balance |

---

## Compliance modes

| Mode | Excluded signals |
|---|---|
| standard | None |
| banking_us | age_bin, income_tier, zip signals |
| banking_eu | age_bin, income_tier, zip signals, marital_status |
| eu_gdpr | zip signals |
| All modes | race/eth never direct targeting, gender never filtering signal |

---

## Demo datasets

**GlobalCart** (NB12) — e-commerce
- CSV: `data/demo/globalcart_ecommerce.csv` (50,000 rows)
- Session slug: `globalcart_demo_20e909ae` (old TA_ naming) / `globalcart_demo_e94b73f7` (CS naming via API)
- 2 SOBJs: renew subscription, reactivate cancelled account
- All Case A (no ZIP codes)
- **NOTE:** Examples endpoint currently points to old session — needs re-run after Railway deploy

**CloudSync** (NB13) — SaaS
- CSV: `data/demo/cloudsync_saas.csv` (1,500 rows)
- Session slug: `cloudsync_demo_94e9a435`
- 2 SOBJs: reduce cancellations, upgrade plan tier
- Mix of ZIP enrichment cases (B1/B2/C)
- **NOTE:** TARs may not be generated yet for CloudSync — check before Railway deploy

---

## Running locally

**Start Redis:**
```bash
brew services start redis
```

**Start Celery worker (terminal 1):**
```bash
cd /Users/marcomagnolo/Projects/Market_Kinetics
source venv/bin/activate
PYTHONPATH=/Users/marcomagnolo/Projects/Market_Kinetics celery -A backend.celery_app worker --loglevel=info
```

**Start uvicorn (terminal 2):**
```bash
cd /Users/marcomagnolo/Projects/Market_Kinetics
source venv/bin/activate
PYTHONPATH=/Users/marcomagnolo/Projects/Market_Kinetics uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

**Test (terminal 3):**
```bash
curl http://localhost:8000/health
```

---

## Files to upload in new chat (in order)

1. `ROADMAP.md`
2. `FASTApi_POAM.md`
3. `backend/main.py`
4. `backend/config.py`
5. `backend/routers/pipeline.py`
6. `backend/routers/sessions.py`
7. `backend/routers/demo.py`
8. `backend/routers/examples.py`
9. `backend/tasks/pipeline.py`
10. `backend/db/jobs.py`
11. `backend/db/demo.py`
12. `mk-intel/mk_intel_session.py`
13. `mk-intel/mk_tar_prefilter.py`
14. `mk-intel/mk_tar_generator.py`
15. `mk-intel/mk_ta_scoring_algorithm.py`
16. `mk-intel/ingestion/mk_data_ingestor.py`

---

## Pending items (tracked)

1. **Railway deployment** — B6 is next. Critical issue: BTA baseline parquet and ChromaDB
   are not in git and need a strategy for Railway persistent volume.

2. **Examples endpoint re-run** — GlobalCart example currently points to old session
   with TA_ naming. Need to run fresh pipeline via API and update session slug in
   `backend/routers/examples.py`.

3. **CloudSync TARs** — verify whether CloudSync has generated TARs. If not, run
   the full pipeline on CloudSync before deploying.

4. **Hard gate logic** — SOBJ-specific gates in `mk_tar_prefilter.py` (e.g. reactivation
   only scores TAs with cancelled customers). Deferred post-NB15.

5. **CS naming re-run on notebooks** — NB12-15 still show old TA_ IDs in output cells.
   Re-run after Railway deploy when data paths are stable.

6. **`dominant_tenure` hallucination** — LLM sometimes outputs non-canonical tenure values.
   Pass allowed values list in prompt.

7. **Performance optimization** — parallel TAR section calls, streaming, Phase 7 post-MVP.

8. **P5 React frontend** — not started.

9. **Additional demo datasets** — 3 more needed (banking, retail, media).

10. **`_build_summary_prompt` refactor** — extracted as helper in pipeline.py but
    examples.py still has inline version. Consolidate.
