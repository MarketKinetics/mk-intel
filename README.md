# MK Intel

**AI-first Target Audience Analysis platform.**

MK Intel maps a company's customer data to behavioral archetypes derived from U.S. census and survey research, then generates ranked, evidence-based Target Audience Reports (TARs) — structured analytical documents that tell a campaign team *which* audience segments to target for a specific objective, and exactly *how* to reach them.

This is Module 1 of the [Market Kinetics](https://github.com/MarketKinetics) platform — a suite of AI-assisted tools for data-driven influence strategy.

---

## What it does

A company uploads their customer data. MK Intel:

1. **Maps customers to societal archetypes** — seven behavioral segments derived from ACS PUMS census microdata, GSS survey data, and Pew Research media behavior data. Each archetype carries structural demographics, psychological signals, and media behavior profiles grounded in nationally representative research.

2. **Enriches archetypes with company-specific signals** — behavioral data (LTV, churn risk, engagement, subscription status) is merged with the societal baseline to produce company-specific Target Audience (CS) profiles.

3. **Pre-filters candidates** — for each campaign objective, a rule-based engine scores each TA profile on behavioral plausibility. Only viable candidates proceed to full report generation.

4. **Generates structured Target Audience Reports** — each TAR is built in 8 sequential LLM calls covering effectiveness, behavioral conditions, psychological vulnerabilities, susceptibility, channel accessibility, persuasion narrative, measurement framework, and traceability. Every claim is source-tagged: `company_data`, `bta_baseline`, `zip_inference`, or `llm_inference`.

5. **Scores and ranks audiences** — A transparent 4-dimension scoring algorithm — effectiveness, susceptibility, vulnerability depth, and accessibility — produces a ranked priority list per campaign objective. Every score includes a full dimension breakdown so rankings are auditable and explainable.

6. **Delivers executive summaries** — on-demand HTML and JSON summaries with LLM-generated human-friendly audience names, verdict badges, top recommended actions, and key risks.

---

## Why it's different

**Grounded in real population data.** The societal baseline is built from 15.9M ACS PUMS individual records, GSS psychological trait projections, and Pew media behavior data — not invented personas.

**Transparent scoring.** Every ranked output includes full dimension breakdowns. No black-box scores. An analyst can trace every recommendation back to its evidence source.

**Compliance-aware.** Four compliance modes (standard, banking_us, banking_eu, eu_gdpr) gate which signals may be used as clustering inputs. Race/ethnicity is never a direct targeting criterion.

**Ethical guardrails built in.** Every TAR includes an ethics section specifying excluded tactics, privacy constraints, and fairness requirements — generated alongside the analytical content, not as an afterthought.

---

## Architecture

```
Societal baseline (built once, reused across all clients)
    ACS PUMS + GSS + Pew Research
        → K-Prototypes clustering
        → 7 Baseline Target Audiences (BTAs)
        → ChromaDB vector store

Per-session pipeline (runs per company)
    Upload CSV
        → Column mapping (rules + LLM fallback)
        → Normalization + coverage scoring
        → K-Means clustering
        → BTA structural matching
        → ZIP enrichment (Cases A / B1 / B2 / C)
        → Company Segment (CS) cards
            → TAR pre-filter (profile refinement + SOBJ scoring)
                → TAR generation (8 sequential LLM sections)
                    → Scoring algorithm
                        → Ranked output + executive summaries
```

**Stack:** Python · FastAPI · Celery · Redis · ChromaDB · SQLite · Anthropic API (Claude Haiku)

---

## The 7 societal archetypes

| ID | Name | U.S. Adult Population |
|---|---|---|
| BTA_00 | Diverse Mid-Life Workers | 17.2% |
| BTA_01 | Older Non-Partnered Adults | 16.2% |
| BTA_02 | Young Hispanic Working Adults | 5.4% |
| BTA_03 | Retired Renters | 12.5% |
| BTA_04 | Mid-Career Homeowners | 15.1% |
| BTA_05 | Young Non-Owning Singles | 14.4% |
| BTA_06 | Established Mid-Career Homeowners | 19.1% |

Derived from ACS PUMS microdata using K-Prototypes clustering. Psychological and media signals are projected from GSS and Pew NPORS respondents via demographic cell matching — not imputed at the individual level.

---

## TAR structure

Each Target Audience Report covers:

| Section | Content |
|---|---|
| Effectiveness | Can this audience accomplish the objective? Gate check (rating > 2 required). |
| Conditions | Why do they behave as they do today? External + internal conditions, consequences. |
| Vulnerabilities | Motives, psychographics, demographics, symbols and cues. |
| Susceptibility | Perceived risks and rewards, value alignment, recommended persuasion approach. |
| Accessibility | Channel-by-channel reach quality, restrictions, constraints. |
| Narrative & Actions | Main argument (IF/THEN), supporting arguments, recommended actions with timing and channel. |
| Assessment | Baseline behavior, target behavior, measurement metrics with success thresholds. |
| Traceability | Sources, assumptions, confidence level, ethical guardrails, privacy constraints. |

---

## API

The platform exposes a REST API built with FastAPI + Celery for background pipeline execution.

```
POST /sessions                          create session
POST /sessions/{id}/company             set company profile
POST /sessions/{id}/objective           set campaign objective
POST /sessions/{id}/sobjs               add supporting objective
POST /sessions/{id}/ingest              upload CSV, run ingestion pipeline
GET  /sessions/{id}/jobs/{job_id}       poll job status
GET  /sessions/{id}/ta-cards            list TA cards after ingestion
POST /sessions/{id}/prefilter           run TAR pre-filter
GET  /sessions/{id}/candidates          list TAR candidates
POST /sessions/{id}/generate            run TAR generation + scoring
GET  /sessions/{id}/tars                list generated TARs
GET  /sessions/{id}/tars/{tar_id}       full TAR JSON
GET  /sessions/{id}/tars/{tar_id}/summary        executive summary (JSON)
GET  /sessions/{id}/tars/{tar_id}/summary.html   executive summary (HTML)
GET  /sessions/{id}/rankings            scored ranked output

GET  /examples                          list pre-generated demo datasets
GET  /examples/{slug}/tars/{tar_id}/summary.html   live example summary
```

---

## Demo

Live demo available at **[link coming soon]**

Pre-generated examples included:
- **GlobalCart** — e-commerce subscription platform, 50K customers, renewal + reactivation objectives
- **CloudSync** — SaaS platform with ZIP enrichment, plan upgrade + cancellation reduction objectives

---

## Running locally

**Prerequisites:** Python 3.11+, Redis

```bash
git clone https://github.com/MarketKinetics/mk-intel.git
cd mk-intel

python -m venv venv
source venv/bin/activate
pip install -r backend/requirements.txt

cp .env.example .env
# Add your ANTHROPIC_API_KEY to .env
```

**Start Redis:**
```bash
brew install redis && brew services start redis  # macOS
```

**Start Celery worker (terminal 1):**
```bash
PYTHONPATH=. celery -A backend.celery_app worker --loglevel=info
```

**Start API (terminal 2):**
```bash
PYTHONPATH=. uvicorn backend.main:app --reload
```

**Health check:**
```bash
curl http://localhost:8000/health
```

---

## Project structure

```
mk-intel/
├── mk-intel/                  core pipeline scripts
│   ├── mk_intel_session.py    session model
│   ├── mk_tar_prefilter.py    profile refinement + pre-filter
│   ├── mk_tar_generator.py    TAR generation (8 sections)
│   ├── mk_ta_scoring_algorithm.py  scoring + ranking
│   └── ingestion/             data ingestion pipeline
├── backend/                   FastAPI + Celery backend
│   ├── routers/               API endpoints
│   ├── tasks/                 Celery background tasks
│   └── db/                    SQLite (jobs + demo auth)
├── notebooks/                 methodology demo notebooks
│   ├── 12_ingestion_demo_ecommerce.ipynb
│   ├── 13_ingestion_demo_zip_enrichment.ipynb
│   ├── 14_TAR_prefilter.ipynb
│   └── 15_TAR_generation_and_scoring.ipynb
└── docs/
    └── ARCHITECTURE.md        design decisions + methodology
```

---

## Methodology notebooks

The `notebooks/` directory contains end-to-end demos of each pipeline stage:

- **NB12** — E-commerce ingestion demo (GlobalCart, 50K customers)
- **NB13** — ZIP enrichment validation (CloudSync, Cases A/B1/B2/C)
- **NB14** — TAR pre-filter and profile refinement
- **NB15** — TAR generation and scoring (full pipeline)

---

## Part of Market Kinetics

MK Intel is the first module of the Market Kinetics platform:

| Module | Description | Status |
|---|---|---|
| **MK Intel** | Target audience analysis + report generation | ✅ Active |
| MK Campaign | Campaign execution and channel orchestration | Planned |
| MK Product | Communication product studio — input a product worksheet, output multimedia campaign drafts (copy, visuals, video scripts) | Planned |
| MK Engage | Meeting simulator — input personality traits and objectives for both parties, get an interactive testbed to rehearse and stress-test the encounter before it happens | Planned |

---

## License

MIT — see [LICENSE](LICENSE)
