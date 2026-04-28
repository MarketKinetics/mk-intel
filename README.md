# MK Intel

MK Intel is a target audience analysis system that uses LLM reasoning as a load-bearing component, under structural guardrails that prevent AI-induced inaccuracy. The system maps a company's customer data to behavioral archetypes derived from U.S. census and survey research, then generates ranked, evidence-based Target Audience Reports (TARs). LLMs are used where they excel — column-name interpretation, audience profile refinement, narrative generation, audience naming. Real data, deterministic rules, and explicit gates handle everything else. Every claim in every output is tagged by source, so a reader can always see what the LLM said versus what the data said.

This is Module 1 of the [Market Kinetics](https://github.com/MarketKinetics) platform — a suite of tools for audience research and campaign analytics.

**Live demo:** [mk-intel-delta.vercel.app](https://mk-intel-delta.vercel.app)

---

## Status

Active development. The platform runs end-to-end against real data and exposes a live demo with two pre-generated example datasets. Methodology decisions are documented in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

The four-dimensional scoring weights (effectiveness 30%, susceptibility 30%, lever depth 25%, accessibility 15%) reflect deliberate prior beliefs about how these factors trade off — effectiveness and susceptibility weighted highest because failure on either undermines everything else, lever depth next because more available persuasion levers means more campaign flexibility, accessibility lowest because most channels are workable for most audiences. These priors have not yet been empirically calibrated against labeled campaign outcomes, and that calibration is the next major methodological step before any production use; the architecture is built to make recalibration tractable, with dimension breakdowns preserved in every output and the deterministic scoring step decoupled from LLM-driven content generation.

---

## Demo

The fastest way to understand the system is to browse pre-generated TARs in the live demo:

**[mk-intel-delta.vercel.app](https://mk-intel-delta.vercel.app)**

Two example datasets are included:

- **GlobalCart** — e-commerce subscription platform, 50K customers, renewal + reactivation objectives. No ZIP enrichment (illustrates the structural-only matching path).
- **CloudSync** — consumer SaaS platform with ZIP enrichment enabled, plan upgrade + cancellation reduction objectives. Illustrates the four ZIP confidence cases (A / B1 / B2 / C).

Each example walks through the full pipeline: column mapping, BTA archetype matching, ZIP confidence validation (where applicable), TAR pre-filter and profile refinement, full TAR generation, scoring, and ranked output. The demo also exposes the executive summary view used by analysts in production.

---

## What it does

A company uploads their customer data. MK Intel:

1. **Maps customers to societal archetypes** — seven behavioral segments derived from ACS PUMS census microdata, GSS survey data, and Pew Research media behavior data. Each archetype carries structural demographics, psychological signals, and media behavior profiles grounded in nationally representative research.

2. **Enriches archetypes with company-specific signals** — behavioral data (LTV, churn risk, engagement, subscription status, donation history, attendance, and similar) is merged with the societal baseline to produce company-specific Target Audience (CS) profiles.

3. **Pre-filters candidates** — for each campaign objective, a rule-based engine scores each TA profile on behavioral plausibility. Only viable candidates proceed to full report generation, avoiding expensive LLM calls on implausible combinations.

4. **Generates structured Target Audience Reports** — each TAR is built in 8 sequential LLM calls covering effectiveness, behavioral conditions, persuasion levers, susceptibility, channel accessibility, persuasion narrative, measurement framework, and traceability. Every claim is source-tagged: `company_data`, `bta_baseline`, `zip_inference`, or `llm_inference`.

5. **Scores and ranks audiences** — a transparent four-dimension scoring algorithm produces a ranked priority list per campaign objective. Every score includes a full dimension breakdown so rankings are auditable and explainable.

6. **Delivers executive summaries** — on-demand HTML and JSON summaries with human-friendly audience names, verdict badges, top recommended actions, and key risks.

---

## Why it's different

**AI under guardrails, not AI-everywhere.** The system uses LLMs as the load-bearing reasoning layer for tasks where they outperform rules — interpreting non-canonical column names, refining audience profiles to specific company contexts, generating narrative arguments, naming audiences. It does *not* use LLMs to match real data, enforce thresholds, score audiences, or decide which audiences are recommended. The effectiveness gate is enforced in Python after parsing, never inside the LLM call. Structural fields (age, income, tenure, education) are locked from real data before any LLM prompt is constructed — the LLM cannot override them. Every claim in every TAR is source-tagged (`company_data` / `bta_baseline` / `zip_inference` / `llm_inference`) so the evidential basis of every statement is visible to the analyst. The result is AI-enriched output that an analyst can audit and act on, not AI-generated content that has to be fact-checked.

**Grounded in real population data.** The societal baseline is built from approximately 15.9M ACS PUMS individual records, with psychological traits projected from GSS respondents and media behavior from Pew NPORS — all using demographic cell matching with hierarchical fallback. No invented personas.

**Projection over imputation.** Psychological signals on each archetype are population-level inferences — probability distributions over demographically similar respondents — not point estimates assigned to individuals. The distinction matters for both honesty and accuracy.

**Transparent scoring.** Every ranked output includes its full dimension breakdown. No black-box scores. An analyst can trace every recommendation back to its evidence source via the source-tagging discipline applied to every claim.

**Compliance-aware.** Four compliance modes (`standard`, `banking_us`, `banking_eu`, `eu_gdpr`) gate which signals may be used as clustering inputs. Race and ethnicity are never used as direct targeting criteria — they appear only as population descriptors derived from census data, never inferred for individuals.

**Effectiveness gate as a feature.** Audiences that fail the effectiveness gate (rating ≤ 2) are not silently down-ranked or hidden. They appear in output with an explicit "Not recommended" verdict and a stated disqualification reason. This is deliberately preferable to soft scoring because it forces explicit reasoning about why an audience does not fit, rather than burying the judgment in a number.

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

**Stack:** Python · FastAPI · Celery · Redis · ChromaDB · SQLite · React (Vite) · Anthropic API (Claude Haiku)

For full design rationale and decision history, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

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

Derived from ACS PUMS microdata using K-Prototypes clustering on individual-level features. Psychological and media signals are projected from GSS and Pew NPORS respondents via demographic cell matching with hierarchical fallback — not imputed at the individual level. Survey weights (`PWGTP`) are applied throughout.

---

## TAR structure

Each Target Audience Report covers:

| Section | Content |
|---|---|
| Effectiveness | Can this audience accomplish the objective? Gate check (rating > 2 required for recommendation). |
| Conditions | Why do they behave as they do today? External and internal conditions, consequences. |
| Persuasion levers | Motives, psychographics, demographics, symbols and cues that create receptivity to the campaign. |
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

## Running locally

**Prerequisites:** Python 3.11+, Redis, Node.js 18+ (for frontend).

```bash
git clone https://github.com/MarketKinetics/mk-intel.git
cd mk-intel

python -m venv venv
source venv/bin/activate
pip install -r backend/requirements.txt

cp .env.example .env
```

Add your `ANTHROPIC_API_KEY` to `.env`.

**Start Redis:**

```bash
brew install redis && brew services start redis
```

**Start Celery worker (terminal 1):**

```bash
PYTHONPATH=. celery -A backend.celery_app worker --loglevel=info
```

**Start API (terminal 2):**

```bash
PYTHONPATH=. uvicorn backend.main:app --reload
```

**Start frontend (terminal 3):**

```bash
cd mk-intel-frontend
npm install
npm run dev
```

**Health check:**

```bash
curl http://localhost:8000/health
```

---

## Project structure

```
mk-intel/                       (repo root)
├── mk_intel_session.py         session model
├── mk_tar_prefilter.py         profile refinement + pre-filter
├── mk_tar_generator.py         TAR generation (8 sequential sections)
├── mk_ta_scoring_algorithm.py  scoring + ranking
├── ingestion/                  data ingestion pipeline
├── backend/                    FastAPI + Celery backend
│   ├── routers/                API endpoints
│   ├── tasks/                  Celery background tasks
│   └── db/                     SQLite (jobs + demo auth)
├── mk-intel-frontend/          React (Vite) frontend
├── notebooks/                  methodology demo notebooks
│   ├── 12_ingestion_demo_ecommerce.ipynb
│   ├── 13_ingestion_demo_zip_enrichment.ipynb
│   ├── 14_TAR_prefilter.ipynb
│   └── 15_TAR_generation_and_scoring.ipynb
└── docs/
    └── ARCHITECTURE.md         design decisions + methodology
```

---

## Methodology notebooks

The `notebooks/` directory contains end-to-end demos of each pipeline stage:

- **NB12** — E-commerce ingestion demo (GlobalCart, 50K customers)
- **NB13** — ZIP enrichment validation (CloudSync, Cases A / B1 / B2 / C)
- **NB14** — TAR pre-filter and profile refinement
- **NB15** — TAR generation and scoring (full pipeline)

These are intended as walkthroughs of the methodology, not as data analysis artifacts — they explain *why* each stage exists and *how* it makes its decisions.

---

## Part of Market Kinetics

MK Intel is the first module of the Market Kinetics platform:

| Module | Description | Status |
|---|---|---|
| **MK Intel** | Target audience analysis and report generation | Active |
| MK Campaign | Campaign execution and channel orchestration | Planned |
| MK Product | Campaign draft generator — produces coordinated copy, visuals, and video scripts grounded in the audience archetypes from MK Intel | Planned |
| MK Engage | Stakeholder meeting simulator — rehearse high-stakes conversations against simulated counterparts informed by audience archetypes | Planned |

---

## License

MIT — see [LICENSE](LICENSE).
