# MK Intel — Platform Roadmap

This document captures architectural decisions made during the build,
deferred implementations with full specs, and the planned development
sequence. It serves as both a development guide and a living record
of design reasoning.

---

## Current Status


|---|---|---|
| P0 | Societal baseline pipeline (notebooks 01-11) | ✅ Complete |
| P0 | BTA cards + RAG corpus | ✅ Complete |
| P0 | Segment store (ChromaDB) | ✅ Complete |
| P0 | Session model | ✅ Complete |
| P0 | Scoring algorithm | ✅ Complete |
| P0 | TAR schema v2 | ✅ Complete |
| P1.1 | Segment store tests (32/32) | ✅ Complete |
| P1.2 | Canonical behavioral schema | ✅ Complete |
| P1.2 | Multi-format readers | ✅ Complete |
| P1.2 | Coverage scoring | ✅ Complete |
| P1.2 | Normalizer | ✅ Complete |
| P1.2 | Data ingestor | ✅ Complete |
| P1.2 | Synthe| Phase | Component | Status |tic data generator | ✅ Complete |
| P1.2 | Ingestion tests | ✅ Complete |
| P1.2 | Demo notebooks (12-13) | ✅ Complete |
| P1.2 | Change naming conventions (TAAW -> TAR) and TA_01 (these are the targets coming out from company data) to something less confusing like ComTA or clusters etc | ✅ Complete |
| P2 | FastAPI backend — B0 skeleton | ✅ Complete |
| P2 | FastAPI backend — B1/B2 session endpoints | ✅ Complete |
| P2 | FastAPI backend — B3 ingestion pipeline | ✅ Complete |
| P2 | FastAPI backend — B4 prefilter + TAR generation + scoring | ✅ Complete |
| P2 | FastAPI backend — TAR executive summary (JSON + HTML) | ✅ Complete |
| P2 | FastAPI backend — B5 demo auth + quota | ⏳ Pending |
| P2 | FastAPI backend — B6 Railway deployment | ⏳ Pending |
| P3 | TAR generation | ✅ Complete |
| P4 | Scoring + ranking | ✅ Complete |
| P5 | React frontend | ⏳ Pending |
| P5 | Frontend — session setup flow (company, OBJ, SOBJs) | ⏳ Pending |
| P5 | Frontend — file upload + ingestion progress | ⏳ Pending |
| P5 | Frontend — TA cards view | ⏳ Pending |
| P5 | Frontend — TAR ranked output view | ⏳ Pending |
| P5 | Frontend — TAR executive summary card (audience_name prominent, verdict badge, top actions, key risk) | ⏳ Pending |
| P5 | Frontend — full TAR detail view (all 8 sections, source tags visible) | ⏳ Pending |
| P6 | Demo auth + quota system | ⏳ Pending |
| P5 | Streamlit / React frontend | ⏳ Pending |
| P6 | Demo auth + quota system | ⏳ Pending |

---

## Deferred Implementations

### 1. API Key Management + Demo Access System

**Priority:** High — implement before public GitHub release

**Decision:** Hybrid four-mode policy

| Mode | Key Source | Quota | Use Case |
|---|---|---|---|
| developer | `.env` / env var | None | Local development |
| byok | User-provided key | None (they pay) | Technical evaluators |
| demo | Platform key (funded) | Hard quota | Recruiters, non-technical |
| blocked | None | 0 | Anonymous unlimited — rejected |

**Demo mode spec:**

Access control:
- Email magic link OR social login (GitHub OAuth preferred for developer audience)
- 1-2 full analysis runs per identity
- Hard token cap: 30,000 tokens per demo session
- Cooldown window: 7 days before same identity can request new demo session
- IP/device rate limiting: max 3 demo requests per IP per 24 hours
- Low concurrency: max 1 active demo session per identity at a time
- No expensive loops: SOBJ regeneration limited to 1 cycle in demo mode,
  no force_reload on segment store, no batch reprocessing

Recruiter code:
- Single-use codes generated manually for important applications
- Slightly higher quota: 3 full runs, 50,000 tokens
- Code delivered personally in application email / cover letter
- Stored in demo_sessions table with `access_type = "recruiter_code"`

**Infrastructure:**

Email: Resend (free tier sufficient for < 100 demo users/month)

Auth: GitHub OAuth (recommended) or email magic link
- GitHub OAuth: harder to fake, natural fit for developer audience
- Magic link: lower friction, works for non-GitHub users

Database: SQLite (zero infrastructure, sufficient for demo scale)

Schema:
```sql
CREATE TABLE demo_sessions (
    token           TEXT PRIMARY KEY,
    email           TEXT,
    github_id       TEXT,
    ip_address      TEXT,
    access_type     TEXT DEFAULT 'demo',  -- demo | recruiter_code
    created_at      TEXT,
    last_active_at  TEXT,
    expires_at      TEXT,
    runs_used       INTEGER DEFAULT 0,
    tokens_used     INTEGER DEFAULT 0,
    quota_runs      INTEGER DEFAULT 2,
    quota_tokens    INTEGER DEFAULT 30000,
    is_active       INTEGER DEFAULT 1
);

CREATE TABLE recruiter_codes (
    code            TEXT PRIMARY KEY,
    label           TEXT,           -- e.g. "companyXX_role_March2026"
    created_at      TEXT,
    used_at         TEXT,
    used_by_email   TEXT,
    quota_runs      INTEGER DEFAULT 3,
    quota_tokens    INTEGER DEFAULT 50000,
    is_used         INTEGER DEFAULT 0
);

CREATE TABLE ip_rate_limits (
    ip_address      TEXT PRIMARY KEY,
    request_count   INTEGER DEFAULT 0,
    window_start    TEXT
);
```

FastAPI endpoints:
```
POST /demo/request          — email/GitHub → creates pending session
GET  /demo/verify/{token}   — magic link click → activates session
POST /demo/validate          — checks quota before each API call
GET  /demo/status/{token}   — returns remaining quota
POST /admin/recruiter-code   — generates a recruiter code (auth required)
```

**Where quota enforcement happens:**

In `utils.py` → `_get_client(session)`. Before returning the client,
if `session.session_mode == "demo"`, the function calls the quota
validator. If quota is exhausted, raises `DemoQuotaExceededError`
with a clear user-facing message directing them to BYOK.

**Implementation estimate:** 1 focused day when platform is ready for demo.

---

### 2. Column Name Mapping — LLM Layer

**Priority:** Medium — required for normalizer completion

**Decision:** Three-layer approach (Rules → LLM → Analyst)

Layer 1 — Rules (rapidfuzz fuzzy matching + synonym dict):
- Handles obvious matches: "Age", "age_years", "customer_age" → "age"
- Runs first, zero cost, instant
- Covers ~70-80% of real-world column names

Layer 2 — LLM inference (Claude, structured output):
- Runs only on columns not matched by rules
- Sends: unmatched column names + sample of 5 values each
- Returns: structured mapping dict with confidence scores
- Model: claude-haiku (cheapest, sufficient for this task)
- Estimated cost: < $0.01 per new company ingestion

Layer 3 — Analyst review:
- Full proposed mapping displayed (rules + LLM) with confidence scores
- Analyst confirms, corrects, or adds manual mappings
- Approved mapping saved to:
  `data/company_data/{company_slug}/column_mapping.json`
- Never runs again for this company after first approval

**LLM prompt template:** (to be written in normalizer.py)

**Implementation:** normalizer.py — pending

---

### 2b. Data Upload Flow — Required Fields

**Priority:** High — implement before public GitHub release

**Decision:** The data upload screen must collect the following fields
from the analyst before the pipeline runs:

| Field | Type | Default | Notes |
|---|---|---|---|
| `dataset_export_date` | date | today | Reference date for duration-to-date conversions (e.g. Membership_Years → customer_since). Critical for accuracy — a dataset exported 6 months ago produces wrong dates if today is used as reference. |
| `company_name` | string | required | Used to generate company slug and session directory |
| `sector` | enum | None | standard / banking / ecommerce — controls compliance mode and sector-specific field handling |
| `compliance_mode` | enum | standard | standard / banking_us / banking_eu / eu_gdpr |
| `dataset_description` | string | optional | Free text context about the dataset — passed to LLM column mapping for better inference |

**Implementation note:** `dataset_export_date` must be surfaced prominently
in the upload UI — not buried in advanced settings. A wrong reference date
silently corrupts all duration-derived dates throughout the pipeline.

---
### 2c. Data Readiness Engine — Value Vocabulary Check

**Priority:** High — implement before public demo

After column mapping is approved, run a value profile check on each
mapped field. Compare actual values against canonical vocabulary.
Flag VALUE_VOCABULARY_MISMATCH when match rate < threshold.
Present mapping options (rule-based for known patterns, LLM for ambiguous).

Known rule-based patterns:
- Binary 0/1 on status/boolean fields
- Y/N or Yes/No on boolean fields  
- Single-char codes (M/F, A/C) on categorical fields

LLM invoked for non-standard or business-specific values.

---

### 3. Text Signals — LLM Extraction Pipeline

**Priority:** Low — Phase 1.2b, optional

**Decision:** Schema placeholder in place. Implement only if social media
ingestion pipeline is built.

If implemented:
- Input: raw text (reviews, support tickets, social posts)
- Processing: LLM batch extraction → structured signal dict
- Output: populates text_signals domain in canonical schema
- Configurable batch size and cost guardrail
- Source types: review, support_ticket, social_post, survey_response

**Implementation estimate:** 2-3 days when/if decided.

---

### 4. Usable Coverage Metric

**Priority:** Low — Phase 2 cleanup

**Decision:** Current coverage.py computes data coverage (what is present).
A second metric — usable coverage (what is present AND legally usable
for clustering in the active compliance mode) — should be added later.

Formula:
```
usable_coverage = coverage computed only over fields not in
                  compliance_excluded_fields for active mode
```

This is especially important for banking_us and banking_eu modes where
several high-weight fields are excluded.

**Implementation estimate:** 2 hours in coverage.py.

---

### 5. Non-US BTA Expansion

**Priority:** Low — future product milestone

**Decision:** Current BTAs are US-only (derived from ACS PUMS).
Non-US customers skip BTA mapping entirely.

Future: build equivalent baseline segmentation for EU markets using
Eurostat microdata + ESS (European Social Survey) for psychological layer.
This would unlock the banking_eu compliance mode fully.

**Implementation estimate:** Full pipeline rebuild (notebooks 01-11
equivalent) for EU data. Significant effort — plan as a separate project.

---

## API Call Inventory

Every LLM call in the platform. All must go through `_get_client(session)`.

| Step | Phase | Model | Approx tokens | Notes |
|---|---|---|---|---|
| Column mapping inference | P1.2 | haiku | ~2,000 | Per new company only |
| Company intelligence summary | P1.1 | sonnet | ~3,000 | Per session |
| OBJ validation | P1.1 | haiku | ~500 | Per session |
| SOBJ generation | P1.1 | sonnet | ~2,000 | Per session, up to 3 cycles |
| LLM pre-filter per SOBJ | P2 | sonnet | ~3,000 | Per SOBJ |
| TAR generation | P3 | sonnet | ~5,000 | Per (segment, SOBJ) pair |
| BTA card enrichment | P1.2 | sonnet | ~2,000 | Per BTA updated |
| Narrative regeneration | P1.2 | sonnet | ~1,500 | Per BTA updated |

Estimated tokens per full analysis run (2 SOBJs, 3 BTAs each):
~40,000-50,000 tokens. At claude-sonnet-4-6 pricing this is approximately
$0.15-0.20 per full run.

Demo quota of 30,000 tokens covers approximately 1 full run comfortably.

---

## Architecture Decisions Log

| Decision | Choice | Rationale |
|---|---|---|
| Clustering algorithm | K-Prototypes | Mixed categorical + numeric, best balance |
| BTA count | 7 | Adult-only population, k=7 optimal imbalance ratio |
| Vector DB | ChromaDB + all-MiniLM-L6-v2 | Local, zero API cost, sufficient for 7 docs |
| Session storage | JSON files | Simple, auditable, portable |
| BTA naming | BTA_00 to BTA_06 | Civilian-friendly, avoids PSYOP doctrine |
| Canonical schema | JSON | Human-readable, version-controlled |
| Compliance modes | 4 modes | standard, banking_us, banking_eu, eu_gdpr |
| Structural mapping | Hybrid (rules + embedding) | Transparent + behavioral refinement |
| Demo auth | Email magic link + recruiter codes | Automatic, no manual approval needed |
| API key policy | BYOK default + funded demo | Protects platform owner, enables demos |
| Path resolution | Path().resolve().parent | No hardcoded absolute paths anywhere |
| Segment store reload | force_reload on enrichment | BTAs never overwritten, TAs are session-scoped |
| Behavioral-only mode trigger | bta_eligible_count == 0 | Binary decision at session level — no hybrid output, avoids false score equivalence |
| Behavioral Tier 1 features | SOBJ-dynamic via SOBJ_SIGNAL_MAP | Reuses existing signal map, no new infrastructure, objective-aware |

### 6. Clustering Results Screen — Frontend Requirements

**Priority:** High — core transparency feature, implement in P5

**The clustering results screen must surface the full decision trail
to the analyst. MK Intel's transparency layer is a key differentiator
over black-box clustering tools.**

**Section 1 — Feature selection rationale**
Table showing every field considered for clustering with decision,
gate, and reason:

| Field | Decision | Gate | Reason |
|---|---|---|---|
| sessions_last_30d | ✓ Included | — | Core behavioral signal |
| churn_risk_score | ✗ Excluded | Gate 2 | Outcome-adjacent: churn OBJ detected |
| subscription_status | ✗ Excluded | Gate 1 | Outcome label |

Data source: `cluster_stats.json` → `excluded_features` dict.

**Section 2 — k selection chart**
Interactive silhouette + inertia chart across tested k values.
Plain-language caption: "The platform tested k=2 through k=8.
k=2 produced the most cohesive clusters (silhouette=0.378)."
Data source: `cluster_stats.json` → `silhouette_scores`, `inertias`.

**Section 3 — Cluster profile cards**
One card per cluster showing:
- Size (n and % of total)
- Median values per behavioral feature
- Post-hoc field distributions (churn rate, subscription status)
- LLM-generated plain-language archetype name and one-line description

**Section 4 — Post-hoc labels**
Explicitly labeled section: "The following fields were excluded from
clustering but used to characterize the resulting segments."
Shows distribution of each post-hoc field per cluster.

**Section 5 — Override panel (collapsible)**
Allows analyst to force-include a Gate 2 excluded field with a
documented reason. Re-runs clustering with override applied.
Shows side-by-side comparison of original vs override clustering.
Override and reason logged to `cluster_stats.json` as auditable record.

**Data sources:**
- `clustering/cluster_stats.json` — k, silhouette, excluded features
- `clustering/cluster_profiles.parquet` — dominant profiles per cluster
- `clustering/cluster_assignments.parquet` — per-customer assignments
- `normalized/normalized_records.parquet` — post-hoc field values


### TO TRACK:
if MK Intel expands to domains where behavioral data is predominantly categorical (e.g. survey response data, CRM tag data), revisit K-Prototypes or UMAP+HDBSCAN at that point.

---

### 7. Behavioral-Only Analysis Mode

**Priority:** High — required for production readiness

**Problem:**
The current pipeline requires `age_bin` or `income_tier` for BTA matching (`structural_weight_coverage >= 0.35`). Many real B2C datasets — e-commerce platforms, mobile apps, loyalty programs — do not collect age or income at signup. Without these fields, 0 records are `bta_eligible` and the pipeline produces no output.

**Decision: Binary mode detection, no hybrid output**

A dataset either has demographic signals or it doesn't. Running a hybrid pipeline that mixes BTA-grounded and behavioral-only TARs in the same ranked list creates a false equivalence — the two types are not scored on the same evidence basis and cannot be meaningfully compared.

The mode is determined once, at the session level, after ingestion:
- `bta_eligible_count == 0` → **behavioral-only mode** for entire session
- `bta_eligible_count > 0` → **standard BTA mode** (existing pipeline)

The mode is stored on the session object as `analysis_mode: "bta" | "behavioral"`.

**Trigger condition:**
Detected at end of ingestion step in `mk_data_ingestor.py`, after coverage scoring. If all records have `bta_eligible = False`, session is flagged as behavioral mode before BTA matching runs. BTA matching step is skipped entirely.

**Minimum viable data requirement:**
At least 2 fields from the SOBJ-matched signal set must be present in the dataset. If not met, pipeline surfaces a clear error: "Insufficient behavioral signals to run analysis for this objective. Please verify your column mapping or choose a different objective."

**Feature selection — SOBJ-dynamic Tier 1:**
Rather than a static field ranking, Tier 1 features are dynamically inferred from the SOBJ statement using the existing `SOBJ_SIGNAL_MAP` in `mk_tar_prefilter.py`. Fields matched by the SOBJ keyword rules become Tier 1 for that session. All other behavioral fields become Tier 2/3.

Examples:
- SOBJ contains "churn" → `churn_risk_score`, `subscription_status`, `ltv`, `mrr` are Tier 1
- SOBJ contains "upgrade" → `nps_score`, `feature_adoption_count`, `ltv` are Tier 1
- SOBJ contains "email" → `email_open_rate`, `email_click_rate` are Tier 1

This reuses existing logic with no new mapping infrastructure.

**Clustering in behavioral mode:**
Standard K-Means/K-Prototypes on available behavioral features. No BTA matching. No ZIP enrichment. All clusters receive a `confidence_case: "BEH"` flag.

**Psychographic and media layer:**
In BTA mode, the psychographic and media layer comes from the census-derived BTA card. In behavioral mode, there is no BTA card. The LLM generates a custom archetype from behavioral signals only — same mechanism as Case C (`_generate_name_for_case_c` in `mk_tar_prefilter.py`), extended to cover all clusters.

TAR content will be sourced entirely from `company_data` and `llm_inference`. No `bta_baseline` claims will appear. The traceability section must explicitly note the absence of the population baseline layer.

**Confidence case:**
New confidence case `"BEH"` (behavioral-only) added alongside existing A / B1 / B2 / C cases.

Internal value: `"BEH"`
UI label: `"Behavioral profile"`
UI description: `"This audience was identified from your customer data patterns. Population-level demographic baseline not available for this dataset."`
Badge color: distinct from A/B/C cases — use neutral/blue rather than green/amber/red.

**TAR generation:**
Unchanged. The TAR generator receives the same candidate structure regardless of mode. The behavioral archetype profile replaces the BTA-enriched profile as the context document. All 8 TAR sections generate normally. Source tags will reflect `company_data` and `llm_inference` dominance.

**Prefilter:**
Unchanged. The prefilter already scores on behavioral signals regardless of BTA mode. The SOBJ signal rules work identically.

**Scoring algorithm:**
Unchanged. Scoring operates on TAR section outputs, not on the upstream profile type.

**Frontend changes required:**
- `SessionDetail.jsx` — show a notice banner when `analysis_mode == "behavioral"`: "This analysis ran in behavioral profile mode — demographic baseline not available for this dataset."
- TAR cards — show "Behavioral profile" badge instead of confidence case badge (A / B1 / B2 / C)
- Processing page — add a stage indicator note when behavioral mode is detected post-ingestion

**Files to modify:**
- `mk_data_ingestor.py` — detect mode after coverage scoring, set `session.analysis_mode`
- `mk_intel_session.py` — add `analysis_mode` field to session model
- `mk_tar_prefilter.py` — extend `_generate_name_for_case_c` to handle all behavioral clusters; set `confidence_case = "BEH"`
- `mk_tar_generator.py` — handle `"BEH"` confidence case in `_build_profile_context`; suppress BTA-specific context fields gracefully
- `coverage.py` — no changes needed; `bta_eligible` flag already computed correctly
- `backend/routers/pipeline.py` — expose `analysis_mode` in session response
- `backend/routers/sessions.py` — include `analysis_mode` in session metadata
- `mk-intel-frontend/src/pages/SessionDetail.jsx` — behavioral mode banner + badge
- `mk-intel-frontend/src/pages/Processing.jsx` — mode detection notice

**Implementation estimate:** 2-3 focused days.

**Known limitation:**
Behavioral-only TARs will be less rich than BTA-grounded TARs. The psychographic layer, media behavior signals, and trust cues will be LLM-inferred rather than census-grounded. Analysts should treat behavioral-only output as directional intelligence, not population-validated insight. This limitation is surfaced explicitly in the TAR traceability section.

**Test datasets:**
- IBM Telco Customer Churn (Kaggle) — no age/income, 7,043 customers ✓ triggers behavioral mode
- ShopFlow / E-Commerce Customer Churn (Kaggle, Ankit Verma) — no age/income, 5,630 customers ✓ triggers behavioral mode

---


## Phase 7 — Performance and UX Optimization

**Priority: High**

### P7.1 — Pipeline latency reduction

**Quick wins (post-MVP, low effort):**
- Parallel LLM calls for independent TAR sections — accessibility and
  traceability do not depend on narrative and can run concurrently.
  Target: reduce per-TAR generation time by ~25%.
- Refined profile caching — hash the TA card + company context and skip
  LLM refinement on re-runs where inputs haven't changed.
- Batch profile refinement — current implementation makes one Haiku call
  per TA card. A single call with all TA cards in one prompt would reduce
  latency and cost for large ingestion runs.

**Larger architectural moves (pre-production):**
- Streaming LLM responses — pipe section output to the UI as it generates
  rather than waiting for the full response. Requires async architecture.
- Background job queue — decouple UX from generation latency. User submits
  a pipeline run, gets notified (email/webhook) when TARs are ready.
  Eliminates the "waiting for LLM" problem entirely from the user's perspective.
- Model tiering — use Sonnet for the first TAR per session (highest quality,
  sets the benchmark) and Haiku for remaining TARs. Reduces cost and latency
  for sessions with many candidates without sacrificing output quality on the
  highest-priority TAR.

### P7.2 — Demo UX

- Section-level progress indicator during TAR generation:
  "Analyzing effectiveness... conditions... vulnerabilities..."
  A 2-minute wait with visible progress feels fast.
  A 2-minute spinner feels broken.
- Estimated time remaining display based on candidate count and
  average section generation time from prior runs.
- Incremental TAR display — show each completed TAR as it finishes
  rather than waiting for the full batch.

  ---
  ### Data retention and privacy

- Raw CSVs and derived outputs retained privately per session for
  platform refinement. Never shared or sold.
- Upload notice shown at ingestion time.
- Deletion endpoint available on request.
- If platform scales beyond demo: implement formal DPA template,
  GDPR subject access request flow, and data retention schedule.
---

### to add in the README or document somewhere
MK Intel in its current form is strictly B2C or prosumer — individuals as customers. B2B would require a completely different societal baseline built from business registries, industry data, company size distributions etc. That's a future product scope.

MK Intel requires US-based customer data for BTA-grounded analysis. Non-US datasets or datasets without age/income fields will run in behavioral-only mode automatically.

## Test datasets for platform validation
The following public datasets are recommended for testing MK Intel:
- **IBM Telco Customer Churn** — https://www.kaggle.com/datasets/blastchar/telco-customer-churn — B2C Telco, 7,043 customers, has ZIP codes (US), triggers standard BTA mode
- **E-Commerce Customer Churn** (Ankit Verma) — https://www.kaggle.com/datasets/ankitverma2010/ecommerce-customer-churn-analysis-and-prediction — B2C e-commerce, 5,630 customers, no age/income, triggers behavioral-only mode
- **GlobalCart** (synthetic, included) — E-commerce subscription, 50K customers, full demographic signals, standard BTA mode
- **CloudSync** (synthetic, included) — B2B SaaS proxy, 1,500 customers with ZIP enrichment, standard BTA mode

