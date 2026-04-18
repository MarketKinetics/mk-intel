# MK Intel — Project Handoff Document v5
## Updated: April 18, 2026

---

## What is MK Intel

AI-first Target Audience Analysis platform built as a portfolio piece for AI engineering roles. Takes a company's customer data, maps customers to societal archetypes derived from U.S. census data, and produces ranked, evidence-based Target Audience Reports (TARs).

**Stack:** Python, FastAPI, Celery, Redis, ChromaDB, SQLite, Anthropic API (Claude Haiku)
**Local root:** `/Users/marcomagnolo/Projects/Market_Kinetics/`
**GitHub:** github.com/MarketKinetics/mk-intel (public)
**Railway (live backend):** https://web-production-7ec13.up.railway.app
**Frontend (local dev):** http://localhost:5173
**Frontend repo:** `/Users/marcomagnolo/Projects/Market_Kinetics/mk-intel-frontend/`
**Admin key:** `Fibonacci12358!`

---

## Architecture overview

```
Societal baseline (built once, reused)
    ACS PUMS + GSS + Pew → K-Prototypes clustering → 7 BTAs → ChromaDB

Per-company pipeline (runs per session)
    Upload CSV → normalize → cluster → BTA match → ZIP enrich → TA cards
         ↓
    TAR pre-filter (profile refinement A/B1/B2/BEH + SOBJ scoring)
         ↓
    TAR generation (8 sequential LLM sections per candidate)
         ↓
    Scoring algorithm (4 dimensions → ranked output)
         ↓
    audience_name (LLM-generated, persisted to TAR JSON via summary endpoint)
```

### Analysis modes
- **BTA mode** — default. Customer records matched to census archetypes (BTA_00–BTA_06). Requires structural fields (age, income etc). Produces census-grounded TARs.
- **Behavioral mode** — triggered when `bta_eligible_count == 0` after ingestion. No census baseline. Clusters on behavioral signals only. TARs are directional/exploratory, not population-validated. Indicated by `session.analysis_mode = "behavioral"`.

---

## Backend status — COMPLETE AND LIVE

All backend endpoints are live on Railway. Key facts:

- **Session slugs for examples:**
  - GlobalCart: `afd8c333` (session `afd8c333-35cf-4c3b-ae88-68812b4d6366`)
  - CloudSync: `fcb77ed8` (session `fcb77ed8-f892-41a0-9006-55dffdfcd640`)

- **CORS:** Enabled in `backend/main.py` with `allow_origins=["*"]`

- **audience_name persistence:** When `/sessions/{id}/tars/{tar_id}/summary` is called, the LLM-generated `audience_name` is saved back to the TAR JSON file.

- **Examples endpoint:** `backend/routers/examples.py` — `_get_example_dir()` searches by session_id prefix as fallback.

- **Demo auth:** Fingerprint + IP hashing, quota enforcement at `POST /sessions/generate` via `X-Demo-Token` header. 2 runs default quota, 3 for recruiter codes.

- **BYOK:** `X-Anthropic-Key` header accepted at `POST /sessions/generate`. If present, skips quota check entirely.

- **Admin endpoints:** `backend/routers/admin.py` — session list + ZIP export. Admin key: `Fibonacci12358!`

---

## Key file locations

```
# Project root
mk_intel_session.py               — session model, analysis_mode field ("bta"|"behavioral")
mk_tar_prefilter.py               — profile refinement Cases A/B1/B2/BEH, company_specific_name
mk_tar_generator.py               — TAR generation, temperature=0 on effectiveness gate
mk_tar_prefilter.py               — BEH case, _generate_profile_for_beh, naming fixes

# Ingestion
ingestion/mk_data_ingestor.py     — full pipeline: normalize, cluster, BTA match, behavioral mode
ingestion/normalizer.py           — 143 synonyms, FIELD_TYPES, auto-rescaling
ingestion/readers.py              — multi-format file reader (csv/xlsx/json/parquet etc)

# Backend
backend/routers/sessions.py       — session endpoints, analysis_mode in response, column mapping
backend/routers/pipeline.py       — ingest/prefilter/generate endpoints
backend/routers/admin.py          — session list + ZIP export
backend/tasks/pipeline.py         — Celery tasks: run_ingestion, run_prefilter, run_tar_generation
backend/main.py                   — FastAPI app, routers registered

# Frontend
mk-intel-frontend/src/
  pages/SessionDetail.jsx         — behavioral mode banner + badge, download button
  pages/Processing.jsx            — mapping review pause, startPrefilter
  pages/Setup.jsx                 — multi-format upload, BYOK field
  components/MappingReview.jsx    — type hints, confirmed tab hidden, context hints
  api/client.js                   — all API endpoints including getColumnMapping, updateColumnMapping
```

---

## Completed this session (April 18, 2026)

### 1. Re-normalization on mapping amendment — FIXED
**File:** `backend/tasks/pipeline.py`

The pipeline normalized data during ingestion before the user reviewed the mapping. User amendments to `column_mapping.json` were saved correctly but never applied to the normalized data.

**Fix:** `run_prefilter` now checks if `column_mapping.json` has `user_amended: true`. If yes, re-runs full normalization pipeline (load_and_normalize → compute_coverage → re-detect analysis_mode → cluster → build_ta_cards) before proceeding to prefilter.

**Critical detail:** `analysis_mode` is re-detected after re-normalization by checking `bta_eligible_count` from the fresh normalized data. This prevents the stale session JSON value ("bta") from incorrectly routing a behavioral dataset through the BTA pipeline.

### 2. Synonym dictionary expanded — 143 synonyms
**File:** `ingestion/normalizer.py`

Key additions:
- `churn`, `churned`, `churn_flag`, `churn_label`, `attrited`, `is_active` → `subscription_status`
- `tenure`, `tenure_months`, `customer_tenure`, `months_on_book` → `days_since_active`
- `order_count`, `ordercount`, `recency` → `purchases_last_30d` / `days_since_purchase`
- `satisfaction_score`, `csat`, `star_rating` → `avg_review_score`
- `coupon_used`, `couponused`, `promo_used` → `discount_usage_pct`
- `monthly_charges`, `monthly_fee` → `mrr`
- `preferred_login_device`, `preferredlogindevice` → `preferred_channel`
- `preferedordercat` → `product_categories_purchased`
- `net_promoter`, `promoter_score` → `nps_score`
- `cltv`, `predicted_ltv` → `ltv`

ShopFlow result: 13/20 columns auto-confirmed at Layer 1 (was 3/20 before).

### 3. MappingReview.jsx UX improvements — Fix 3+4
**File:** `mk-intel-frontend/src/components/MappingReview.jsx`

- **Fix 4:** Confirmed tab hidden by default. User sees only actionable fields (Needs review + Unmatched). Confirmed tab available but not shown unless clicked.
- **Fix 3:** `FIELD_TYPE_HINTS` dictionary — 60+ fields with expected format shown in dropdown: `churn_risk_score · float 0-1 e.g. 0.72`, `subscription_status · string e.g. active/cancelled`. Note: native `<select>` doesn't render styled option text in all browsers — custom dropdown needed for full visual effect (TODO).
- Tab order changed: Needs review → Unmatched/skipped → Confirmed
- Context hint text above each tab's field list
- **Known limitation:** Type hints in dropdown not always visible due to browser `<select>` rendering constraints.

### 4. SessionDetail.jsx — Behavioral mode UI
**File:** `mk-intel-frontend/src/pages/SessionDetail.jsx`

- Blue info banner shown when `analysis_mode == "behavioral"`: explains no demographic baseline, profiles are directional only
- "Behavioral profile" blue badge on TAR cards when `ta_id` contains `_BEH`
- `BEH` added to `CONFIDENCE_LABELS`

### 5. sessions.py — analysis_mode in response
**File:** `backend/routers/sessions.py`

- `GET /sessions/{id}` now returns `analysis_mode` field
- Uses `getattr(session, 'analysis_mode', 'bta')` as safe fallback

### 6. temperature=0 on effectiveness gate
**File:** `mk_tar_generator.py`

- `_call()` method now accepts `temperature` parameter (default 1.0)
- Effectiveness section call uses `temperature=0` — gate decision must be deterministic given identical input
- All other sections remain at default temperature for richer prose variation

### 7. Deterministic k-selection — silhouette random_state=42
**File:** `ingestion/mk_data_ingestor.py`

- `silhouette_score()` full data call: added `random_state=42`
- `silhouette_score()` sample call: added `random_state=42`
- `np.random.choice()` for stratified sample: changed to `np.random.RandomState(42).choice()`

**Root cause:** `silhouette_score` with `sample_size` uses internal random sampling. Without a seed, scores varied slightly across runs, occasionally tipping `best_k` selection (e.g. k=4 vs k=5), producing different cluster IDs.

### 8. company_specific_name differentiation using structural fields
**File:** `mk_tar_prefilter.py`

Updated `company_specific_name` instruction in all 3 BTA cases (A, B1, B2):

**Before:** differentiate using behavioral signals only
**After:** differentiate behavioral signals first, then use locked structural fields (`dominant_age_bin`, `dominant_sex_label`, `dominant_edu_tier`) as fallback when behavioral signals are similar

Examples added: `'Mid-50s Male Low-Risk Renewers'`, `'Established Female Homeowner Subscribers'`

**Architecture justification:** Structural fields are census-derived ground truth, already locked in prompts. Using them in names is architecturally correct and ethically sound in standard compliance mode.

---

## Stability testing results (April 18, 2026)

### BTA mode (GlobalCart)
- Cluster IDs: stable across fresh sessions ✅ (same n per BTA)
- Ranking order: mostly stable, occasional flip ⚠️
- Effectiveness variance: BTA_04 ranged 0.61–0.86 across runs ❌
- Root cause: `_refine_with_llm` runs at default temperature → different profile prose → different effectiveness context despite `temperature=0` on gate call
- Names: now distinct between BTA_04 and BTA_06 after naming fix ✅

### Behavioral mode (ShopFlow)
- Cluster IDs: unstable across fresh sessions (CS00/01/03/05 vs CS00/01/02/04) ⚠️
- Normalized data: proven identical across runs ✅
- Root cause: K-Means label ordering is arbitrary — same centroids get different integer labels
- Fix needed: sort cluster labels deterministically by size after K-Means

---

## Pending items — prioritized

### IMMEDIATE — next session
1. **`_refine_with_llm` temperature=0** (`mk_tar_prefilter.py`) — fixes BTA effectiveness rating variance across runs. This is the main remaining source of score instability in BTA mode.

2. **Cluster label determinism** (`ingestion/mk_data_ingestor.py`) — sort K-Means cluster labels by size after fitting so CS00 is always the largest cluster. Fixes behavioral mode cluster ID instability.
   ```python
   # After km.fit_predict(features):
   from collections import Counter
   size_order = sorted(Counter(labels).keys(), key=lambda k: -Counter(labels)[k])
   remap = {old: new for new, old in enumerate(size_order)}
   labels = np.array([remap[l] for l in labels])
   ```

3. **`subscription_status` value standardization** (`ingestion/normalizer.py`) — add `'0': 'active', '1': 'cancelled'` to `VALUE_SYNONYMS` for `subscription_status`. Currently ShopFlow stores `'0'`/`'1'` as strings, not `'active'`/`'cancelled'`.

4. **Custom dropdown for MappingReview** (`MappingReview.jsx`) — native `<select>` doesn't render type hints visually in all browsers. Replace with custom dropdown component that shows `field · expected format` with proper styling.

5. **Add `clustering/cluster_stats.json` to ZIP export** (`backend/routers/admin.py`) — contains `feature_names`, `k`, `silhouette_scores` needed for clustering diagnostics and future Clustering Results Screen.

### SOON
6. **LLM alternatives in mapping dropdown** — when user opens dropdown to change a field, show 3-5 LLM-suggested alternatives at top before full list. Requires: `alternatives` field in LLM mapping response, stored in `column_mapping.json`, exposed in API, rendered in `MappingReview.jsx`. Quality depends on synonym + type validation improvements.

7. **Post-LLM type validation** (`ingestion/normalizer.py`) — Layer 1: reject string values mapped to numeric fields. Layer 2: reject binary 0/1 distribution mapped to continuous float fields (e.g. `churn_risk_score`). Deterministic, universally applicable. Hold until synonym dictionary is comprehensive enough that LLM only sees genuinely ambiguous fields.

8. **Behavioral mode — `_generate_profile_for_beh` temperature=0** (`mk_tar_prefilter.py`) — further reduces behavioral TAR variance. Lower priority than BTA stability.

### ROADMAP
9. **Behavioral archetype library** — equivalent of BTA baseline but derived from real B2C behavioral datasets. Would give behavioral mode a stable anchor similar to census data in BTA mode. Major product investment.

10. **TAR deduplication** — if two TARs in the same session generate the same `company_specific_name`, merge them (combined cell size, higher-confidence profile). Prevents analyst confusion from near-identical reports.

11. **Clustering results transparency screen** (ROADMAP Section 6)

12. **Vercel deployment** for frontend

13. **Run live tab** (Examples page) — pre-loaded GlobalCart/CloudSync dataset with auto-filled setup

14. **Download buttons** — exec summary HTML + TAR print stylesheet

---

## Known bugs / issues

| Issue | File | Status |
|---|---|---|
| `subscription_status` values stored as `'0'`/`'1'` strings | normalizer.py VALUE_SYNONYMS | TODO |
| Cluster label ordering non-deterministic (behavioral mode) | mk_data_ingestor.py | TODO |
| `_refine_with_llm` runs at default temperature → effectiveness variance | mk_tar_prefilter.py | TODO |
| Type hints not visible in native `<select>` dropdown | MappingReview.jsx | TODO |
| `coverage_computed.txt` always empty | normalizer.py | Known, low priority |

---

## Frontend status — COMPLETE

### Stack
- React + Vite + Tailwind v3
- react-router-dom
- axios (via `src/api/client.js`)

### Design system (tailwind.config.js)
```js
colors: {
  navy: { 900: '#0A1628', 800: '#0D1F3C', 700: '#102847' },
  teal: { accent: '#14C9B8', dark: '#0D7377' },
  ink: '#0F1923',
  slate: '#5C6B7A',
  surface: '#F8F7F4',
}
// Hero gradient: radial-gradient(ellipse at 50% -10%, #102847 0%, #0A1628 65%)
// Page background: #F8F7F4 (warm off-white)
// Font: Inter
```

### Routes (App.jsx) — ALL BUILT
```jsx
<Route path="/" element={<Landing />} />
<Route path="/examples" element={<Examples />} />
<Route path="/examples/:slug" element={<ExampleDetail />} />
<Route path="/examples/:slug/tars/:tarId" element={<TARDetail />} />
<Route path="/setup" element={<Setup />} />
<Route path="/processing/:sessionId" element={<Processing />} />
<Route path="/session/:sessionId" element={<SessionDetail />} />
<Route path="/session/:sessionId/tars/:tarId" element={<TARDetail source="session" />} />
```

---

## Key universal patterns (frontend)

### s(val) — safe string extractor
```js
function s(val) {
  if (val === null || val === undefined) return ''
  if (typeof val === 'string') return val
  if (typeof val === 'number' || typeof val === 'boolean') return String(val)
  if (Array.isArray(val)) return val.map(v => s(v)).filter(Boolean).join(', ')
  if (typeof val === 'object') {
    return val.statement || val.description || val.premise || val.consequence ||
           val.assessment || val.reason || val.text || val.content || val.value ||
           val.action || val.name || ''
  }
  return ''
}
```

### CONFIDENCE_LABELS
```js
const CONFIDENCE_LABELS = {
  A:   'Full census alignment — high confidence',
  B1:  'Income divergence — income descriptors adjusted',
  B2:  'Race divergence — cultural layer adjusted',
  C:   'Full conflict — custom archetype, confidence penalty applied',
  BEH: 'Behavioral profile — no demographic baseline',
}
```

---

## Examples endpoint — current state

**GlobalCart (examples):**
- Session: `afd8c333-35cf-4c3b-ae88-68812b4d6366`
- TARs: CS00_BTA_05, CS01_BTA_04, CS01_BTA_05, CS01_BTA_06

**CloudSync (examples):**
- Session: `fcb77ed8-f892-41a0-9006-55dffdfcd640`
- TARs: CS01_BTA_00, CS01_BTA_02, CS01_BTA_04, CS01_BTA_06

---

## Test datasets

| Dataset | Mode | Customers | BTAs/Clusters | Notes |
|---|---|---|---|---|
| GlobalCart | BTA | 50,000 | BTA_04, BTA_06 | Good stability test dataset |
| CloudSync | BTA | 1,500 | BTA_00, BTA_02, BTA_04, BTA_06 | B2B SaaS |
| ShopFlow / E-Commerce | Behavioral | 5,630 | 3-5 clusters | Behavioral-only, low variance dataset |
| TelcoX / IBM Telco | Behavioral | ~7,000 | 2-3 clusters | Good behavioral mode test |

---

## Architecture decisions made this session

- `temperature=0` on effectiveness only (not all sections) — gate must be deterministic, prose can vary
- Behavioral mode trigger: `bta_eligible_count == 0` — binary, no hybrid output
- Synonym dictionary expansion preferred over type validation (safer, lower risk)
- Structural fields (age/sex) acceptable for name differentiation in standard compliance mode
- BTA mode acceptable for production with current variance; behavioral mode needs cluster label determinism
- TAR deduplication deferred — naming fix preferred first

---

## Running locally

```bash
# Terminal 1 — Celery worker
cd /Users/marcomagnolo/Projects/Market_Kinetics
source venv/bin/activate
brew services start redis
PYTHONPATH=/Users/marcomagnolo/Projects/Market_Kinetics celery -A backend.celery_app worker --loglevel=info

# Terminal 2 — Backend API
cd /Users/marcomagnolo/Projects/Market_Kinetics
source venv/bin/activate
PYTHONPATH=/Users/marcomagnolo/Projects/Market_Kinetics uvicorn backend.main:app --host 0.0.0.0 --port 8000

# Terminal 3 — Frontend
cd /Users/marcomagnolo/Projects/Market_Kinetics/mk-intel-frontend
npm run dev
# → http://localhost:5173
```

---

## API client reference (src/api/client.js)

```js
const BASE_URL = import.meta.env.VITE_API_URL || 'https://web-production-7ec13.up.railway.app'

sessions: {
  create: () => api.post('/sessions'),
  get: (id) => api.get(`/sessions/${id}`),              // returns analysis_mode
  setCompany: (id, data) => ...,
  setObjective: (id, data) => ...,
  addSobj: (id, data) => ...,
  approveSobj: (id, sobjId) => ...,
  ingest: (id, file) => ...,
  getJob: (id, jobId) => ...,
  prefilter: (id) => ...,
  generate: (id, demoToken, byokKey) => ...,
  getTars: (id) => ...,
  getTar: (id, tarId) => ...,
  getRankings: (id) => ...,
  getSummary: (id, tarId) => ...,
  getColumnMapping: (id) => api.get(`/sessions/${id}/column-mapping`),
  updateColumnMapping: (id, amendments) => api.patch(`/sessions/${id}/column-mapping`, { amendments }),
  export: (id) => api.get(`/sessions/${id}/export`, { responseType: 'blob' }),
}

examples: {
  list: () => api.get('/examples'),
  get: (slug) => api.get(`/examples/${slug}`),
  getTar: (slug, tarId) => api.get(`/examples/${slug}/tars/${tarId}`),
  getSummary: (slug, tarId) => api.get(`/examples/${slug}/tars/${tarId}/summary`),
}
```

---

## What a new Claude instance needs to know immediately

1. **Two analysis modes:** BTA (census-grounded, default) and behavioral (no census baseline, triggered when no BTA-eligible records). `session.analysis_mode` field controls routing throughout pipeline.

2. **Re-normalization on mapping amendment:** `run_prefilter` in `backend/tasks/pipeline.py` re-runs the full normalization pipeline if `user_amended: true` in `column_mapping.json`. Includes analysis_mode re-detection.

3. **Synonym dictionary at 143 entries:** Most common B2C column names handled at Layer 1 (rules) before LLM sees them. ShopFlow gets 13/20 columns auto-confirmed.

4. **temperature=0 on effectiveness gate only** — gate decision is deterministic, prose sections vary by design.

5. **Behavioral mode UI:** Blue banner in SessionDetail, "Behavioral profile" badge on TAR cards, `BEH` confidence case in CONFIDENCE_LABELS.

6. **Known stability issue:** `_refine_with_llm` runs at default temperature → effectiveness rating can vary ±0.15 across identical runs in BTA mode. Fix: set `temperature=0` on `_refine_with_llm` in `mk_tar_prefilter.py`.

7. **Known stability issue:** K-Means cluster label ordering is arbitrary → behavioral mode cluster IDs can shift across fresh sessions despite identical data. Fix: sort cluster labels by size after K-Means.

8. **The `s(val)` function is critical.** Every TAR field rendered in JSX must go through `s()`. Never render raw TAR fields directly.

9. **After every Railway deploy:** call `POST /admin/setup-store?admin_key=Fibonacci12358!` to repopulate ChromaDB.

10. **Next session priority order:**
    - Fix `_refine_with_llm` temperature=0 in `mk_tar_prefilter.py`
    - Fix cluster label determinism in `mk_data_ingestor.py`
    - Fix `subscription_status` value synonyms (`'0'`→`'active'`, `'1'`→`'cancelled'`)
    - Custom dropdown for MappingReview type hints
    - Add `cluster_stats.json` to ZIP export

---

## Files to attach in new chat

### Core pipeline files
1. `MK_Intel_Handoff_v5.md` (this file)
2. `mk_intel_session.py`
3. `mk_tar_prefilter.py`
4. `mk_tar_generator.py`
5. `ingestion/mk_data_ingestor.py`
6. `ingestion/normalizer.py`

### Backend files
7. `backend/routers/pipeline.py`
8. `backend/routers/sessions.py`
9. `backend/routers/admin.py`
10. `backend/tasks/pipeline.py`
11. `backend/main.py`

### Frontend files
12. `src/api/client.js`
13. `src/pages/SessionDetail.jsx`
14. `src/pages/Processing.jsx`
15. `src/components/MappingReview.jsx`
16. `src/pages/Setup.jsx`
