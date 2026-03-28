# MK Intel — Architecture & Decision Record

**Version:** 1.0  
**Last updated:** March 2026  
**Status:** Living document — update on every significant architectural decision

This document records the design reasoning behind every major component of the MK Intel platform. It is intended as a reference for future development, onboarding collaborators, and portfolio explanation. It covers not just what was built but why, what was considered and rejected, and what constraints shaped each decision.

---

## Part 1 — Data Foundation: ACS PUMS

### Why ACS PUMS, not Census summary tables or CPS

The American Community Survey Public Use Microdata Sample (ACS PUMS) was chosen as the primary structural data source for the following reasons:

**Individual-level records.** ACS PUMS provides one row per person, preserving the full joint distribution of demographic and socioeconomic attributes. Census summary tables aggregate counts by geography and lose the cross-tabulation structure needed for segmentation. The Current Population Survey (CPS) covers fewer variables and is optimized for labor market analysis rather than audience profiling.

**Sample size.** The 5-year ACS PUMS covers approximately 15.9 million person records, representing the full U.S. adult population with person-level survey weights (`PWGTP`). This is large enough to support K-Prototypes clustering at k=7 with all clusters exceeding 38,000 weighted adults.

**Variable richness.** ACS PUMS includes individual income (PINCP), household income (HINCP), employment status, educational attainment, housing tenure, marital status, race/ethnicity, household composition, and vehicle counts — the full structural feature set required for audience archetypes.

**Survey weights.** All population-level statistics are computed using `PWGTP` weights, ensuring cluster profiles represent the actual U.S. adult population distribution rather than the raw sample.

### Individual vs. household as the unit of analysis

**Decision: the individual is the targeting unit.**

MK Intel produces audience profiles for B2C targeting. The end target is always an individual — a person who will receive a message, see an ad, or make a purchase decision. Household-level characteristics are contextually useful but should not drive segmentation.

This principle has two concrete implications:

1. **Clustering features are individual-level.** Employment status, individual income (`income_tier_fixed` derived from `PINCP`), age, education, marital status, and housing tenure all describe the individual. Household income (`hhincome_tier` derived from `HINCP`) was explicitly removed as a clustering feature in the v2 re-clustering.

2. **Household income is retained as a descriptor.** Household income appears on BTA cards as `dominant_household_income_tier` alongside `dominant_income_tier` (individual). The gap between them is analytically valuable — a `20-49k` individual in a `100-199k` household is in a structurally different position than a `20-49k` individual in a `20-49k` household. The first may be economically dependent or in a multi-earner family; the second is likely the primary or sole earner. This distinction drives susceptibility differences downstream.

**What was rejected:** an earlier v1 approach used `hhincome_tier` as a clustering feature alongside `income_tier_fixed`. This was identified as a design flaw because it allowed household-level income to shape the archetypes, making them inconsistent with the individual targeting unit principle. The fix required a full re-clustering (NB05b through NB11).

### Income variables: PINCP vs. HINCP

| Variable | ACS field | Scope | Used for |
|---|---|---|---|
| `income_tier_fixed` | PINCP | Individual personal income | Clustering feature, BTA card, matching |
| `income_tier_pct` | PINCP | Individual personal income (percentile bins) | Dropped — 22% missing, overlaps with fixed |
| `hhincome_tier` | HINCP | Household total income | BTA card descriptor only |

**Income tier binning:** Fixed-width bins aligned to ACS reporting thresholds: `<=0`, `0-19k`, `20-49k`, `50-99k`, `100-199k`, `200k+`. The `<=0` category captures non-earners (students, unemployed, early career). Minors are assigned NA — income is not conceptually applicable to dependent children.

**Why fixed bins over percentile bins:** Percentile bins (`income_tier_pct`) create moving thresholds that shift with population distribution changes. Fixed bins allow direct comparison to external datasets that report income in dollar ranges, which is the common format for business data (`income_annual` in the canonical schema). This alignment is critical for the income mismatch fix in NB13.

### Adult-only restriction

Clustering and all downstream layers operate on adults only (actor_class == 'Adult', AGEP ≥ 18). Minors are excluded for three reasons:

1. Individual income (PINCP) is not conceptually applicable to dependent children.
2. Media and psychographic surveys (GSS, Pew NPORS) are adult-only instruments.
3. B2C targeting in all supported sectors (SaaS, e-commerce, banking) targets adult decision-makers.

Minors are in the raw ACS PUMS data but are filtered at NB05b (clustering input preparation).

### HUS merge: how household income gets onto person records

ACS PUMS has two files: the Person file (PUS) and the Housing Unit file (HUS). Household-level variables like `HINCP` live in HUS. The join key is `serialno` — the unique housing unit identifier shared across all persons in a household.

The merge is performed in NB04. Every person record gets the `hhincome_tier` of their household. Validation confirms that all members of a given `serialno` share the same `hhincome_tier` (every `serialno` group has `hhincome_tier.nunique() == 1`).

---

## Part 2 — Psychological Layer: GSS

### Why GSS

The General Social Survey (GSS) is the only recurring, nationally representative survey in the U.S. that simultaneously covers ideology, party alignment, religiosity, life satisfaction, and media engagement with consistent methodology across decades. Alternatives considered:

- **ANES (American National Election Studies):** Deeper political coverage but biennial, election-focused, and missing media engagement variables.
- **Pew Research surveys:** Excellent media data (→ Part 3) but fragmented across topic-specific studies, not structured for cross-tabulation with ACS demographics.
- **Proprietary attitudinal surveys:** Not available for open-source portfolio use.

### Why projection, not direct merge

GSS and ACS cannot be directly merged — they are independent samples with no shared identifiers. Direct merge is impossible.

**The projection approach:** For each ACS adult, psychographic trait probabilities are inferred from GSS respondents who share the same demographic cell. The result is not an individual's actual ideology — it is the probability distribution of ideological orientations among demographically similar respondents in the GSS.

This is a population-level inference, not individual imputation. The resulting probabilities are aggregated to the cluster level using `PWGTP` weights, producing cluster-level psychological profiles that represent the likely attitudinal distribution of each segment.

### Hierarchical fallback schema

GSS cells are defined by `[age_bin, sex_label, race_eth, edu_tier]` at Level 1. When a GSS cell has fewer than 10 respondents (insufficient for reliable probability estimation), the schema falls back progressively:

| Level | Schema | Cells dropped |
|---|---|---|
| 1 | age_bin + sex + race + edu | — |
| 2 | age_bin + race + edu | sex |
| 3 | age_bin + sex + race | edu |
| 4 | sex + race + edu | age |
| 5 | race + edu | age + sex |
| 6 | race | all except race |
| Global | national baseline | all |

This ensures every ACS adult receives a probability assignment rather than null values, while using the most granular available cell.

### Signal direction preservation

**This is the most important design rule for the GSS layer.** When psychological signals are passed to downstream LLM prompts (NB11 BTA card generation), they must carry direction information — whether the cluster is *above* or *below* the national baseline on each trait.

A cluster where `party_alignment: republican` is *below* baseline (−0.050) is a non-Republican cluster. A cluster where the same trait is *above* baseline (+0.069) is a Republican-leaning cluster. Without direction, the LLM cannot distinguish these cases and will default to interpreting trait labels as positive associations.

**Implementation:** In NB10, when constructing the `psych_signals` list passed to the BTA card prompt, each signal includes its direction:

```python
trait_with_dir = f"{trait} ({direction})"
# e.g. "party_alignment: republican (below baseline)"
# vs   "party_alignment: republican (above baseline)"
```

**Tags:** Only above-baseline signals appear in BTA tags. Below-baseline signals are excluded from tags to avoid misleading retrieval. A tag `party_alignment_republican` should mean "this segment leans Republican," not "this segment was tested on Republican alignment and scored low."

### Income mapping: GSS `realrinc` → `income_tier_fixed`

GSS uses `realrinc` (inflation-adjusted real family income in 1986 dollars). This is mapped to `income_tier_fixed` using the same ACS-aligned bins after adjusting for inflation to current dollars. The mapping preserves individual income semantics — `realrinc` is a personal/family income variable, not household aggregate.

### Education harmonization

GSS uses `degree` (0–4 ordinal: less than HS, HS, junior college, bachelor, graduate). This is mapped to the ACS `edu_tier` schema:

| GSS `degree` | ACS `edu_tier` |
|---|---|
| 0 — Less than HS | HS_or_less |
| 1 — High school | HS_or_less |
| 2 — Junior college | Some_college |
| 3 — Bachelor | Bachelor |
| 4 — Graduate | Graduate |

The mapping loses the distinction between "less than HS" and "HS diploma" — acceptable given that the BTA matching schema uses the same collapsed categories.

---

## Part 3 — Media Layer: Pew NPORS

### Why Pew NPORS

The Pew News Platforms and Online Revenue Sources (NPORS) survey provides platform-level media usage data (YouTube, Facebook, Instagram, TikTok, WhatsApp, Reddit, Snapchat, X/Twitter, Threads, Bluesky, TruthSocial) alongside internet access and frequency measures. It is the most granular publicly available survey for digital media behavior by demographic segment.

The projection methodology mirrors the GSS approach — media probabilities from NPORS are assigned to ACS adults by demographic cell matching.

### Education split: Bachelor vs. Graduate

NPORS aggregates Bachelor and Graduate into a single "college" category (`educcat == 1`). The ACS distinguishes them. To resolve this, a donor proportion table is built from the ACS structural population:

For each `[age_bin, sex_label, race_eth, income_tier_fixed]` cell, the share of college-educated adults holding a Bachelor vs. Graduate degree is computed. This proportion is merged onto NPORS records and used to probabilistically assign edu_tier within the college category.

---

## Part 4 — Clustering

### Algorithm: K-Prototypes

K-Prototypes was chosen over K-Means, K-Modes, and UMAP+HDBSCAN for mixed-type data:

- **K-Means:** Requires numeric features only. Encoding categorical variables (age_bin, emp_tier, etc.) as dummies inflates dimensionality and creates distance metric distortions.
- **K-Modes:** Categorical only. Drops `household_size` and `vehicle_count` (numeric), losing meaningful structural signals.
- **K-Prototypes:** Handles mixed categorical + numeric features natively using a combined dissimilarity measure. Best fit for the ACS feature set.
- **UMAP+HDBSCAN:** More powerful but non-deterministic, harder to interpret, and produces variable cluster counts. Deferred to roadmap if behavioral data becomes predominantly categorical.

### Feature set (v2 — current)

**Clustering features (individual-level):**
- `age_bin` — life stage
- `sex_label` — demographic
- `race_eth` — demographic (descriptive, not used for targeting)
- `edu_tier` — socioeconomic
- `emp_tier` — labor market position
- `income_tier_fixed` — individual income (PINCP-derived)
- `mar_tier` — household structure proxy
- `tenure` — housing tenure
- `household_size` — numeric
- `vehicle_count` — numeric
- `household_type` — housing unit classification

**Explicitly excluded from clustering:**
- `hhincome_tier` — household-level; kept as archetype descriptor (see Part 1)
- `income_tier_pct` — overlaps with `income_tier_fixed`, 22% missing
- `commute_tier` — 52.6% missing, unreliable signal
- `puma` — high cardinality, no reliable area-type proxy at PUMA level

### k=7 selection

Three candidate solutions (k=6, 7, 8) were evaluated on cost (K-Prototypes objective) and balance (max cluster size / min cluster size):

| k | Cost | Imbalance ratio | Min cluster |
|---|---|---|---|
| 6 | 3.027M | 4.56 | 39,741 |
| 7 | 2.901M | **4.10** | 38,470 |
| 8 | 2.851M | 9.76 | 18,895 |

k=7 was selected: it achieves the best balance (4.10 imbalance ratio) at a meaningful cost reduction from k=6 (−4.2%). k=8 produces an 18,895-record minimum cluster — small enough to raise fragmentation concerns and reduce real-world targetability.

All seven k=7 clusters exceed 38,000 adults, representing at least 5.4% of the U.S. adult population each. Every segment is large enough to be a meaningful, targetable group.

### The seven archetypes (v2)

| ID | Name | Pop. share | Key traits |
|---|---|---|---|
| BTA_00 | Diverse Mid-Life Workers | 17.2% | 35-44, employed, married, White/Hispanic/Black mix, owner, 20-49k individual / 50-99k HH |
| BTA_01 | Older Non-Partnered Adults | 16.2% | 65+, previously married, No_Rent, 0-19k individual, Democrat-leaning |
| BTA_02 | Young Hispanic Working Adults | 5.4% | 35-44, Hispanic plurality, married, owner, 20-49k individual / 100-199k HH |
| BTA_03 | Retired Renters | 12.5% | 65+, retired, male, renter, strongly conservative, high media engagement |
| BTA_04 | Mid-Career Homeowners | 15.1% | 45-54, employed, male, owner, 20-49k individual / 100-199k HH |
| BTA_05 | Young Non-Owning Singles | 14.4% | 25-34, never married, No_Rent, 0-19k individual, secular, independent |
| BTA_06 | Established Mid-Career Homeowners | 19.1% | 55-64, employed, female, owner, Some_college, Republican-leaning, Facebook + radio |

### Archetype naming principles

Names describe the **individual**, not the household. "Working Families" was rejected in favor of "Mid-Life Workers" because MK Intel targets individuals, not households. Names reflect the three most structurally distinctive traits: life stage, economic position, and housing situation. Race/ethnicity appears only where it is the dominant demographic signal (BTA_02 Hispanic plurality is structurally meaningful given the household income gap).

---

## Part 5 — BTA Card Architecture

### What a BTA card is

A BTA (Baseline Target Audience) card is a structured intelligence object representing one societal archetype. It is not a client-specific customer segment — it is a reference object derived from U.S. population data. Client data is mapped *to* BTA cards during ingestion.

Each card contains:
- Structural descriptors (`dominant_age_bin`, `dominant_income_tier`, `dominant_household_income_tier`, `dominant_emp_tier`, etc.)
- Psychological signals and summary (from GSS projection)
- Media signals and summary (from Pew NPORS projection)
- LLM-generated narrative, psych summary, media summary, and RAG interpretation
- Tags for retrieval filtering
- RAG text for ChromaDB embedding

### Dual income fields

Every BTA card carries both:
- `dominant_income_tier` — individual income, the primary matching field for business data
- `dominant_household_income_tier` — household income, contextual descriptor

The gap between them surfaces household composition insights. BTA_02 (individual 20-49k, household 100-199k) signals multi-earner Hispanic households. BTA_04 (individual 20-49k, household 100-199k) signals bimodal multi-generational homeowner households. BTA_01 (individual 0-19k, household 20-49k) signals economic dependency in older non-partnered adults.

### Signal direction in prompts

LLM prompts for psych summaries receive direction-aware signal labels:
- `"party_alignment: republican (above baseline)"` → cluster leans Republican
- `"party_alignment: republican (below baseline)"` → cluster is distinctly non-Republican

Without direction, the LLM cannot interpret the signal correctly and will default to positive association. This was identified as a critical flaw in v1 BTA generation and fixed in v2.

### Tag design

Tags include only **above-baseline** psychological signals. Below-baseline signals are excluded because a tag should describe what a segment distinctively *is*, not what it is not. A `party_alignment_republican` tag should be interpretable without consulting direction metadata.

Structural tags (age, income, tenure, etc.) carry no direction concept and are included unconditionally — they are dominant values, not deviations.

### RAG corpus

The RAG corpus (`mk_bta_rag_corpus.jsonl`) is loaded into ChromaDB using the `all-MiniLM-L6-v2` embedding model. Retrieval queries are LLM-inferred trait descriptions, not raw SOBJ text. The segment store supports:
- `query_segments(text, n_results, where)` — semantic similarity search with optional metadata filtering
- `get_segment_by_id(id)` — direct lookup
- `list_all_segments()` — full enumeration sorted by segment_id

---

## Part 6 — Business Data Ingestion

### The canonical behavioral schema

The canonical schema (`mk_canonical_behavioral_schema_v1.json`) standardizes all incoming business data into a common format regardless of source system. It defines six domains:

| Domain | Tier | Purpose |
|---|---|---|
| identity | 1 | Structural fields for BTA mapping (age, income, tenure, etc.) |
| behavioral | 1 | Engagement signals (sessions, activity) |
| transactional | 1 | Revenue and subscription state |
| journey | 2 | Lifecycle and churn signals |
| engagement | 2 | Channel preference and opt-in state |
| text_signals | 3 | LLM-extracted sentiment and themes (placeholder) |

Fields are typed as `raw` (provided by company), `derived` (auto-computed), or `accepted_either`.

**Why a schema at all:** Without a schema, every business dataset requires custom code. The schema allows the normalizer to handle arbitrary column names and value formats through a mapping layer, reducing per-company integration to a column mapping approval step rather than bespoke engineering.

### Column name mapping: three-layer approach

**Layer 1 — Rules (rapidfuzz fuzzy matching + synonym dictionary):**  
Handles obvious matches: `"Age"`, `"age_years"`, `"customer_age"` → `"age"`. The synonym dictionary covers ~200 known column name variants. Rapidfuzz handles approximate matches (edit distance). Covers ~70-80% of real-world column names at zero cost.

**Layer 2 — LLM inference (Claude Haiku):**  
Runs only on columns not matched by rules. Sends unmatched column names + 5 sample values to Claude Haiku. Returns structured mapping with confidence scores. Cost: <$0.01 per new company ingestion.

**Layer 3 — Analyst review:**  
Full proposed mapping is displayed (rules + LLM) with confidence scores. Analyst confirms, corrects, or adds manual mappings. Approved mapping saved to `data/company_data/{slug}/column_mapping.json`. Never runs again for this company.

**Why this order:** Rules are free and instant. LLM is cheap but adds latency. Analyst review adds friction but ensures correctness for production use. Running all three in order minimizes cost while guaranteeing accuracy.

### Value vocabulary standardization

After column mapping, values are standardized against canonical vocabularies. Known rule-based patterns handled automatically:
- Binary `0/1` → `True/False` on boolean fields
- `Y/N`, `Yes/No` → `True/False`
- Single-char codes (`M/F`) → `Male/Female`

LLM invoked for non-standard or business-specific values.

### Derived fields

Auto-computed during ingestion:
- `age` → `age_bin` (ACS-aligned bins: 18-24, 25-34, 35-44, 45-54, 55-64, 65+)
- `income_annual` → `income_tier` (ACS-aligned fixed bins: 0-19k, 20-49k, 50-99k, 100-199k, 200k+)
- `last_active_date` → `days_since_active`
- `renewal_date` → `days_to_renewal`
- `customer_since` ← `membership_years` using `dataset_export_date` as reference

**Critical:** `dataset_export_date` must be collected at upload time. Duration-to-date conversions (e.g., `membership_years` → `customer_since`) produce wrong dates if today's date is used as reference for a dataset exported months ago. This field must be prominently surfaced in the upload UI — not buried in advanced settings.

### The income mismatch bug (fixed in NB13)

**Problem:** BTA cards were generated with `dominant_household_income_tier` as the income field. Business datasets almost universally report `income_tier` — individual income. When the matcher tried to compare incoming `income_tier` against `dominant_household_income_tier`, it was comparing individual income to household income — apples to oranges. This produced incorrect confidence scores.

**Fix:** BTA cards now carry `dominant_income_tier` (individual) as the primary matching field. The matcher compares incoming `income_tier` against `dominant_income_tier`. `dominant_household_income_tier` remains on the card as contextual metadata, used in the ZIP enrichment validation but not in primary structural matching.

### Compliance modes

Four modes control which fields may be used as clustering inputs:

| Mode | Excluded from clustering |
|---|---|
| standard | None |
| banking_us | gender, age_bin, zip_code, credit_score_tier |
| banking_eu | gender, age_bin, zip_code, marital_status, credit_score_tier |
| eu_gdpr | gender, zip_code |

**Never collected from business data under any mode:** race_eth, religion, disability_status, sexual_orientation, genetic_data, political_opinions.

**`race_eth` appears only as a BTA population descriptor** derived from ACS census data — never from individual customer records. `credit_score_tier` is descriptor-only in all modes.

**ZIP code policy:** Permitted for clustering in standard and eu_gdpr. Excluded from clustering in banking_us and banking_eu due to disparate impact risk (ZIP codes are well-established income and race proxies in credit decisioning law).

### Coverage scoring

Coverage is computed per-record and per-ingestion-run:
- `coverage_score`: overall proportion of non-null fields (0-1)
- Domain-level scores: `identity_coverage`, `behavioral_coverage`, etc.
- `structural_weight_coverage`: normalized sum of BTA mapping weights for present identity fields
- `bta_eligible`: True if US record AND `structural_weight_coverage >= 0.35`
- `confidence_tier`: low (<0.3), medium (0.3-0.6), high (>0.6)

BTA mapping weights for identity fields (normalized at runtime):

| Field | Weight |
|---|---|
| age_bin | 0.4 |
| income_tier | 0.4 |
| housing_tenure | 0.1 |
| education | 0.1 |
| marital_status | 0.1 |

Age and income are the primary discriminators with equal weight (0.4 each). A dataset with only age_bin and income_tier has normalized structural coverage of 0.80 — well above the 0.35 bta_eligible threshold.

---

## Part 7 — ZIP Code Enrichment

### Purpose

ZIP code enrichment is a secondary validation layer, not a primary data source. It cross-checks BTA assignments by comparing individual-level data to ZIP-level demographic baselines derived from ACS 5-year estimates.

The enrichment table (`zcta_enrichment.parquet`) maps ZCTAs to:
- `income_tier` — derived from median household income (B19013)
- `dominant_race_eth` — derived from race/ethnicity population counts (B03002) using a 35% plurality threshold

### Compliance gating

ZIP enrichment is **disabled** in banking_us, banking_eu, and eu_gdpr modes. ZIP-inferred race is a proxy discrimination risk in regulated sectors. In standard mode, `zip_inferred_race_eth` is a validation signal only — it is never used as a targeting criterion.

### Three confidence cases

After structural BTA matching, ZIP enrichment runs a confidence validation:

| Case | Condition | Confidence | Action |
|---|---|---|---|
| A | Age + income + race all align with matched BTA | high | Accept match as-is |
| B | Age + income match, race diverges | medium | Accept match, flag divergence |
| C | Income conflict (individual income and ZIP income point to different BTA) | low | Flag for LLM custom archetype |

Case C triggers a custom archetype builder — an LLM generates a modified BTA description that accounts for the structural tension (e.g., a high-income individual in a low-income ZIP, or vice versa).

### Why ZCTA, not ZIP codes

ZIP codes are postal routing constructs with no fixed geographic boundaries. ZCTAs (ZIP Code Tabulation Areas) are Census-defined approximations of ZIP code service areas that do have geographic boundaries. The correspondence is close (>95% of ZIP codes map cleanly to a ZCTA) but not exact. The `zcta_enrichment.parquet` table uses ZCTA as the join key and notes this imprecision in the documentation.

---

## Part 8 — Structural Matching (BTA ↔ Customer)

### Matching logic

Each customer record is matched to a BTA card using weighted structural field comparison. The primary fields are `age_bin` and `income_tier` (weight 0.4 each). Secondary fields are `housing_tenure`, `education`, and `marital_status` (weight 0.1 each).

Matching is normalized at runtime — a dataset missing `housing_tenure` has its structural weights renormalized across the fields that are present. A dataset with only `age_bin` and `income_tier` achieves 80% of maximum structural coverage.

### Tied matches: dual-assignment approach

When two BTA cards are structurally equidistant from a customer cluster, both are returned with `confidence: low` rather than applying a tiebreaker. The reasoning: a tiebreaker would impose a false precision. Returning both preserves the ambiguity and signals to the analyst that additional data would resolve the assignment.

This is preferable to returning a single "winner" that may be wrong.

---

## Part 9 — TAR Generation (formerly TAAW)

### What a TAR is

A Target Audience Report (TAR) is a structured analytical document produced for each (TA, SOBJ) pair. It integrates:
- BTA card signals (structural, psychological, media)
- Company behavioral data
- SOBJ-specific analysis (vulnerabilities, susceptibility, channel implications)

A TAR is an analytical product, not a campaign brief. It describes the audience and the case for/against targeting them for a specific objective — it does not prescribe creative execution.

### Session model

One company → one OBJ → N SOBJs → M TARs per SOBJ

Session state is stored as JSON per session directory. The `MKSession` object manages:
- `CompanyProfile` — business context
- `Objective` — primary campaign objective
- `SupportingObjective` list — specific behavioral objectives
- `ProprietaryDataset` — ingested company data with compliance mode
- `SOBJResult` list — scored and ranked TA lists per SOBJ

Session mode (`developer`, `byok`, `demo`, `blocked`) controls API key sourcing and quota enforcement.

---

## Part 10 — Scoring Algorithm

### Design philosophy

The scoring algorithm makes TA prioritization **comparable and auditable**, not automatic. Judgment lives in the analyst's TAAW inputs — the algorithm converts that judgment into a ranked list with dimension breakdowns that explain the ranking.

All weights are explicitly labeled as placeholders. They must be calibrated against known-good TA rankings before production use (Phase 3.3).

### Four-step structure

**Step 1 — Hard gates (all four must pass):**
- G1: Effectiveness rating > 2 (analyst-assigned 0-5)
- G2: Desired behavior must be specific + observable OR specific + measurable
- G3: At least one vulnerability identified (motives, psychographics, or symbols)
- G4: At least one usable channel (not all channels restricted)

Failed gate → TA disqualified, no score assigned. Disqualified TAs appear at the bottom of the ranked list with the gate failure reason.

**Steps 2-5 — Dimension scoring (each returns 0.0–1.0):**

*Effectiveness (30%):* Analyst rating (50%) + decision rights (25%) + resource access (15%) + behavior quality boost (10%) − restriction penalty (−0.15 per high-severity restriction, capped at −0.45).

*Susceptibility (30%):* Analyst rating (45%) + net reward-risk balance (25%) + value/belief alignment (30%) × SOBJ direction modifier. Direction modifiers: maintain=1.10, increase=1.0, decrease=0.90, initiate=0.85, stop=0.80.

*Vulnerability depth (25%):* Motives (45%, with sourcing bonus) + psychographics (30%, with sourcing bonus) + symbols/cues (15%, recognized-by-TA and sourced only) + demographics (10%, presence bonus only).

*Accessibility (15%):* Best usable channel reach + breadth bonus (0.05 per strong channel, capped at +0.15) − excluded channels.

**Step 6 — Composite:**  
`C = (E × 0.30) + (S × 0.30) + (V × 0.25) + (A × 0.15)`

**Step 7 — Audience size modifier (0.85–1.15):**  
Scales by estimated audience size and estimate confidence. Low confidence blends toward neutral (1.0). Size tiers: <1K → 0.90, 1K-50K → 0.97, 50K-500K → 1.03, 500K-5M → 1.08, >5M → 1.12.

**Step 8 — Final score:**  
`F = C × M`

**Step 9 — Ranked output:**  
All (TA, SOBJ) pairs sorted by final score. Each includes composite score, dimension breakdown, strongest/weakest signal, and plain-language recommendation.

---

## Part 11 — Roadmap Items Deferred from This Build

### Test: individual income only clustering

Deferred to roadmap. Re-run clustering with `hhincome_tier` completely excluded (not even as a descriptor), cluster on `income_tier_fixed` only, and compare archetype coherence against the current dual-presence approach. Hypothesis: individual-only clustering produces sharper economic profiles at the cost of losing household composition signal.

### Usable coverage metric

Current `coverage.py` computes data coverage (what fields are present). A usable coverage metric (what fields are present AND legally usable in the active compliance mode) should be added. Formula: compute coverage only over fields not in `compliance_excluded_fields`. Critical for banking modes where several high-weight fields are excluded.

### Non-US BTA expansion

Current BTAs are US-only (ACS PUMS). Non-US customers skip BTA mapping. Future: EU baseline segmentation using Eurostat microdata + ESS for the psychological layer.

### Text signals pipeline

`text_signals` domain is a schema placeholder. LLM extraction pipeline (reviews, support tickets, social posts → structured signal dict) not yet implemented. Implement only if social media ingestion pipeline is built.

### Clustering results transparency screen

Frontend must surface: feature selection rationale table, k selection chart, cluster profile cards, post-hoc labels section, and analyst override panel. See ROADMAP.md Section 6 for full spec.

---

## Appendix: Key Column Name Decisions

| Canonical name | What it means | Source |
|---|---|---|
| `income_tier_fixed` | Individual income, fixed-width bins (PINCP) | ACS NB02 |
| `hhincome_tier` | Household income, fixed-width bins (HINCP) | ACS NB04 |
| `dominant_income_tier` | Dominant individual income tier in a cluster | NB06b / BTA card |
| `dominant_household_income_tier` | Dominant household income tier in a cluster | NB06b / BTA card |
| `income_tier` | Individual income in ingested business data | Canonical schema |
| `income_annual` | Raw annual income in ingested business data | Canonical schema |

**The naming chain matters for matching:** incoming `income_tier` maps to `dominant_income_tier` on the BTA card (both individual). Not to `dominant_household_income_tier`.
