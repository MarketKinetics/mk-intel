# MK Intel — Architecture Overview

MK Intel is an AI-first Target Audience Analysis platform that combines U.S. population segmentation, psychographic inference, and business data ingestion to produce ranked, evidence-based audience intelligence for B2C campaigns.

This document explains how the system is built and why key design decisions were made.

---

## How it works

A company uploads their customer data. MK Intel maps each customer to one of seven baseline U.S. population archetypes, enriches those archetypes with psychological and media behavior signals, scores each archetype against a campaign objective, and produces a ranked audience priority list with a structured analytical brief per audience.

The pipeline has three layers:

1. **Societal baseline** — seven archetypes derived from U.S. census and survey data, built once and reused across all clients
2. **Business data ingestion** — client data normalized, mapped to archetypes, and scored for coverage
3. **Audience analysis** — objectives defined, archetypes scored and ranked, reports generated

---

## Societal baseline pipeline

### Data sources

The baseline layer integrates three public datasets:

**ACS PUMS (American Community Survey Public Use Microdata Sample)**
The structural foundation. ~15.9M individual person records with demographic, economic, and housing variables. Used at the individual level — not aggregated summary tables — to preserve the full joint distribution of attributes needed for segmentation. Person weights (`PWGTP`) are applied throughout to ensure population representativeness.

**GSS (General Social Survey)**
The psychological layer. The only recurring nationally representative U.S. survey that simultaneously covers ideology, party alignment, religiosity, life satisfaction, and media engagement. GSS respondents cannot be directly merged with ACS records, so psychological traits are projected onto ACS adults via demographic cell matching with hierarchical fallback.

**Pew NPORS (News Platforms and Online Revenue Sources)**
The media behavior layer. Platform-level usage data for YouTube, Facebook, Instagram, TikTok, WhatsApp, Reddit, Snapchat, and others. Projected onto ACS adults using the same methodology as GSS.

### Why projection instead of direct merge

GSS and Pew are independent samples with no shared identifiers — direct merge is impossible. Instead, for each ACS adult, trait probabilities are inferred from survey respondents who share the same demographic cell (age, sex, race, education). The result is a population-level inference: not what this individual believes, but the probability distribution of beliefs among demographically similar people.

These probabilities are aggregated to the cluster level, producing psychological and media profiles that represent the likely attitudinal distribution of each segment.

### Segmentation: K-Prototypes clustering

The ACS adult population is clustered using K-Prototypes — an algorithm designed for mixed categorical and numeric data. K-Means requires numeric features only; K-Modes drops numeric features; K-Prototypes handles both natively.

**The individual is the targeting unit.** This is the foundational design principle. MK Intel targets people, not households. Clustering features are all individual-level: age, sex, race/ethnicity, education, employment status, individual income, marital status, housing tenure, household size, vehicle count, and household type.

Household income is retained as a contextual descriptor on each archetype card — it surfaces meaningful signals like multi-earner households or economic dependency — but it does not drive segmentation.

**k=7 was selected** based on balance (imbalance ratio 4.10, best of three candidates) and cost reduction. All seven clusters represent at least 5.4% of the U.S. adult population, ensuring every archetype is a substantial and targetable group.

### The seven archetypes

| ID | Name | Pop. share |
|---|---|---|
| BTA_00 | Diverse Mid-Life Workers | 17.2% |
| BTA_01 | Older Non-Partnered Adults | 16.2% |
| BTA_02 | Young Hispanic Working Adults | 5.4% |
| BTA_03 | Retired Renters | 12.5% |
| BTA_04 | Mid-Career Homeowners | 15.1% |
| BTA_05 | Young Non-Owning Singles | 14.4% |
| BTA_06 | Established Mid-Career Homeowners | 19.1% |

### BTA cards

Each archetype is represented as a BTA (Baseline Target Audience) card — a structured intelligence object that contains structural descriptors, psychological signals, media signals, LLM-generated summaries, and a RAG-ready text representation. Cards are stored in ChromaDB using `all-MiniLM-L6-v2` embeddings for semantic retrieval.

**Signal direction matters.** Psychological signals are deviations from the national baseline — a cluster can be above or below baseline on any trait. Direction is preserved in prompts passed to the LLM. A cluster that is distinctly *non*-Republican is described accurately as such, not mischaracterized as Republican-leaning. This distinction required explicit design: trait labels without direction are ambiguous and produce incorrect LLM interpretations.

---

## Business data ingestion

### Canonical schema

All incoming business data is normalized to a canonical behavioral schema with six domains: identity (age, income, gender, zip, tenure), behavioral (sessions, activity), transactional (MRR, subscription status, LTV), journey (lifecycle stage, churn risk), engagement (email, push, SMS), and text signals (LLM-extracted, placeholder).

The schema is format-agnostic. A dataset with columns named `"Age"`, `"age_years"`, or `"customer_age"` all map to the canonical `age` field through a three-layer mapping process.

### Column mapping: rules → LLM → analyst

**Layer 1 — Rules:** A synonym dictionary plus rapidfuzz fuzzy matching handles ~70-80% of real-world column names at zero cost.

**Layer 2 — LLM inference:** Unmatched columns are sent to Claude Haiku with sample values. Returns a structured mapping with confidence scores. Cost: <$0.01 per new company.

**Layer 3 — Analyst review:** The full proposed mapping is presented for confirmation. Approved mappings are saved per company and never rerun.

### Compliance modes

Four compliance modes control which fields may be used as clustering inputs:

| Mode | Key exclusions |
|---|---|
| standard | None |
| banking_us | gender, age_bin, zip_code (ECOA/FCRA informed) |
| banking_eu | gender, age_bin, zip_code, marital_status (GDPR + EU anti-discrimination) |
| eu_gdpr | gender, zip_code |

Race/ethnicity is never collected from business data under any mode. It appears only as a BTA population descriptor derived from census data.

### Coverage scoring

Each ingestion run computes structural coverage — the proportion of BTA-relevant identity fields present, weighted by their matching importance. Age and individual income each carry 40% of the structural weight. A dataset with only these two fields achieves 80% structural coverage — sufficient for reliable BTA mapping.

Records fall below a `bta_eligible` threshold if structural coverage is under 35% or if the customer is non-US.

### ZIP code enrichment

An optional enrichment layer cross-checks BTA assignments against ZIP-level demographic baselines (ACS 5-year ZCTA estimates). It produces a confidence validation with four cases:

- **Case A** — full alignment (age + race + income all match BTA): high confidence
- **Case B1** — income diverges (age + race match, ZIP household income conflicts): medium confidence
- **Case B2** — race diverges (age + income match, ZIP demographic profile differs): medium confidence
- **Case C** — full conflict (no reliable structural anchor): low confidence, LLM generates a custom archetype

ZIP enrichment is disabled in banking and EU GDPR modes due to disparate impact risk. ZIP-inferred race is never used as a targeting criterion — only as a validation signal.

---

## Audience analysis

### Session model

Each analysis run is a session: one company → one objective (OBJ) → N supporting objectives (SOBJs) → M audience reports per SOBJ. Session state is persisted as JSON and supports iterative SOBJ refinement across requests.

### TAR pre-filter and profile refinement

Before generating a full Target Audience Report (TAR), the platform runs a two-stage pre-filter:

**Stage 1 — Profile refinement.** Each TA card is refined by the LLM for the specific company and product context. The refinement scope depends on the ZIP confidence case:
- Case A: full contextual refinement of psychographic, media, and messaging descriptors
- Case B1: income-related descriptors adjusted to reflect ZIP-inferred household income
- Case B2: cultural and media layer adjusted to reflect ZIP-inferred demographic context
- Case C: no refinement — custom archetypes are already LLM-generated; passed through with a confidence penalty

Structural fields (age, income, tenure, education) are always locked from real data. The LLM only touches descriptive and contextual fields. All output stays at population segment level — no price points or tactical predictions.

**Stage 2 — Candidate shortlisting.** For each SOBJ, a rule engine scores each refined TA card on likelihood of performing the desired behavior. SOBJ vocabulary is matched to behavioral signals on the TA card (churn risk, LTV, subscription status, NPS, feature adoption, etc.). An LLM fallback handles SOBJs not matched by keyword rules. The top candidates per SOBJ — typically 3-4 — proceed to full TAR generation.

This stage reduces the number of TARs generated from O(TAs × SOBJs) to a manageable shortlist, avoiding expensive generation for implausible combinations.

### Scoring algorithm

Each (audience archetype, SOBJ) pair is scored across four dimensions:

| Dimension | Weight | What it measures |
|---|---|---|
| Effectiveness | 30% | Can this audience accomplish the objective? |
| Susceptibility | 30% | Will this audience respond to the approach? |
| Vulnerability depth | 25% | How many persuasion levers exist? |
| Accessibility | 15% | Can we reach them through available channels? |

Four hard gates screen out disqualified audiences before scoring begins (insufficient effectiveness rating, ill-defined behavior, no vulnerabilities, no usable channels). Failed gates produce a disqualification reason rather than a score.

The composite score is multiplied by an audience size modifier (0.85–1.15, scaled by estimate confidence), producing a final score used to rank all audiences for each SOBJ.

**Weights are explicitly labeled as placeholders** and must be calibrated against known-good rankings before production use. The dimension breakdown is always included in output so rankings are auditable.

---

## Key design decisions

**Individual over household.** The targeting unit is always a person. Household income is context, not signal.

**Projection over imputation.** Psychological traits are probability distributions from demographically matched survey data, not point estimates invented for individuals without survey responses.

**Direction-aware signals.** Being distinctly *non*-X on a trait is as meaningful as being strongly X. Signal direction must be preserved through the entire pipeline.

**Rules before LLM.** Column mapping runs rules first (free, instant), LLM second (cheap, slower), analyst third (accurate, requires human). Cost and latency scale with difficulty.

**Transparent scoring.** The algorithm produces ranked lists with full dimension breakdowns. Every score is explainable — no black-box outputs.

**Compliance as a first-class concern.** Compliance mode is set at ingestion time and gates are applied automatically throughout. The platform does not rely on analyst recall for compliance rules.

**Pre-filter before generate.** Running TAR generation on every possible (audience, SOBJ) combination is expensive and produces noise. A rule-based pre-filter shortlists only plausible candidates before any generation call. Weak candidates that slip through are handled by the TAR effectiveness gate and scoring algorithm — the pre-filter is deliberately coarse.

**LLM refines, data grounds.** The LLM contextualizes audience profiles for company and product context — it does not override real data signals. Company data beats ZIP inference beats BTA baseline beats LLM speculation. This hierarchy is enforced structurally: fields derived from real data are locked before LLM prompts are constructed.
