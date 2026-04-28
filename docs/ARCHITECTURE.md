# MK Intel — Architecture Overview

MK Intel is a target audience analysis system that uses LLM reasoning as a load-bearing component, under structural guardrails that prevent AI-induced inaccuracy. The system combines U.S. population segmentation, psychographic projection, and business data ingestion to produce ranked, evidence-based audience intelligence for consumer-facing campaigns.

This document explains how the system is built and why key design decisions were made. The animating principle throughout is the same: use LLMs where they outperform rules — interpretation, contextualization, narrative generation, naming — and never where they don't — matching real data, enforcing thresholds, scoring, deciding what is recommended. The boundary is enforced structurally, not procedurally.

---

## How it works

A company uploads their customer data. MK Intel maps each customer to one of seven baseline U.S. population archetypes, enriches those archetypes with psychological and media behavior signals, scores each archetype against a campaign objective, and produces a ranked audience priority list with a structured analytical brief per audience.

The pipeline has three layers:

1. **Societal baseline** — seven archetypes derived from U.S. census and survey data, built once and reused across all clients
2. **Business data ingestion** — client data normalized, mapped to archetypes, and scored for coverage
3. **Audience analysis** — objectives defined, archetypes scored and ranked, reports generated

---

## The methodological thesis: projection over imputation

This is the design choice that does the most analytical work in the system, and it deserves to be stated explicitly before the data sources are described.

The system needs psychological and media-behavior signals on every adult in the U.S. population. Two large public surveys carry these signals — GSS for psychological traits, Pew NPORS for media behavior — but neither shares identifiers with the structural population frame (ACS PUMS), so direct merge is impossible. Two responses are possible:

- **Imputation** would invent a point estimate for each individual — "this 34-year-old in this ZIP code holds these specific beliefs" — by training a predictor on the survey respondents and applying it to the ACS frame. This is convenient and produces clean per-individual fields, but the per-individual claim is fictional. The model is reading off the modal beliefs of demographically similar people and asserting them as facts about a specific person.

- **Projection** infers a probability distribution instead. For each ACS adult, traits are not values but distributions — "among demographically similar respondents, X% hold this view." Aggregated to the cluster level, this produces psychological and media profiles that represent the likely attitudinal distribution of each segment, with the per-individual fiction removed.

MK Intel uses projection. Cluster-level psychological signals on each archetype are aggregated probability distributions over demographically matched survey respondents, not point estimates assigned to individuals. The distinction matters for both honesty (the system does not claim to know what a specific person believes) and for accuracy (cluster-level distributions are the correct unit of inference for population-level targeting decisions).

Direction is preserved alongside magnitude — being distinctly *non*-X on a trait is as informative for targeting as being distinctly X. Trait labels stripped of direction are ambiguous and produce incorrect LLM interpretations downstream, so the direction is carried through the entire pipeline into the prompts that generate persuasion narratives.

---

## Societal baseline pipeline

### Data sources

The baseline layer integrates three public datasets:

**ACS PUMS (American Community Survey Public Use Microdata Sample)**
The structural foundation. Approximately 15.9M individual person records with demographic, economic, and housing variables. Used at the individual level — not aggregated summary tables — to preserve the full joint distribution of attributes needed for segmentation. Person weights (`PWGTP`) are applied throughout to ensure population representativeness.

**GSS (General Social Survey)**
The psychological layer. The only recurring nationally representative U.S. survey that simultaneously covers ideology, party alignment, religiosity, life satisfaction, and media engagement. GSS respondents cannot be directly merged with ACS records, so psychological traits are projected onto ACS adults via demographic cell matching with hierarchical fallback (see "The methodological thesis" above).

**Pew NPORS (News Platforms and Online Revenue Sources)**
The media behavior layer. Platform-level usage data for YouTube, Facebook, Instagram, TikTok, WhatsApp, Reddit, Snapchat, and others. Projected onto ACS adults using the same methodology as GSS.

### Segmentation: K-Prototypes clustering

The ACS adult population is clustered using K-Prototypes — an algorithm designed for mixed categorical and numeric data. K-Means requires numeric features only; K-Modes drops numeric features; K-Prototypes handles both natively.

**The individual is the targeting unit.** This is the foundational design principle. MK Intel targets people, not households. Clustering features are all individual-level: age, sex, race/ethnicity, education, employment status, individual income, marital status, housing tenure, household size, vehicle count, and household type.

Household income is retained as a contextual descriptor on each archetype card — it surfaces meaningful signals like multi-earner households or economic dependency — but it does not drive segmentation.

**k=7 was selected** based on balance (imbalance ratio 4.10, best of candidates) and cost reduction. All seven clusters represent at least 5.4% of the U.S. adult population, ensuring every archetype is a substantial and targetable group.

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

The LLM-generated summaries on each card are descriptive synthesis of the underlying signals — they do not invent traits. The structural and probabilistic data is the source; the LLM produces readable prose. This is the first instance in the system of the "AI under guardrails" pattern: the LLM contextualizes data it is given, but cannot generate or override the underlying signals.

---

## Business data ingestion

### Canonical schema

All incoming business data is normalized to a canonical behavioral schema with six domains: identity (age, income, gender, zip, tenure), behavioral (sessions, activity), transactional (MRR, subscription status, LTV), journey (lifecycle stage, churn risk), engagement (email, push, SMS), and text signals (LLM-extracted, placeholder).

The schema is format-agnostic. A dataset with columns named `"Age"`, `"age_years"`, or `"customer_age"` all map to the canonical `age` field through a three-layer mapping process.

### Column mapping: rules → LLM → analyst

This is the second instance of the "AI under guardrails" pattern, and it makes the principle concrete:

**Layer 1 — Rules:** A synonym dictionary plus rapidfuzz fuzzy matching handles ~70-80% of real-world column names at zero cost. Rules are deterministic and cheap, so they go first.

**Layer 2 — LLM inference:** Unmatched columns are sent to Claude Haiku with sample values. Returns a structured mapping with confidence scores. Cost: <$0.01 per new company. The LLM never sees columns the rules already resolved — it only handles the residual where rules genuinely cannot help.

**Layer 3 — Analyst review:** The full proposed mapping is presented for confirmation. Approved mappings are saved per company and never rerun.

The hierarchy reflects cost and reliability: deterministic rules are free and predictable, the LLM is cheap and probabilistic, the analyst is expensive and authoritative. Each layer handles only what the previous layer cannot, and each layer's output is constrained by the next layer's check.

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

This is the third instance of the "AI under guardrails" pattern. Structural fields (age, income, tenure, education) are always locked from real data — the LLM cannot touch them. The LLM only refines descriptive and contextual fields, and the refinement scope is constrained by what the data actually supports (no refinement at all in Case C, where the data is too weak to anchor the LLM's reasoning). All output stays at population segment level — no price points or tactical predictions, both of which the LLM is bad at.

**Stage 2 — Candidate shortlisting.** For each SOBJ, a rule engine scores each refined TA card on likelihood of performing the desired behavior. SOBJ vocabulary is matched to behavioral signals on the TA card (churn risk, LTV, subscription status, NPS, donation history, attendance, feature adoption, and similar). An LLM fallback handles SOBJs not matched by keyword rules. The top candidates per SOBJ — typically 3-4 — proceed to full TAR generation.

This stage reduces the number of TARs generated from O(TAs × SOBJs) to a manageable shortlist, avoiding expensive generation for implausible combinations. Rule-based scoring is the default; LLM is a fallback only when the rules genuinely cannot match.

### Scoring algorithm

Each (audience archetype, SOBJ) pair is scored across four dimensions:

| Dimension | Weight | What it measures |
|---|---|---|
| Effectiveness | 30% | Can this audience accomplish the objective? |
| Susceptibility | 30% | Will this audience respond to the approach? |
| Lever depth | 25% | How many persuasion levers exist? |
| Accessibility | 15% | Can we reach them through available channels? |

Four hard gates screen out disqualified audiences before scoring begins (insufficient effectiveness rating, ill-defined behavior, no persuasion levers, no usable channels). Failed gates produce a disqualification reason rather than a score.

The composite score is multiplied by an audience size modifier (0.85–1.15, scaled by estimate confidence), producing a final score used to rank all audiences for each SOBJ.

**The scoring algorithm is fully deterministic and runs in Python — no LLM involvement in the scoring decision itself.** The weights reflect deliberate prior beliefs about how the four factors trade off — effectiveness and susceptibility weighted highest because failure on either undermines everything else, lever depth next because more available persuasion levers means more campaign flexibility, accessibility lowest because most channels are workable for most audiences. These priors have not yet been empirically calibrated against labeled campaign outcomes, and that calibration is the next major methodological step before any production use. The dimension breakdown is always included in output so rankings are auditable. This is the fourth instance of the guardrails pattern: the LLM produces the *content* that the scoring algorithm acts on, but the scoring decision itself is rules-based.

### TAR generation

Each Target Audience Report is generated in 8 sequential LLM calls — one per schema section (effectiveness, conditions, persuasion levers, susceptibility, accessibility, narrative and actions, assessment, traceability). Sequential calls allow each section to receive prior sections as context, enabling internally consistent cross-referencing between condition IDs, lever IDs, argument IDs, and action IDs.

Every factual claim in the TAR is source-tagged: `company_data`, `bta_baseline`, `zip_inference`, or `llm_inference`. This makes the evidential basis of every claim visible to the analyst — data-grounded claims can be acted on immediately, `llm_inference` claims flag for validation before scaling. Source-tagging is the system's most important guardrail: it does not prevent the LLM from producing inferred content, but it makes that content visible and labelable as such, so the reader is never asked to take an LLM-inferred claim as fact.

The effectiveness gate (`rating > 2`) is enforced deterministically in Python after parsing — never trusted to the LLM. Gate-failed TARs are saved with a disqualification reason rather than discarded. The LLM is asked to *rate* effectiveness; the *gate* on that rating is structural. This is the difference between letting an LLM judge whether to recommend an audience (unreliable) and letting an LLM produce an input to a deterministic decision rule (reliable).

### Naming conventions

| Entity | ID format | Example |
|---|---|---|
| Baseline archetype | `BTA_XX` | `BTA_06` |
| Company segment | `CSXX_BTA_XX` | `CS01_BTA_06` |
| TAR document | `TAR-SOBJXX-CSXX_BTA_XX` | `TAR-SOBJ01-CS01_BTA_06` |

`CS` (Company Segment) distinguishes business-specific audience clusters from baseline `BTA` archetypes. The cluster ID and BTA ID are both preserved in the CS identifier — they carry different information and both matter downstream.

---

## Key design decisions

**AI under guardrails, not AI-everywhere.** LLMs are used where they outperform rules — interpretation, refinement, narrative generation, naming. They are not used to match real data, enforce thresholds, score audiences, or decide what is recommended. The boundary is enforced structurally: structural fields are locked from real data before any LLM prompt is constructed; the effectiveness gate runs in Python after parsing; the scoring algorithm is fully deterministic; every claim is source-tagged so LLM inference is visible as such.

**Individual over household.** The targeting unit is always a person. Household income is context, not signal.

**Projection over imputation.** Psychological traits are probability distributions from demographically matched survey data, not point estimates invented for individuals without survey responses. (See "The methodological thesis" above.)

**Direction-aware signals.** Being distinctly *non*-X on a trait is as meaningful for targeting as being strongly X — for example, an audience that is distinctly *not* religiously observant is a different and equally identifiable group from one that is. Signal direction must be preserved through the entire pipeline, including into the prompts that generate persuasion narratives, because trait labels stripped of direction are ambiguous and produce incorrect LLM interpretations.

**Rules before LLM.** Column mapping runs rules first (free, instant), LLM second (cheap, slower), analyst third (accurate, requires human). Cost and latency scale with difficulty; deterministic logic handles the common case so the LLM only sees the residual.

**Transparent scoring.** The algorithm produces ranked lists with full dimension breakdowns. Every score is explainable — no black-box outputs.

**Compliance as a first-class concern.** Compliance mode is set at ingestion time and gates are applied automatically throughout. The platform does not rely on analyst recall for compliance rules.

**Pre-filter before generate.** Running TAR generation on every possible (audience, SOBJ) combination is expensive and produces noise. A rule-based pre-filter shortlists only plausible candidates before any generation call. Weak candidates that slip through are handled by the TAR effectiveness gate and scoring algorithm — the pre-filter is deliberately coarse.

**LLM refines, data grounds.** The LLM contextualizes audience profiles for company and product context — it does not override real data signals. Company data beats ZIP inference beats BTA baseline beats LLM speculation. This hierarchy is enforced structurally: fields derived from real data are locked before LLM prompts are constructed, and source-tagging makes the hierarchy visible in every output.
