"""
coverage.py
===========
MK Intel Ingestion — Coverage and Eligibility Scoring

Computes coverage metadata for a single normalized record conforming to
the MK canonical behavioral schema. Called by the normalizer after a
record has been mapped to canonical fields.

This module is pure computation — no I/O, no external calls.
Input: one dict (canonical record) + compliance_mode + sector
Output: one dict (coverage metadata) to be merged into the record

──────────────────────────────────────────────────────────────────
Coverage rules
──────────────────────────────────────────────────────────────────

A field is considered PRESENT if:
    - it is not None
    - it is not an empty string ""
    - it is not an empty list []

Note: False and 0 are considered present — they are informative values.
    mobile_deposit_active = False  →  present (customer does not use it)
    support_tickets_90d   = 0      →  present (no tickets — informative)

Fields excluded from coverage calculation:
    - required=True fields (customer_id) — always present, not informative
    - tier 3 / placeholder domains (text_signals) — not yet implemented
    - sector-tagged fields when sector does not match the ingestion run
      e.g. banking fields excluded for SaaS company coverage scores

Raw/derived field families:
    Some fields are raw/derived pairs that represent the same underlying
    signal. To avoid double-counting, these are treated as a single
    coverage unit — the family is present if ANY member is present.

    Families:
        age family         : age, age_bin
        income family      : income_annual, income_tier
        active_date family : last_active_date, days_since_active
        purchase_date fam. : last_purchase_date, days_since_purchase
        renewal_date fam.  : renewal_date, days_to_renewal
        nps family         : nps_score, nps_tier
        churn family       : churn_risk_score, churn_risk_tier
        feature fam.       : feature_adoption_count, feature_adoption_tier

──────────────────────────────────────────────────────────────────
BTA eligibility
──────────────────────────────────────────────────────────────────

bta_eligible = True when ALL of the following are met:
    1. country is US or null (non-US skips BTA mapping)
       Policy: missing country defaults to US-compatible.
               This is an explicit product decision, not a neutral default.
    2. structural_weight_coverage >= 0.35
       Computed only from structural fields that are USABLE in the
       active compliance mode. Fields excluded by compliance mode
       do not count toward BTA eligibility.

──────────────────────────────────────────────────────────────────
Public API
──────────────────────────────────────────────────────────────────

    compute_coverage(record, compliance_mode, sector)
        Main entry point. Returns a coverage dict for one record.

    get_compliance_excluded_fields(compliance_mode)
        Returns fields excluded from clustering for a given mode.

    is_present(value)
        Returns True if a field value is considered present.
"""

from __future__ import annotations

from typing import Optional


# ── Field families ────────────────────────────────────────────────────────────
#
# Raw/derived pairs that represent the same underlying signal.
# Treated as a single coverage unit — present if ANY member is present.
# Listed as (canonical_name, [all_members]).

FIELD_FAMILIES: list[tuple[str, list[str]]] = [
    ("age_family",          ["age", "age_bin"]),
    ("income_family",       ["income_annual", "income_tier"]),
    ("active_date_family",  ["last_active_date", "days_since_active"]),
    ("purchase_date_family",["last_purchase_date", "days_since_purchase"]),
    ("renewal_date_family", ["renewal_date", "days_to_renewal"]),
    ("nps_family",          ["nps_score", "nps_tier"]),
    ("churn_family",        ["churn_risk_score", "churn_risk_tier"]),
    ("feature_family",      ["feature_adoption_count", "feature_adoption_tier"]),
]

# Flat set of all fields that belong to a family (used to exclude them
# from the individual field list to avoid double-counting)
_FAMILY_MEMBERS: set[str] = {
    member
    for _, members in FIELD_FAMILIES
    for member in members
}


# ── Schema definitions ────────────────────────────────────────────────────────
#
# DOMAIN_FIELDS lists individual fields (non-family members).
# Family representatives are added separately during coverage computation.
# customer_id excluded — required=True, always present, not informative.
# text_signals domain excluded — tier 3 placeholder.

DOMAIN_FIELDS: dict[str, list[str]] = {
    "identity": [
        # age and income are handled as families — not listed here
        "gender",
        "education",
        "marital_status",
        "housing_tenure",
        "zip_code",
        "country",
        "customer_since",
    ],
    "behavioral": [
        # nps, feature_adoption, active_date handled as families
        "sessions_last_7d",
        "sessions_last_30d",
        "sessions_last_90d",
        "support_tickets_total",
        "support_tickets_90d",
        "cancellation_attempts",
        # banking — included only when sector == "banking"
        "digital_banking_sessions_30d",
        "branch_visits_90d",
        "atm_transactions_30d",
        "mobile_deposit_active",
    ],
    "transactional": [
        # purchase_date handled as family
        "subscription_plan",
        "subscription_status",
        "mrr",
        "arr",
        "ltv",
        "total_purchases",
        "purchases_last_30d",
        "purchases_last_90d",
        "avg_order_value",
        "payment_failures_total",
        "discount_usage_pct",
        # e-commerce
        "cart_abandonment_rate",
        "product_categories_purchased",
        "return_rate",
        # banking — included only when sector == "banking"
        "account_type",
        "account_status",
        "credit_score_tier",
        "overdraft_frequency_90d",
        "direct_deposit_active",
        "avg_monthly_balance",
        "product_count",
        "cross_sell_products",
    ],
    "journey": [
        # renewal_date and churn handled as families
        "lifecycle_stage",
        "onboarding_completed",
        "onboarding_completion_pct",
        "upgrades_total",
        "downgrades_total",
        "referrals_made",
        # banking — included only when sector == "banking"
        "delinquency_days",
        "credit_limit_utilization",
        "loan_to_value_ratio",
    ],
    "engagement": [
        "email_open_rate",
        "email_click_rate",
        "push_opt_in",
        "sms_opt_in",
        "preferred_channel",
        "content_categories",
        "last_email_open_date",
        "community_member",
        "reviews_submitted",
        "avg_review_score",
    ],
    # text_signals excluded — tier 3 placeholder, not yet implemented
}

# Which domain each family belongs to
FAMILY_DOMAIN: dict[str, str] = {
    "age_family":           "identity",
    "income_family":        "identity",
    "active_date_family":   "behavioral",
    "purchase_date_family": "transactional",
    "renewal_date_family":  "journey",
    "nps_family":           "behavioral",
    "churn_family":         "journey",
    "feature_family":       "behavioral",
}

# Fields that are sector-specific and their sector tag
FIELD_SECTOR: dict[str, str] = {
    # banking behavioral
    "digital_banking_sessions_30d": "banking",
    "branch_visits_90d":            "banking",
    "atm_transactions_30d":         "banking",
    "mobile_deposit_active":        "banking",
    # banking transactional
    "account_type":                 "banking",
    "account_status":               "banking",
    "credit_score_tier":            "banking",
    "overdraft_frequency_90d":      "banking",
    "direct_deposit_active":        "banking",
    "avg_monthly_balance":          "banking",
    "product_count":                "banking",
    "cross_sell_products":          "banking",
    # banking journey
    "delinquency_days":             "banking",
    "credit_limit_utilization":     "banking",
    "loan_to_value_ratio":          "banking",
    # e-commerce transactional
    "cart_abandonment_rate":        "ecommerce",
    "product_categories_purchased": "ecommerce",
    "return_rate":                  "ecommerce",
}

# Structural fields and their relative weights for BTA mapping.
# These are normalized at runtime — do not need to sum to 1.0.
# NOTE: uses age_bin and income_tier (derived forms) because coverage.py
# is called after normalization, which guarantees bins are populated
# from raw values. If normalization fails, bta_eligible correctly falls.
STRUCTURAL_WEIGHTS: dict[str, float] = {
    "age_bin":        0.40,
    "income_tier":    0.40,
    "housing_tenure": 0.10,
    "education":      0.10,
    "marital_status": 0.10,
}

# Fields excluded from clustering per compliance mode
COMPLIANCE_EXCLUSIONS: dict[str, list[str]] = {
    "standard":   [],
    "banking_us": ["gender", "age_bin", "zip_code", "credit_score_tier"],
    "banking_eu": ["gender", "age_bin", "zip_code", "marital_status", "credit_score_tier"],
    "eu_gdpr":    ["gender", "zip_code"],
}

# Fields that are descriptor-only in ALL compliance modes
# (never clustering inputs regardless of mode)
# Note: dominant_race_eth is a BTA card field, not a customer record field.
# It is intentionally NOT listed here.
DESCRIPTOR_ONLY_ALL_MODES: list[str] = [
    "credit_score_tier",
]

# Confidence tier thresholds
CONFIDENCE_THRESHOLDS = {
    "high":   0.6,
    "medium": 0.3,
}

# Minimum structural weight coverage for bta_eligible
BTA_ELIGIBLE_THRESHOLD = 0.35


# ── Public helpers ────────────────────────────────────────────────────────────

def is_present(value) -> bool:
    """
    Returns True if a field value is considered present.

    A field is present if it is:
        - not None
        - not an empty string
        - not an empty list

    Note: False and 0 are considered present — they carry information.
        mobile_deposit_active = False  →  present
        support_tickets_90d   = 0      →  present

    Args:
        value : any field value from a canonical record.

    Returns:
        bool
    """
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    if isinstance(value, list) and len(value) == 0:
        return False
    return True


def get_compliance_excluded_fields(compliance_mode: str) -> list[str]:
    """
    Returns the list of fields excluded from clustering for a given
    compliance mode. Always includes descriptor-only fields.

    Args:
        compliance_mode : one of standard, banking_us, banking_eu, eu_gdpr.

    Returns:
        Sorted list of field names excluded from clustering inputs.

    Raises:
        ValueError : unknown compliance_mode.
    """
    if compliance_mode not in COMPLIANCE_EXCLUSIONS:
        raise ValueError(
            f"Unknown compliance_mode: '{compliance_mode}'. "
            f"Valid values: {list(COMPLIANCE_EXCLUSIONS.keys())}"
        )

    mode_exclusions  = COMPLIANCE_EXCLUSIONS[compliance_mode]
    all_exclusions   = list(set(mode_exclusions + DESCRIPTOR_ONLY_ALL_MODES))
    return sorted(all_exclusions)


# ── Main coverage computation ─────────────────────────────────────────────────

def compute_coverage(
    record: dict,
    compliance_mode: str = "standard",
    sector: Optional[str] = None,
) -> dict:
    """
    Compute coverage metadata for one normalized canonical record.

    Coverage is computed after normalization. Assumes that:
        - age_bin is populated if age was present
        - income_tier is populated if income_annual was present
        - days_since_* fields are populated if date fields were present

    Args:
        record          : dict conforming to the canonical behavioral schema.
        compliance_mode : active compliance mode.
                          Values: standard, banking_us, banking_eu, eu_gdpr.
                          Default: standard.
        sector          : active sector for this ingestion run.
                          Values: None (general), banking, ecommerce.
                          Sector-tagged fields for other sectors are excluded
                          from the coverage denominator.

    Returns:
        Coverage metadata dict to be merged into the record.

    Raises:
        ValueError : unknown compliance_mode.
    """
    if compliance_mode not in COMPLIANCE_EXCLUSIONS:
        raise ValueError(
            f"Unknown compliance_mode: '{compliance_mode}'. "
            f"Valid values: {list(COMPLIANCE_EXCLUSIONS.keys())}"
        )

    compliance_excluded = get_compliance_excluded_fields(compliance_mode)

    # ── Per-domain coverage ───────────────────────────────────────────────────
    # Build domain → list of (unit_name, is_present) tuples
    # Each unit is either an individual field or a family representative

    domain_units: dict[str, list[bool]] = {d: [] for d in DOMAIN_FIELDS}

    # Individual fields (non-family members)
    for domain, fields in DOMAIN_FIELDS.items():
        eligible = _get_eligible_fields(fields, sector)
        for field in eligible:
            domain_units[domain].append(is_present(record.get(field)))

    # Field families — one unit per family, present if any member present
    for family_name, members in FIELD_FAMILIES:
        domain = FAMILY_DOMAIN[family_name]
        eligible_members = _get_eligible_fields(members, sector)
        if eligible_members:
            family_present = any(
                is_present(record.get(m)) for m in eligible_members
            )
            domain_units[domain].append(family_present)

    # Compute per-domain scores
    domain_scores: dict[str, float] = {}
    total_present  = 0
    total_eligible = 0

    for domain, units in domain_units.items():
        if not units:
            domain_scores[domain] = 0.0
            continue
        present = sum(units)
        domain_scores[domain] = round(present / len(units), 4)
        total_present  += present
        total_eligible += len(units)

    coverage_score = (
        round(total_present / total_eligible, 4)
        if total_eligible > 0 else 0.0
    )

    # ── Structural weight coverage (compliance-aware) ─────────────────────────
    # Only structural fields that are USABLE in the active compliance mode
    # count toward BTA eligibility. Fields excluded by compliance mode
    # do not contribute — ensuring eligibility is consistent with
    # what can actually be used for clustering.

    structural_fields_present = []
    weighted_sum  = 0.0
    total_weight  = 0.0

    for field, weight in STRUCTURAL_WEIGHTS.items():
        if field in compliance_excluded:
            # Field excluded in this compliance mode — skip entirely
            # It does not count toward either numerator or denominator
            continue
        total_weight += weight
        if is_present(record.get(field)):
            structural_fields_present.append(field)
            weighted_sum += weight

    structural_weight_coverage = (
        round(weighted_sum / total_weight, 4)
        if total_weight > 0 else 0.0
    )

    # ── Confidence tier ───────────────────────────────────────────────────────
    if coverage_score >= CONFIDENCE_THRESHOLDS["high"]:
        confidence_tier = "high"
    elif coverage_score >= CONFIDENCE_THRESHOLDS["medium"]:
        confidence_tier = "medium"
    else:
        confidence_tier = "low"

    # ── BTA eligibility ───────────────────────────────────────────────────────
    # Policy: missing country defaults to US-compatible.
    # This is an explicit product decision — MK Intel is US-first.
    # Non-US records are explicitly flagged by the normalizer via country field.
    country  = record.get("country")
    is_us    = (country is None) or (str(country).strip().upper() == "US")
    bta_eligible = (
        is_us and structural_weight_coverage >= BTA_ELIGIBLE_THRESHOLD
    )

    # ── Assemble output ───────────────────────────────────────────────────────
    return {
        "coverage_score":             coverage_score,
        "identity_coverage":          domain_scores.get("identity",      0.0),
        "behavioral_coverage":        domain_scores.get("behavioral",    0.0),
        "transactional_coverage":     domain_scores.get("transactional", 0.0),
        "journey_coverage":           domain_scores.get("journey",       0.0),
        "engagement_coverage":        domain_scores.get("engagement",    0.0),
        "structural_fields_present":  structural_fields_present,
        "structural_weight_coverage": structural_weight_coverage,
        "confidence_tier":            confidence_tier,
        "bta_eligible":               bta_eligible,
        "compliance_mode":            compliance_mode,
        "compliance_excluded_fields": compliance_excluded,
    }


# ── Internal helpers ──────────────────────────────────────────────────────────

def _get_eligible_fields(fields: list[str], sector: Optional[str]) -> list[str]:
    """
    Filter a field list to those eligible for coverage calculation
    given the active sector.

    A field is eligible if:
        - it has no sector tag (universal field), OR
        - its sector tag matches the active sector

    Args:
        fields : list of field names.
        sector : active sector (None, banking, ecommerce).

    Returns:
        Filtered list of eligible field names.
    """
    eligible = []
    for field in fields:
        field_sector = FIELD_SECTOR.get(field)
        if field_sector is None:
            eligible.append(field)
        elif field_sector == sector:
            eligible.append(field)
    return eligible