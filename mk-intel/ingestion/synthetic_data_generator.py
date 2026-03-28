"""
synthetic_data_generator.py
===========================
MK Intel Ingestion — Synthetic Data Generator

Generates controlled synthetic datasets for testing and demo purposes.
All scenarios use realistic company-style column names (not canonical names)
to exercise the full column mapping pipeline.

──────────────────────────────────────────────────────────────────
Two generation modes
──────────────────────────────────────────────────────────────────

Mode 1 — Named scenarios (pre-defined, deterministic):
    Generate a documented scenario with a fixed random seed.
    Same scenario always produces the same data.

    Available scenarios:
        saas_standard           Happy path SaaS. 1000 customers, 4 clusters.
        saas_churn_focus        Retention use case. High churn risk segment.
        ecommerce_standard      E-commerce. Cart abandon, returns, categories.
        banking_us              US banking. Banking fields, banking_us compliance.
        mixed_coverage          Sparse data. 30% customers have 1-2 fields only.
        non_us_mixed            International. 40% non-US customers.
        edge_cases              All edge cases. Shorthand values, bad data.
        small_company           Small dataset. 150 customers, % threshold test.
        no_behavioral_features  No behavioral fields. Tests clustering fallback.
        zip_enrichment_demo     ZIP enrichment validation. 1500 customers, 3 cohorts.

Mode 2 — Custom generation:
    Caller specifies parameters and gets a tailored dataset.

──────────────────────────────────────────────────────────────────
Column naming strategy
──────────────────────────────────────────────────────────────────

Datasets use realistic company-style column names by default:
    age          → CustomerAge
    mrr          → MonthlyRevenue
    ltv          → CustomerLifetimeValue
    nps_score    → NPS
    ...

This tests the full column mapping pipeline (rules → LLM → analyst).
Pass use_canonical_names=True to skip mapping and use canonical names directly.

──────────────────────────────────────────────────────────────────
Public API
──────────────────────────────────────────────────────────────────

    generate_scenario(scenario_name, output_path, format, seed)
        Generate a named scenario dataset.

    generate_custom(n_customers, sector, coverage_level, ...)
        Generate a custom dataset with specified parameters.

    list_scenarios()
        Print descriptions of all available scenarios.

    SCENARIO_REGISTRY
        Dict of all scenarios with metadata.
"""

from __future__ import annotations

import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# ── Realistic column name mapping ─────────────────────────────────────────────
# Maps canonical field names to realistic company-style column names.
# Deliberately varied to test the mapping layer thoroughly.

REALISTIC_COLUMN_NAMES: dict[str, str] = {
    # identity
    "customer_id":       "CustomerID",
    "age":               "CustomerAge",
    "gender":            "Gender",
    "income_annual":     "AnnualIncome",
    "education":         "EducationLevel",
    "marital_status":    "MaritalStatus",
    "housing_tenure":    "HomeOwnership",
    "zip_code":          "ZipCode",
    "country":           "Country",
    "customer_since":    "JoinDate",

    # behavioral
    "sessions_last_7d":       "LoginsLast7Days",
    "sessions_last_30d":      "MonthlyLogins",
    "sessions_last_90d":      "QuarterlyLogins",
    "last_active_date":       "LastLoginDate",
    "feature_adoption_count": "FeaturesUsed",
    "nps_score":              "NPS",
    "support_tickets_total":  "TotalSupportTickets",
    "support_tickets_90d":    "SupportTickets90Days",
    "cancellation_attempts":  "CancelAttempts",

    # transactional
    "subscription_plan":    "PlanName",
    "subscription_status":  "SubscriptionStatus",
    "mrr":                  "MonthlyRevenue",
    "arr":                  "AnnualRevenue",
    "ltv":                  "CustomerLifetimeValue",
    "total_purchases":      "TotalOrders",
    "purchases_last_30d":   "OrdersLast30Days",
    "avg_order_value":      "AvgOrderValue",
    "last_purchase_date":   "LastOrderDate",
    "payment_failures_total": "FailedPayments",
    "discount_usage_pct":   "DiscountUsageRate",

    # e-commerce
    "cart_abandonment_rate":       "CartAbandonRate",
    "product_categories_purchased": "ProductCategories",
    "return_rate":                  "ReturnRate",

    # journey
    "lifecycle_stage":         "CustomerStage",
    "churn_risk_score":        "ChurnProbability",
    "renewal_date":            "NextRenewalDate",
    "onboarding_completed":    "OnboardingDone",
    "onboarding_completion_pct": "OnboardingProgress",
    "upgrades_total":          "PlanUpgrades",
    "downgrades_total":        "PlanDowngrades",
    "referrals_made":          "ReferralCount",

    # engagement
    "email_open_rate":    "EmailOpenRate",
    "email_click_rate":   "EmailClickRate",
    "push_opt_in":        "PushNotificationsEnabled",
    "sms_opt_in":         "SMSOptIn",
    "community_member":   "CommunityMember",
    "reviews_submitted":  "ReviewsWritten",
    "avg_review_score":   "AvgRating",

    # banking
    "account_type":              "AccountType",
    "account_status":            "AccountStatus",
    "overdraft_frequency_90d":   "Overdrafts90Days",
    "direct_deposit_active":     "DirectDepositActive",
    "avg_monthly_balance":       "AvgBalance",
    "product_count":             "ProductsHeld",
    "delinquency_days":          "DaysPastDue",
    "credit_limit_utilization":  "CreditUtilization",
    "loan_to_value_ratio":       "LTVRatio",
    "digital_banking_sessions_30d": "OnlineBankingLogins30Days",
    "branch_visits_90d":         "BranchVisits90Days",
    "mobile_deposit_active":     "MobileDepositActive",
}


# ── Value pools for realistic data generation ─────────────────────────────────

AGES           = list(range(18, 75))
GENDERS        = ["Male", "Female", "Male", "Female", "Other"]  # weighted
EDUCATION      = ["hs_diploma", "some college", "Bachelor", "Graduate", "hs or less"]
MARITAL        = ["married", "single", "divorced", "married", "married"]
TENURE         = ["Owner", "Renter", "Owner", "Owner", "Renter"]
COUNTRIES_US   = ["US"] * 20
COUNTRIES_INTL = ["US"] * 12 + ["GB", "CA", "DE", "FR", "IT", "AU", "MX", "BR"]
PLANS          = ["Basic", "Professional", "Enterprise", "Starter", "Growth"]
STATUSES       = ["active", "active", "active", "cancelled", "paused", "trial"]
LIFECYCLES     = ["new", "growing", "mature", "mature", "at_risk", "churned"]
CATEGORIES     = ["apparel", "electronics", "home", "beauty", "sports", "books"]
ACCOUNT_TYPES  = ["checking", "savings", "credit", "mortgage", "checking"]

# US ZIP codes (sample)
ZIP_CODES = [
    "10001", "90210", "60601", "77001", "85001",
    "19101", "98101", "30301", "02101", "94102",
]

# ── ZIP cohorts for zip_enrichment_demo scenario ──────────────────────────────
# Verified against ACS 2022 5-year ZCTA data (zcta_enrichment.parquet).
# Cohort design anchored to BTA_00 (dominant_household_income_tier=50-99k,
# dominant_race_eth=White) — the most common BTA match for this age/income range.
#
# ZIP enrichment uses ZCTA household income (ACS median HH income at ZIP level).
# Confidence validation compares zip_inferred_income_tier against
# dominant_household_income_tier on the matched BTA card.
#
# Case A — Full alignment: White ZIP, 50-99k HH income → matches BTA_00 HH tier
ZIP_COHORT_A = [
    "01001",  # Agawam MA        — White 0.85, 50-99k
    "01005",  # Barre MA         — White 0.93, 50-99k
    "01008",  # Blandford MA     — White 0.95, 50-99k
    "01009",  # Bondsville MA    — White 1.00, 50-99k
    "01010",  # Brimfield MA     — White 0.81, 50-99k
]

# Case B — Race diverges: Hispanic ZIP, 50-99k HH income
# HH income matches BTA_00 HH tier but ZIP race diverges from BTA_00 (White)
ZIP_COHORT_B = [
    "00966",  # Puerto Rico      — Hispanic 0.98, 50-99k
    "00968",  # Puerto Rico      — Hispanic 0.98, 50-99k
    "00969",  # Puerto Rico      — Hispanic 0.99, 50-99k
    "01841",  # Lawrence MA      — Hispanic 0.86, 50-99k
    "01843",  # Lawrence MA      — Hispanic 0.73, 50-99k
]

# Case C — Income conflict: Hispanic ZIP, 20-49k HH income
# Conflicts with BTA_00 HH tier (50-99k) → low confidence → LLM archetype
ZIP_COHORT_C = [
    "00602",  # Puerto Rico      — Hispanic 0.95, 20-49k
    "00612",  # Puerto Rico      — Hispanic 0.99, 20-49k
    "00622",  # Puerto Rico      — Hispanic 0.99, 20-49k
]


# ── Scenario registry ─────────────────────────────────────────────────────────

SCENARIO_REGISTRY: dict[str, dict] = {
    "saas_standard": {
        "description": "Happy path SaaS demo. 1000 US customers, good coverage, "
                       "4 natural behavioral clusters, clean data throughout.",
        "n_customers":    1000,
        "sector":         None,
        "coverage_level": "high",
        "countries":      COUNTRIES_US,
        "seed":           42,
        "include_banking":    False,
        "include_ecommerce":  False,
        "include_edge_cases": False,
        "sparse_pct":         0.0,
    },
    "saas_churn_focus": {
        "description": "Retention use case. 800 customers with a prominent "
                       "high-churn-risk segment and varied lifecycle stages.",
        "n_customers":    800,
        "sector":         None,
        "coverage_level": "high",
        "countries":      COUNTRIES_US,
        "seed":           99,
        "include_banking":    False,
        "include_ecommerce":  False,
        "include_edge_cases": False,
        "sparse_pct":         0.0,
        "churn_skew":         True,
    },
    "ecommerce_standard": {
        "description": "E-commerce demo. 1200 customers with cart abandonment, "
                       "return rates, and product categories.",
        "n_customers":    1200,
        "sector":         "ecommerce",
        "coverage_level": "high",
        "countries":      COUNTRIES_US,
        "seed":           17,
        "include_banking":    False,
        "include_ecommerce":  True,
        "include_edge_cases": False,
        "sparse_pct":         0.0,
    },
    "banking_us": {
        "description": "US banking B2C demo. 900 customers with banking-specific "
                       "fields. Tests banking_us compliance mode.",
        "n_customers":    900,
        "sector":         "banking",
        "coverage_level": "high",
        "countries":      COUNTRIES_US,
        "seed":           55,
        "include_banking":    True,
        "include_ecommerce":  False,
        "include_edge_cases": False,
        "sparse_pct":         0.0,
        "compliance_mode":    "banking_us",
    },
    "mixed_coverage": {
        "description": "Sparse data test. 600 customers where 30% have only "
                       "1-2 fields populated. Tests graceful degradation.",
        "n_customers":    600,
        "sector":         None,
        "coverage_level": "mixed",
        "countries":      COUNTRIES_US,
        "seed":           77,
        "include_banking":    False,
        "include_ecommerce":  False,
        "include_edge_cases": False,
        "sparse_pct":         0.30,
    },
    "non_us_mixed": {
        "description": "International dataset. 500 customers with 40% non-US. "
                       "Tests country detection and BTA skip logic.",
        "n_customers":    500,
        "sector":         None,
        "coverage_level": "high",
        "countries":      COUNTRIES_INTL,
        "seed":           33,
        "include_banking":    False,
        "include_ecommerce":  False,
        "include_edge_cases": False,
        "sparse_pct":         0.0,
    },
    "edge_cases": {
        "description": "All edge cases. Under-18 ages, negative values, "
                       "out-of-range NPS, duplicate IDs, shorthand values "
                       "('4m', '1.2k', '45%'), mixed encodings.",
        "n_customers":    200,
        "sector":         None,
        "coverage_level": "high",
        "countries":      COUNTRIES_US,
        "seed":           13,
        "include_banking":    False,
        "include_ecommerce":  False,
        "include_edge_cases": True,
        "sparse_pct":         0.0,
    },
    "small_company": {
        "description": "Small dataset. 150 customers. Tests percentage-based "
                       "cell threshold behavior for small companies.",
        "n_customers":    150,
        "sector":         None,
        "coverage_level": "medium",
        "countries":      COUNTRIES_US,
        "seed":           88,
        "include_banking":    False,
        "include_ecommerce":  False,
        "include_edge_cases": False,
        "sparse_pct":         0.10,
    },
    "no_behavioral_features": {
        "description": "No behavioral fields. 400 customers with only identity "
                       "and basic subscription data. Tests clustering fallback "
                       "when < 2 Tier-1 features are available.",
        "n_customers":    400,
        "sector":         None,
        "coverage_level": "identity_only",
        "countries":      COUNTRIES_US,
        "seed":           66,
        "include_banking":    False,
        "include_ecommerce":  False,
        "include_edge_cases": False,
        "sparse_pct":         0.0,
    },
    "zip_enrichment_demo": {
        "description": "ZIP enrichment validation demo. 1,500 US SaaS customers "
                       "across three deliberate cohorts anchored to BTA_00 "
                       "(dominant_household_income_tier=50-99k, White): "
                       "Cohort A (500) full alignment (White ZIP, 50-99k HH income → Case A high); "
                       "Cohort B (500) race diverges (Hispanic ZIP, 50-99k HH income → Case B medium); "
                       "Cohort C (500) income conflict (Hispanic ZIP, 20-49k HH income → Case C low).",
        "n_customers":    1500,
        "sector":         None,
        "coverage_level": "high",
        "countries":      COUNTRIES_US,
        "seed":           123,
        "include_banking":    False,
        "include_ecommerce":  False,
        "include_edge_cases": False,
        "sparse_pct":         0.0,
        "zip_enrichment_demo": True,
    },
}


# ── Core generator ────────────────────────────────────────────────────────────

def _generate_base_record(
    i: int,
    rng: random.Random,
    np_rng: np.random.Generator,
    config: dict,
) -> dict:
    """
    Generate one customer record in canonical field names.
    Converted to realistic names by the caller.
    """
    coverage_level = config.get("coverage_level", "high")
    churn_skew     = config.get("churn_skew", False)
    countries      = config.get("countries", COUNTRIES_US)

    # ── Identity ──────────────────────────────────────────────────────────────
    age    = rng.choice(AGES)
    gender = rng.choice(GENDERS)
    record = {
        "customer_id":    f"CUST_{i:06d}",
        "age":            age,
        "gender":         gender,
        "income_annual":  round(np_rng.normal(65000, 25000)),
        "education":      rng.choice(EDUCATION),
        "marital_status": rng.choice(MARITAL),
        "housing_tenure": rng.choice(TENURE),
        "zip_code":       rng.choice(ZIP_CODES),
        "country":        rng.choice(countries),
        "customer_since": (
            date.today() - timedelta(days=rng.randint(30, 1460))
        ).isoformat(),
    }

    if coverage_level == "identity_only":
        # Only add subscription status and plan — no behavioral fields
        record["subscription_plan"]   = rng.choice(PLANS)
        record["subscription_status"] = rng.choice(STATUSES)
        return record

    if coverage_level == "medium":
        # Behavioral fields present but journey and engagement ~50% dropped.
        # Simulates a company that has product usage data but limited
        # lifecycle and comms data — common for early-stage SaaS companies.
        _MEDIUM_DROP_JOURNEY     = 0.50  # probability of dropping each journey field
        _MEDIUM_DROP_ENGAGEMENT  = 0.50  # probability of dropping each engagement field
        _medium_drop_journey     = rng.random() < _MEDIUM_DROP_JOURNEY
        _medium_drop_engagement  = rng.random() < _MEDIUM_DROP_ENGAGEMENT

    # ── Behavioral ────────────────────────────────────────────────────────────
    days_since = rng.randint(0, 180)
    sessions   = max(0, int(np_rng.normal(15, 8))) if days_since < 60 else rng.randint(0, 5)

    # Churn skew: ~30% of customers are high-risk
    if churn_skew and rng.random() < 0.30:
        churn_score  = round(rng.uniform(0.65, 0.99), 4)
        days_since   = rng.randint(30, 120)
        sessions     = rng.randint(0, 5)
        lifecycle    = rng.choice(["at_risk", "churned"])
        nps          = rng.randint(0, 5)
    else:
        churn_score = round(rng.uniform(0.0, 0.45), 4)
        lifecycle   = rng.choice(LIFECYCLES)
        nps         = rng.randint(5, 10)

    record.update({
        "sessions_last_7d":       max(0, sessions // 4),
        "sessions_last_30d":      sessions,
        "sessions_last_90d":      sessions * 3 + rng.randint(0, 10),
        "last_active_date":       (
            date.today() - timedelta(days=days_since)
        ).isoformat(),
        "feature_adoption_count": rng.randint(0, 12),
        "nps_score":              nps,
        "support_tickets_total":  rng.randint(0, 20),
        "support_tickets_90d":    rng.randint(0, 5),
        "cancellation_attempts":  rng.randint(0, 3),
    })

    # ── Transactional ─────────────────────────────────────────────────────────
    plan   = rng.choice(PLANS)
    status = rng.choice(STATUSES)
    mrr    = round(rng.uniform(9.99, 499.99), 2)

    record.update({
        "subscription_plan":     plan,
        "subscription_status":   status,
        "mrr":                   mrr,
        "arr":                   round(mrr * 12, 2),
        "ltv":                   round(mrr * rng.uniform(6, 36), 2),
        "total_purchases":       rng.randint(1, 50),
        "purchases_last_30d":    rng.randint(0, 5),
        "avg_order_value":       round(rng.uniform(20, 500), 2),
        "last_purchase_date":    (
            date.today() - timedelta(days=rng.randint(0, 90))
        ).isoformat(),
        "payment_failures_total": rng.randint(0, 4),
        "discount_usage_pct":    round(rng.uniform(0, 0.6), 4),
    })

    # ── Journey ───────────────────────────────────────────────────────────────
    record.update({
        "lifecycle_stage":          lifecycle,
        "churn_risk_score":         churn_score,
        "renewal_date":             (
            date.today() + timedelta(days=rng.randint(1, 365))
        ).isoformat(),
        "onboarding_completed":     rng.choice([True, True, True, False]),
        "onboarding_completion_pct": round(rng.uniform(0.5, 1.0), 4),
        "upgrades_total":           rng.randint(0, 3),
        "downgrades_total":         rng.randint(0, 2),
        "referrals_made":           rng.randint(0, 5),
    })

    # ── Engagement ────────────────────────────────────────────────────────────
    record.update({
        "email_open_rate":  round(rng.uniform(0.05, 0.55), 4),
        "email_click_rate": round(rng.uniform(0.01, 0.20), 4),
        "push_opt_in":      rng.choice([True, True, False]),
        "sms_opt_in":       rng.choice([True, False, False]),
        "community_member": rng.choice([True, False, False, False]),
        "reviews_submitted": rng.randint(0, 8),
        "avg_review_score": round(rng.uniform(2.5, 5.0), 1),
    })

    # ── Medium coverage: drop journey and engagement fields ───────────────────
    if coverage_level == "medium":
        if rng.random() < 0.50:
            journey_fields = [
                "lifecycle_stage", "churn_risk_score", "renewal_date",
                "onboarding_completed", "onboarding_completion_pct",
                "upgrades_total", "downgrades_total", "referrals_made",
            ]
            for field in journey_fields:
                record.pop(field, None)
        if rng.random() < 0.50:
            engagement_fields = [
                "email_open_rate", "email_click_rate", "push_opt_in",
                "sms_opt_in", "community_member", "reviews_submitted",
                "avg_review_score",
            ]
            for field in engagement_fields:
                record.pop(field, None)

    return record


def _add_ecommerce_fields(record: dict, rng: random.Random) -> dict:
    """Add e-commerce specific fields to a record."""
    record["cart_abandonment_rate"] = round(rng.uniform(0.1, 0.8), 4)
    record["return_rate"]           = round(rng.uniform(0.0, 0.3), 4)
    n_cats = rng.randint(1, 4)
    cats   = rng.sample(CATEGORIES, n_cats)
    record["product_categories_purchased"] = ",".join(cats)
    return record


def _add_banking_fields(record: dict, rng: random.Random) -> dict:
    """Add banking specific fields to a record."""
    record["account_type"]               = rng.choice(ACCOUNT_TYPES)
    record["account_status"]             = rng.choice(
        ["active", "active", "active", "dormant", "delinquent"]
    )
    record["overdraft_frequency_90d"]    = rng.randint(0, 5)
    record["direct_deposit_active"]      = rng.choice([True, True, False])
    record["avg_monthly_balance"]        = round(rng.uniform(500, 25000), 2)
    record["product_count"]              = rng.randint(1, 6)
    record["delinquency_days"]           = rng.randint(0, 30)
    record["credit_limit_utilization"]   = round(rng.uniform(0.0, 0.95), 4)
    record["loan_to_value_ratio"]        = round(rng.uniform(0.4, 1.2), 4)
    record["digital_banking_sessions_30d"] = rng.randint(0, 25)
    record["branch_visits_90d"]          = rng.randint(0, 6)
    record["mobile_deposit_active"]      = rng.choice([True, True, False])
    return record


def _apply_sparsity(record: dict, rng: random.Random, sparse_pct: float) -> dict:
    """
    Randomly null out most fields for a sparse record.
    Simulates customers where the company has very little data.
    """
    if rng.random() > sparse_pct:
        return record  # not a sparse record

    # Keep only customer_id + 1-2 random fields
    keep = {"customer_id"}
    optional_fields = [k for k in record if k != "customer_id"]
    keep_extra = rng.sample(optional_fields, min(2, len(optional_fields)))
    keep.update(keep_extra)

    return {k: v for k, v in record.items() if k in keep}


def _inject_edge_cases(records: list[dict], rng: random.Random) -> list[dict]:
    """
    Inject controlled edge cases into a record set.

    Edge cases injected:
        - Under-18 age (rows 0-4)
        - Negative MRR (rows 5-9)
        - NPS out of range (rows 10-14)
        - Duplicate customer IDs (rows 15-19 duplicate rows 0-4)
        - Shorthand values: "4m" for days field (rows 20-24)
        - Shorthand magnitudes: "1.2k" for MRR (rows 25-29)
        - Percentage strings: "45%" for rate fields (rows 30-34)
        - Empty strings in categorical fields (rows 35-39)
        - Mixed date formats (rows 40-44)
        - Very high values (rows 45-49)
    """
    n = len(records)

    def safe_idx(i):
        return i % n

    # Under-18 ages
    for i in range(min(5, n)):
        records[safe_idx(i)]["age"] = rng.randint(10, 17)

    # Negative MRR
    for i in range(5, min(10, n)):
        records[safe_idx(i)]["mrr"] = -abs(records[safe_idx(i)].get("mrr", 50))

    # NPS out of range
    for i in range(10, min(15, n)):
        records[safe_idx(i)]["nps_score"] = rng.choice([-1, 11, 15, 100])

    # Duplicate customer IDs
    for i in range(15, min(20, n)):
        records[safe_idx(i)]["customer_id"] = records[safe_idx(i - 15)]["customer_id"]

    # Shorthand time values
    for i in range(20, min(25, n)):
        records[safe_idx(i)]["last_active_date"] = None
        records[safe_idx(i)]["days_since_active"] = rng.choice(
            ["4m", "2w", "90d", "1y", "3m"]
        )

    # Shorthand magnitude values
    for i in range(25, min(30, n)):
        records[safe_idx(i)]["mrr"] = rng.choice(
            ["1.2k", "2.5k", "500", "0.8k", "3k"]
        )
        records[safe_idx(i)]["ltv"] = rng.choice(
            ["10k", "25k", "5.5k", "2k", "50k"]
        )

    # Percentage strings
    for i in range(30, min(35, n)):
        records[safe_idx(i)]["discount_usage_pct"] = rng.choice(
            ["45%", "20%", "0%", "75%", "10.5%"]
        )
        records[safe_idx(i)]["email_open_rate"] = rng.choice(
            ["32%", "18%", "55%", "5%"]
        )
        records[safe_idx(i)]["churn_risk_score"] = rng.choice(
            ["70%", "15%", "90%", "45%"]
        )

    # Empty strings in categorical fields
    for i in range(35, min(40, n)):
        records[safe_idx(i)]["subscription_status"] = rng.choice(
            ["", " ", "N/A", "unknown", "TBD"]
        )

    # Mixed date formats
    date_formats = [
        "01/15/2023", "2023-01-15", "Jan 15 2023",
        "15-01-2023", "2023/01/15", "January 15, 2023",
    ]
    for i in range(40, min(45, n)):
        records[safe_idx(i)]["customer_since"] = rng.choice(date_formats)

    # Very high values
    for i in range(45, min(50, n)):
        records[safe_idx(i)]["ltv"]            = round(rng.uniform(50000, 500000), 2)
        records[safe_idx(i)]["total_purchases"] = rng.randint(500, 5000)

    return records


def _apply_zip_enrichment_cohort(
    record: dict,
    i: int,
    n_total: int,
    rng: random.Random,
) -> dict:
    """
    Override ZIP code and demographic fields to produce deliberate
    ZIP enrichment validation cases for the zip_enrichment_demo scenario.

    Splits customers into three equal cohorts:
        Cohort A (first third)  — full alignment: White ZIP, 50-99k HH income
        Cohort B (middle third) — race diverges: Hispanic ZIP, 50-99k HH income
        Cohort C (last third)   — income conflict: Hispanic ZIP, 20-49k HH income

    Each cohort is designed to trigger a specific BTA confidence validation
    case (A=high, B=medium, C=low/LLM archetype) when ZIP enrichment runs.

    Cohort design anchored to BTA_00 (dominant_household_income_tier=50-99k,
    dominant_race_eth=White). ZIP enrichment compares zip_inferred_income_tier
    (ZCTA household income) against dominant_household_income_tier on the BTA.
    """
    cohort_size = n_total // 3

    if i < cohort_size:
        # ── Cohort A — Full alignment ─────────────────────────────────────────
        # White-dominant ZIP, 50-99k HH income → matches BTA_00 on both
        # income AND race → Case A (high confidence)
        record["zip_code"]       = rng.choice(ZIP_COHORT_A)
        record["age"]            = rng.randint(35, 64)
        record["income_annual"]  = round(rng.uniform(20_000, 49_000))
        record["housing_tenure"] = "Owner"
        record["gender"]         = rng.choice(["Male", "Female"])

    elif i < cohort_size * 2:
        # ── Cohort B — Race diverges ──────────────────────────────────────────
        # Hispanic-dominant ZIP, 50-99k HH income → income matches BTA_00
        # but ZIP race diverges from BTA_00 dominant (White) → Case B (medium)
        record["zip_code"]       = rng.choice(ZIP_COHORT_B)
        record["age"]            = rng.randint(35, 44)
        record["income_annual"]  = round(rng.uniform(20_000, 49_000))
        record["housing_tenure"] = rng.choice(["Owner", "Renter"])
        record["gender"]         = rng.choice(["Male", "Female"])

    else:
        # ── Cohort C — Income conflict ────────────────────────────────────────
        # Hispanic-dominant ZIP, 20-49k HH income → conflicts with BTA_00
        # HH tier (50-99k) → Case C (low confidence) → LLM archetype invoked
        record["zip_code"]       = rng.choice(ZIP_COHORT_C)
        record["age"]            = rng.randint(25, 54)
        record["income_annual"]  = round(rng.uniform(20_000, 49_000))
        record["housing_tenure"] = "Owner"
        record["gender"]         = rng.choice(["Male", "Female"])

    return record


def _rename_columns(
    df: pd.DataFrame,
    use_canonical_names: bool = False,
) -> pd.DataFrame:
    """
    Rename canonical field names to realistic company-style names.
    Preserves any fields not in the mapping as-is.
    """
    if use_canonical_names:
        return df

    rename_map = {
        canonical: realistic
        for canonical, realistic in REALISTIC_COLUMN_NAMES.items()
        if canonical in df.columns
    }
    return df.rename(columns=rename_map)


def _save_dataframe(
    df: pd.DataFrame,
    output_path: Path,
    fmt: str,
) -> Path:
    """Save DataFrame to the specified format."""
    fmt = fmt.lower()
    suffix_map = {
        "csv":     ".csv",
        "json":    ".json",
        "xlsx":    ".xlsx",
        "parquet": ".parquet",
        "tsv":     ".tsv",
    }
    suffix = suffix_map.get(fmt, ".csv")

    # Ensure correct extension
    output_path = output_path.with_suffix(suffix)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        df.to_csv(output_path, index=False)
    elif fmt == "tsv":
        df.to_csv(output_path, sep="\t", index=False)
    elif fmt == "json":
        df.to_json(output_path, orient="records", indent=2)
    elif fmt == "xlsx":
        df.to_excel(output_path, index=False)
    elif fmt == "parquet":
        df.to_parquet(output_path, index=False)
    else:
        raise ValueError(f"Unsupported format: '{fmt}'. "
                         f"Use: csv, tsv, json, xlsx, parquet")

    return output_path


# ── Public API ────────────────────────────────────────────────────────────────

def generate_scenario(
    scenario_name:       str,
    output_path:         Optional[Path] = None,
    fmt:                 str = "csv",
    use_canonical_names: bool = False,
    seed:                Optional[int] = None,
) -> pd.DataFrame:
    """
    Generate a named scenario dataset.

    Args:
        scenario_name       : name of the scenario (see SCENARIO_REGISTRY)
        output_path         : path to save the file (without extension).
                              If None, returns DataFrame without saving.
        fmt                 : output format: csv, tsv, json, xlsx, parquet
        use_canonical_names : if True, use canonical field names (skip mapping test)
        seed                : override the scenario's default random seed

    Returns:
        DataFrame of generated records.

    Raises:
        ValueError : unknown scenario name.
    """
    if scenario_name not in SCENARIO_REGISTRY:
        raise ValueError(
            f"Unknown scenario: '{scenario_name}'. "
            f"Available: {list(SCENARIO_REGISTRY.keys())}"
        )

    config = SCENARIO_REGISTRY[scenario_name].copy()
    effective_seed = seed if seed is not None else config["seed"]

    rng    = random.Random(effective_seed)
    np_rng = np.random.default_rng(effective_seed)

    n          = config["n_customers"]
    sparse_pct = config.get("sparse_pct", 0.0)

    print(f"[synthetic] Generating scenario '{scenario_name}'...")
    print(f"[synthetic] {config['description']}")
    print(f"[synthetic] n={n}, seed={effective_seed}, format={fmt}")

    # ── Generate records ──────────────────────────────────────────────────────
    records = []
    for i in range(n):
        record = _generate_base_record(i, rng, np_rng, config)

        if config.get("include_ecommerce"):
            record = _add_ecommerce_fields(record, rng)

        if config.get("include_banking"):
            record = _add_banking_fields(record, rng)

        if config.get("zip_enrichment_demo"):
            record = _apply_zip_enrichment_cohort(record, i, n, rng)

        if sparse_pct > 0:
            record = _apply_sparsity(record, rng, sparse_pct)

        records.append(record)

    # Inject edge cases after generation (needs full record set)
    if config.get("include_edge_cases"):
        records = _inject_edge_cases(records, rng)

    # ── Build DataFrame ───────────────────────────────────────────────────────
    df = pd.DataFrame(records)
    df = _rename_columns(df, use_canonical_names)

    print(f"[synthetic] ✓ Generated {len(df):,} rows × {len(df.columns)} columns")
    print(f"[synthetic] Columns: {list(df.columns)[:8]}{'...' if len(df.columns) > 8 else ''}")

    # ── Save if path provided ─────────────────────────────────────────────────
    if output_path is not None:
        saved = _save_dataframe(df, Path(output_path), fmt)
        print(f"[synthetic] Saved to: {saved}")

    return df


def generate_custom(
    n_customers:         int             = 500,
    sector:              Optional[str]   = None,
    coverage_level:      str             = "high",
    countries:           Optional[list]  = None,
    include_banking:     bool            = False,
    include_ecommerce:   bool            = False,
    include_edge_cases:  bool            = False,
    sparse_pct:          float           = 0.0,
    churn_skew:          bool            = False,
    output_path:         Optional[Path]  = None,
    fmt:                 str             = "csv",
    use_canonical_names: bool            = False,
    seed:                int             = 42,
) -> pd.DataFrame:
    """
    Generate a custom synthetic dataset with specified parameters.

    Args:
        n_customers         : number of customer records to generate
        sector              : None | "banking" | "ecommerce"
        coverage_level      : "high" | "medium" | "mixed" | "identity_only"
        countries           : list of country codes to sample from
        include_banking     : add banking-specific fields
        include_ecommerce   : add e-commerce-specific fields
        include_edge_cases  : inject controlled bad data
        sparse_pct          : proportion of records to make sparse (0-1)
        churn_skew          : if True, ~30% of customers are high churn risk
        output_path         : path to save (without extension), or None
        fmt                 : output format
        use_canonical_names : skip realistic column renaming
        seed                : random seed for reproducibility

    Returns:
        DataFrame of generated records.
    """
    config = {
        "n_customers":       n_customers,
        "sector":            sector,
        "coverage_level":    coverage_level,
        "countries":         countries or COUNTRIES_US,
        "include_banking":   include_banking,
        "include_ecommerce": include_ecommerce,
        "include_edge_cases": include_edge_cases,
        "sparse_pct":        sparse_pct,
        "churn_skew":        churn_skew,
        "seed":              seed,
    }

    rng    = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    print(f"[synthetic] Generating custom dataset...")
    print(f"[synthetic] n={n_customers}, sector={sector}, "
          f"coverage={coverage_level}, seed={seed}")

    records = []
    for i in range(n_customers):
        record = _generate_base_record(i, rng, np_rng, config)
        if include_ecommerce:
            record = _add_ecommerce_fields(record, rng)
        if include_banking:
            record = _add_banking_fields(record, rng)
        if config.get("zip_enrichment_demo"):
            record = _apply_zip_enrichment_cohort(record, i, n_customers, rng)
        if sparse_pct > 0:
            record = _apply_sparsity(record, rng, sparse_pct)
        records.append(record)

    if include_edge_cases:
        records = _inject_edge_cases(records, rng)

    df = pd.DataFrame(records)
    df = _rename_columns(df, use_canonical_names)

    print(f"[synthetic] ✓ Generated {len(df):,} rows × {len(df.columns)} columns")

    if output_path is not None:
        saved = _save_dataframe(df, Path(output_path), fmt)
        print(f"[synthetic] Saved to: {saved}")

    return df


def list_scenarios() -> None:
    """Print descriptions of all available scenarios."""
    print("\nAvailable MK Intel synthetic data scenarios:")
    print("=" * 60)
    for name, config in SCENARIO_REGISTRY.items():
        print(f"\n  {name}")
        print(f"    {config['description']}")
        print(f"    n={config['n_customers']} | seed={config['seed']} | "
              f"sector={config['sector'] or 'general'}")
    print()


def generate_all_scenarios(
    output_dir:          Path,
    fmt:                 str  = "csv",
    use_canonical_names: bool = False,
) -> dict[str, Path]:
    """
    Generate all scenarios and save to output_dir.

    Useful for generating a full test suite in one call.

    Args:
        output_dir          : directory to save all scenario files
        fmt                 : output format for all scenarios
        use_canonical_names : skip realistic column renaming

    Returns:
        Dict mapping scenario name → saved file path.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    saved_paths: dict[str, Path] = {}

    for name in SCENARIO_REGISTRY:
        path = output_dir / name
        df   = generate_scenario(
            scenario_name       = name,
            output_path         = path,
            fmt                 = fmt,
            use_canonical_names = use_canonical_names,
        )
        suffix = {"csv": ".csv", "tsv": ".tsv", "json": ".json",
                  "xlsx": ".xlsx", "parquet": ".parquet"}.get(fmt, ".csv")
        saved_paths[name] = path.with_suffix(suffix)
        print()

    print(f"[synthetic] ✓ All {len(saved_paths)} scenarios saved to: {output_dir}")
    return saved_paths


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="MK Intel Synthetic Data Generator"
    )
    parser.add_argument(
        "scenario",
        nargs="?",
        default=None,
        help="Scenario name, or 'all' to generate all scenarios, "
             "or 'list' to list available scenarios.",
    )
    parser.add_argument(
        "--output", "-o",
        default="./synthetic_data",
        help="Output path (without extension) or directory for 'all'.",
    )
    parser.add_argument(
        "--format", "-f",
        default="csv",
        choices=["csv", "tsv", "json", "xlsx", "parquet"],
        help="Output format (default: csv).",
    )
    parser.add_argument(
        "--canonical",
        action="store_true",
        help="Use canonical field names instead of realistic company names.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Override random seed.",
    )

    args = parser.parse_args()

    if args.scenario is None or args.scenario == "list":
        list_scenarios()
    elif args.scenario == "all":
        generate_all_scenarios(
            output_dir=Path(args.output),
            fmt=args.format,
            use_canonical_names=args.canonical,
        )
    else:
        generate_scenario(
            scenario_name=args.scenario,
            output_path=Path(args.output),
            fmt=args.format,
            use_canonical_names=args.canonical,
            seed=args.seed,
        )