"""
normalizer.py
=============
MK Intel Ingestion — Canonical Schema Normalizer

Transforms a raw company DataFrame into a list of canonical records
conforming to the MK behavioral schema.

Pipeline steps (in order):
    1. Column mapping      — maps company column names to canonical names
                             Rules layer first, LLM layer for unmatched columns.
                             Mapping saved to data/company_data/{slug}/column_mapping.json
    2. Type coercion       — converts raw values to expected Python types
    3. Value standardization — maps company-specific values to canonical values
    4. Derived fields      — bins age→age_bin, income→income_tier, computes days_since etc.
    5. Validation          — flags invalid values, nulls them, logs issues
    6. Custom fields       — stores unrecognized columns in custom_fields dict

Output:
    - List of canonical record dicts (one per input row)
    - column_mapping.json saved per company
    - validation_report.csv saved per company

──────────────────────────────────────────────────────────────────
Column mapping strategy
──────────────────────────────────────────────────────────────────

Layer 1 — Rules (fuzzy match + synonym dictionary):
    Handles obvious matches: "Age", "age_years", "customer_age" → "age"
    Uses rapidfuzz for string similarity. Covers ~70-80% of real columns.
    Zero cost, instant.

Layer 2 — LLM inference (Claude Haiku):
    Runs only on columns not matched by rules.
    Sends unmatched column names + 5 sample values to Claude.
    Returns structured mapping with confidence scores.
    Cost: < $0.01 per new company ingestion.

Mapping is saved after first run. Subsequent runs load it directly —
zero cost and zero LLM calls after first ingestion.

──────────────────────────────────────────────────────────────────
Public API
──────────────────────────────────────────────────────────────────

    normalize(df, session, company_data_dir)
        Main entry point. Returns list of canonical record dicts.

    build_column_mapping(df, session, mapping_path)
        Builds and saves column mapping for a company.

    load_or_create_mapping(df, session, mapping_path)
        Loads existing mapping or creates a new one.
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import pandas as pd
import numpy as np

if TYPE_CHECKING:
    from mk_intel_session import MKSession


# ── Canonical field list ──────────────────────────────────────────────────────
# All valid canonical field names from the schema.
# Used for validation and as the target for column mapping.

CANONICAL_FIELDS = {
    # identity
    "customer_id", "age", "age_bin", "gender", "income_annual", "income_tier",
    "education", "marital_status", "housing_tenure", "zip_code", "country",
    "customer_since",
    # behavioral
    "sessions_last_7d", "sessions_last_30d", "sessions_last_90d",
    "last_active_date", "days_since_active", "feature_adoption_count",
    "feature_adoption_tier", "nps_score", "nps_tier", "support_tickets_total",
    "support_tickets_90d", "cancellation_attempts",
    "digital_banking_sessions_30d", "branch_visits_90d",
    "atm_transactions_30d", "mobile_deposit_active",
    # transactional
    "subscription_plan", "subscription_status", "mrr", "arr", "ltv",
    "total_purchases", "purchases_last_30d", "purchases_last_90d",
    "avg_order_value", "last_purchase_date", "days_since_purchase",
    "payment_failures_total", "discount_usage_pct",
    "cart_abandonment_rate", "product_categories_purchased", "return_rate",
    "account_type", "account_status", "credit_score_tier",
    "overdraft_frequency_90d", "direct_deposit_active", "avg_monthly_balance",
    "product_count", "cross_sell_products",
    # journey
    "lifecycle_stage", "churn_risk_score", "churn_risk_tier",
    "days_to_renewal", "renewal_date", "onboarding_completed",
    "onboarding_completion_pct", "upgrades_total", "downgrades_total",
    "referrals_made", "delinquency_days", "credit_limit_utilization",
    "loan_to_value_ratio",
    # engagement
    "email_open_rate", "email_click_rate", "push_opt_in", "sms_opt_in",
    "preferred_channel", "content_categories", "last_email_open_date",
    "community_member", "reviews_submitted", "avg_review_score",
    # text signals (placeholder)
    "sentiment_overall", "sentiment_score", "top_themes", "pain_points",
    "motivations", "source_type", "source_count", "extraction_model",
    "extraction_date",
}


# ── Synonym dictionary ────────────────────────────────────────────────────────
# Maps common company column name variants to canonical field names.
# Keys are lowercase — matching is always done case-insensitively.

SYNONYMS: dict[str, str] = {
    # customer_id
    # NOTE: "id" removed — too generic (could be row_id, order_id, event_id etc.)
    # Generic "id" columns go through LLM layer for context-aware mapping.
    "customer_id": "customer_id", "cust_id": "customer_id",
    "client_id": "customer_id", "user_id": "customer_id",
    "account_id": "customer_id",
    "customerid": "customer_id", "userid": "customer_id",

    # age / age_bin
    "age": "age", "age_years": "age", "customer_age": "age",
    "age_group": "age_bin", "age_range": "age_bin", "age_bracket": "age_bin",

    # gender
    "gender": "gender", "sex": "gender", "gender_label": "gender",

    # income
    "income": "income_annual", "annual_income": "income_annual",
    "income_usd": "income_annual", "yearly_income": "income_annual",
    "income_annual": "income_annual", "salary": "income_annual",
    "income_tier": "income_tier", "income_bracket": "income_tier",
    "income_group": "income_tier",

    # education
    "education": "education", "edu": "education", "edu_level": "education",
    "education_level": "education", "highest_education": "education",
    "degree": "education",

    # marital_status
    "marital_status": "marital_status", "marital": "marital_status",
    "relationship_status": "marital_status",

    # housing_tenure
    # NOTE: "tenure" removed — too generic (could be employee tenure, job tenure etc.)
    # Generic "tenure" columns go through LLM layer for context-aware mapping.
    "housing_tenure": "housing_tenure",
    "home_ownership": "housing_tenure", "own_or_rent": "housing_tenure",

    # zip_code
    "zip_code": "zip_code", "zip": "zip_code", "postal_code": "zip_code",
    "zipcode": "zip_code",

    # country
    "country": "country", "country_code": "country", "nation": "country",

    # customer_since
    "customer_since": "customer_since", "join_date": "customer_since",
    "signup_date": "customer_since", "created_at": "customer_since",
    "registration_date": "customer_since", "first_purchase_date": "customer_since",
    "member_since": "customer_since", "account_created": "customer_since",
    "date_joined": "customer_since", "enrollment_date": "customer_since",

    # sessions
    "sessions_last_7d": "sessions_last_7d", "sessions_7d": "sessions_last_7d",
    "sessions_last_30d": "sessions_last_30d", "sessions_30d": "sessions_last_30d",
    "sessions_last_90d": "sessions_last_90d", "sessions_90d": "sessions_last_90d",
    "logins_30d": "sessions_last_30d", "logins_90d": "sessions_last_90d",
    "app_sessions": "sessions_last_30d", "app_sessions_30d": "sessions_last_30d",
    "hourspend_on_app": "sessions_last_30d", "hour_spend_on_app": "sessions_last_30d",
    "hourspend": "sessions_last_30d", "time_on_app": "sessions_last_30d",
    "hours_on_app": "sessions_last_30d",

    # last_active
    "last_active_date": "last_active_date", "last_login": "last_active_date",
    "last_seen": "last_active_date", "last_activity": "last_active_date",
    "last_active": "last_active_date", "last_visit": "last_active_date",
    "last_session_date": "last_active_date",

    # days_since_active
    "days_since_active": "days_since_active", "days_inactive": "days_since_active",
    "days_since_login": "days_since_active",
    # NOTE: "tenure" and "tenure_months" map here — duration since last activity,
    # not housing tenure. These are unambiguous in a B2C customer dataset context.
    "tenure": "days_since_active", "tenure_months": "days_since_active",
    "customer_tenure": "days_since_active", "account_tenure": "days_since_active",
    "membership_months": "days_since_active", "months_tenure": "days_since_active",
    "months_with_company": "days_since_active", "months_on_book": "days_since_active",

    # feature adoption
    "feature_adoption_count": "feature_adoption_count",
    "features_used": "feature_adoption_count",
    "features_adopted": "feature_adoption_count",
    "products_used": "feature_adoption_count",
    "add_ons_used": "feature_adoption_count",
    "addons_used": "feature_adoption_count",
    "feature_adoption_tier": "feature_adoption_tier",

    # nps
    "nps_score": "nps_score", "nps": "nps_score", "net_promoter_score": "nps_score",
    "net_promoter": "nps_score", "promoter_score": "nps_score",
    "nps_tier": "nps_tier", "nps_category": "nps_tier",

    # support tickets
    "support_tickets_total": "support_tickets_total",
    "total_tickets": "support_tickets_total",
    "complaints": "support_tickets_total", "complain": "support_tickets_total",
    "complaint_count": "support_tickets_total", "support_contacts": "support_tickets_total",
    "cases_total": "support_tickets_total", "tickets_total": "support_tickets_total",
    "support_tickets_90d": "support_tickets_90d", "tickets_90d": "support_tickets_90d",

    # cancellation
    "cancellation_attempts": "cancellation_attempts",
    "cancel_attempts": "cancellation_attempts",

    # subscription
    # NOTE: "status" and "tier" removed — too generic.
    "subscription_plan": "subscription_plan", "plan": "subscription_plan",
    "plan_name": "subscription_plan", "plan_type": "subscription_plan",
    "contract": "subscription_plan", "contract_type": "subscription_plan",
    "subscription_status": "subscription_status",
    "sub_status": "subscription_status",
    # Binary churn flags — unambiguous in B2C context, map to subscription_status
    "churn": "subscription_status", "churned": "subscription_status",
    "is_churned": "subscription_status", "churn_flag": "subscription_status",
    "churn_label": "subscription_status", "churn_value": "subscription_status",
    "attrited": "subscription_status", "is_active": "subscription_status",
    "active_customer": "subscription_status", "customer_churned": "subscription_status",

    # revenue
    "mrr": "mrr", "monthly_recurring_revenue": "mrr",
    "monthly_charge": "mrr", "monthly_charges": "mrr",
    "monthly_revenue": "mrr", "monthly_spend": "mrr", "monthly_fee": "mrr",
    "arr": "arr", "annual_recurring_revenue": "arr",
    "ltv": "ltv", "lifetime_value": "ltv", "customer_ltv": "ltv",
    "clv": "ltv", "customer_lifetime_value": "ltv",
    "cltv": "ltv", "predicted_ltv": "ltv",

    # purchases
    "total_purchases": "total_purchases", "purchase_count": "total_purchases",
    "purchases_last_30d": "purchases_last_30d", "orders_30d": "purchases_last_30d",
    "order_count": "purchases_last_30d", "ordercount": "purchases_last_30d",
    "orders_last_30d": "purchases_last_30d", "purchases_30d": "purchases_last_30d",
    "monthly_orders": "purchases_last_30d",
    "purchases_last_90d": "purchases_last_90d", "orders_90d": "purchases_last_90d",
    "avg_order_value": "avg_order_value", "aov": "avg_order_value",
    "average_order_value": "avg_order_value", "average_basket": "avg_order_value",
    "basket_size": "avg_order_value", "avg_basket_value": "avg_order_value",
    "average_transaction": "avg_order_value", "avg_transaction_value": "avg_order_value",

    # dates
    "last_purchase_date": "last_purchase_date", "last_order_date": "last_purchase_date",
    "days_since_purchase": "days_since_purchase",
    "days_since_last_order": "days_since_purchase",
    "days_since_last_purchase": "days_since_purchase",
    "daysincelastorder": "days_since_purchase", "last_order_days": "days_since_purchase",
    "recency": "days_since_purchase", "recency_days": "days_since_purchase",

    # payments
    "payment_failures_total": "payment_failures_total",
    "failed_payments": "payment_failures_total",
    "discount_usage_pct": "discount_usage_pct", "discount_rate": "discount_usage_pct",
    "coupon_used": "discount_usage_pct", "couponused": "discount_usage_pct",
    "discount_used": "discount_usage_pct", "promo_used": "discount_usage_pct",
    "voucher_used": "discount_usage_pct", "coupon_usage": "discount_usage_pct",

    # lifecycle / churn risk score
    "lifecycle_stage": "lifecycle_stage", "customer_stage": "lifecycle_stage",
    "churn_risk_score": "churn_risk_score", "churn_score": "churn_risk_score",
    "churn_probability": "churn_risk_score", "churn_propensity": "churn_risk_score",
    "attrition_score": "churn_risk_score", "attrition_probability": "churn_risk_score",
    "churn_risk_tier": "churn_risk_tier", "churn_risk": "churn_risk_tier",

    # renewal
    "renewal_date": "renewal_date", "next_renewal": "renewal_date",
    "days_to_renewal": "days_to_renewal",

    # onboarding
    "onboarding_completed": "onboarding_completed",
    "onboarding_completion_pct": "onboarding_completion_pct",

    # upgrades / downgrades
    "upgrades_total": "upgrades_total", "plan_upgrades": "upgrades_total",
    "downgrades_total": "downgrades_total", "plan_downgrades": "downgrades_total",

    # engagement
    "email_open_rate": "email_open_rate", "open_rate": "email_open_rate",
    "email_opens": "email_open_rate", "email_open_pct": "email_open_rate",
    "email_click_rate": "email_click_rate", "click_rate": "email_click_rate",
    "push_opt_in": "push_opt_in", "sms_opt_in": "sms_opt_in",
    "preferred_channel": "preferred_channel",
    "preferred_login_device": "preferred_channel",
    "preferredlogindevice": "preferred_channel",
    "login_device": "preferred_channel", "preferred_device": "preferred_channel",

    # reviews / satisfaction
    "reviews_submitted": "reviews_submitted", "review_count": "reviews_submitted",
    "avg_review_score": "avg_review_score", "average_rating": "avg_review_score",
    "satisfaction_score": "avg_review_score", "satisfactionscore": "avg_review_score",
    "csat": "avg_review_score", "csat_score": "avg_review_score",
    "star_rating": "avg_review_score", "review_rating": "avg_review_score",

    # e-commerce
    "cart_abandonment_rate": "cart_abandonment_rate",
    "cart_abandon_rate": "cart_abandonment_rate",
    "return_rate": "return_rate", "product_categories_purchased": "product_categories_purchased",
    "preferedordercat": "product_categories_purchased",
    "preferred_order_cat": "product_categories_purchased",
    "product_category": "product_categories_purchased",

    # banking
    "account_type": "account_type", "account_status": "account_status",
    "credit_score_tier": "credit_score_tier",
    "overdraft_frequency_90d": "overdraft_frequency_90d",
    "direct_deposit_active": "direct_deposit_active",
    "avg_monthly_balance": "avg_monthly_balance", "average_balance": "avg_monthly_balance",
    "product_count": "product_count", "products_held": "product_count",
    "cross_sell_products": "cross_sell_products",
    "delinquency_days": "delinquency_days", "days_past_due": "delinquency_days",
    "credit_limit_utilization": "credit_limit_utilization",
    "utilization_rate": "credit_limit_utilization",
    "loan_to_value_ratio": "loan_to_value_ratio", "ltv_ratio": "loan_to_value_ratio",
}

# ── Value synonym dictionaries ────────────────────────────────────────────────
# Maps company-specific values to canonical values.
# All keys lowercase — matching is case-insensitive.

VALUE_SYNONYMS: dict[str, dict[str, str]] = {
    "gender": {
        "m": "Male", "male": "Male", "man": "Male", "boy": "Male",
        "f": "Female", "female": "Female", "woman": "Female", "girl": "Female",
        "other": "Other", "non_binary": "Other", "nonbinary": "Other",
        "prefer_not_to_say": "Unknown", "unknown": "Unknown",
    },
    "education": {
        "hs": "HS_or_less", "high_school": "HS_or_less",
        "hs_diploma": "HS_or_less", "high_school_diploma": "HS_or_less",
        "hs_or_less": "HS_or_less", "less_than_hs": "HS_or_less",
        "no_diploma": "HS_or_less", "secondary": "HS_or_less",
        "some_college": "Some_college", "associate": "Some_college",
        "associates": "Some_college", "junior_college": "Some_college",
        "2_year": "Some_college", "community_college": "Some_college",
        "bachelor": "Bachelor", "bachelors": "Bachelor",
        "bachelor_degree": "Bachelor", "undergraduate": "Bachelor",
        "bs": "Bachelor", "ba": "Bachelor", "4_year": "Bachelor",
        "graduate": "Graduate", "masters": "Graduate", "master": "Graduate",
        "mba": "Graduate", "phd": "Graduate", "doctorate": "Graduate",
        "postgraduate": "Graduate", "ms": "Graduate", "ma": "Graduate",
    },
    "marital_status": {
        "married": "Married", "wed": "Married", "spouse": "Married",
        "single": "Never_Married", "never_married": "Never_Married",
        "unmarried": "Never_Married",
        "divorced": "Previously_Married", "widowed": "Previously_Married",
        "separated": "Previously_Married", "formerly_married": "Previously_Married",
        "previously_married": "Previously_Married",
    },
    "housing_tenure": {
        "owner": "Owner", "own": "Owner", "homeowner": "Owner",
        "owns": "Owner", "owned": "Owner",
        "renter": "Renter", "rent": "Renter", "renting": "Renter",
        "tenant": "Renter",
        "other": "Other",
    },
    "subscription_status": {
        "active": "active", "active_subscriber": "active",
        "paying": "active", "subscribed": "active",
        "cancelled": "cancelled", "canceled": "cancelled",
        "churned": "cancelled", "unsubscribed": "cancelled",
        "paused": "paused", "on_hold": "paused", "suspended": "paused",
        "trial": "trial", "free_trial": "trial", "trialing": "trial",
        "expired": "expired", "lapsed": "expired", "inactive": "expired",
    },
    "lifecycle_stage": {
        "new": "new", "new_customer": "new", "onboarding": "new",
        "growing": "growing", "active": "growing", "engaged": "growing",
        "mature": "mature", "loyal": "mature", "established": "mature",
        "at_risk": "at_risk", "atrisk": "at_risk", "risk": "at_risk",
        "winback": "at_risk",
        "churned": "churned", "lost": "churned", "cancelled": "churned",
        "reactivated": "reactivated", "returned": "reactivated",
        "win_back": "reactivated",
    },
    "churn_risk_tier": {
        "low": "low", "safe": "low", "healthy": "low",
        "medium": "medium", "moderate": "medium",
        "high": "high", "critical": "high", "urgent": "high",
    },
    "feature_adoption_tier": {
        "low": "low", "minimal": "low", "light": "low",
        "medium": "medium", "moderate": "medium",
        "high": "high", "power": "high", "heavy": "high",
    },
    "nps_tier": {
        "detractor": "detractor", "detractors": "detractor",
        "passive": "passive", "passives": "passive", "neutral": "passive",
        "promoter": "promoter", "promoters": "promoter",
    },
    "account_type": {
        "checking": "checking", "chk": "checking",
        "savings": "savings", "sav": "savings",
        "credit": "credit", "credit_card": "credit", "cc": "credit",
        "mortgage": "mortgage", "home_loan": "mortgage",
        "investment": "investment", "brokerage": "investment",
        "other": "other",
    },
    "account_status": {
        "active": "active", "open": "active",
        "dormant": "dormant", "inactive": "dormant",
        "delinquent": "delinquent", "past_due": "delinquent",
        "closed": "closed", "terminated": "closed",
    },
    "credit_score_tier": {
        "excellent": "excellent", "very_good": "excellent",
        "good": "good",
        "fair": "fair", "average": "fair",
        "poor": "poor", "bad": "poor", "very_poor": "poor",
    },
    "sentiment_overall": {
        "positive": "positive", "pos": "positive", "good": "positive",
        "neutral": "neutral", "mixed": "neutral",
        "negative": "negative", "neg": "negative", "bad": "negative",
    },
}

# ── Age binning ───────────────────────────────────────────────────────────────

def _bin_age(age) -> Optional[str]:
    """Bin a raw age value into ACS-aligned age_bin."""
    try:
        age = float(age)
    except (TypeError, ValueError):
        return None
    if age < 18:
        return None  # under 18 — not targetable, flagged in validation
    elif age <= 24:
        return "18-24"
    elif age <= 34:
        return "25-34"
    elif age <= 44:
        return "35-44"
    elif age <= 54:
        return "45-54"
    elif age <= 64:
        return "55-64"
    else:
        return "65+"


def _bin_income(income) -> Optional[str]:
    """Bin a raw income value into ACS-aligned income_tier."""
    try:
        income = float(str(income).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    if income < 0:
        return None
    elif income < 20_000:
        return "0-19k"
    elif income < 50_000:
        return "20-49k"
    elif income < 100_000:
        return "50-99k"
    elif income < 200_000:
        return "100-199k"
    else:
        return "200k+"


# ── Company slug ──────────────────────────────────────────────────────────────

def make_company_slug(name: str) -> str:
    """
    Convert a company name to a clean snake_case slug.
    "Peloton Inc." → "peloton_inc"
    """
    # Normalize unicode
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    # Lowercase
    name = name.lower()
    # Replace non-alphanumeric with underscore
    name = re.sub(r"[^a-z0-9]+", "_", name)
    # Strip leading/trailing underscores
    name = name.strip("_")
    # Collapse multiple underscores
    name = re.sub(r"_+", "_", name)
    return name


# ── Column mapping ────────────────────────────────────────────────────────────

def _normalize_col(col: str) -> str:
    """Normalize a column name for synonym lookup."""
    return re.sub(r"[^a-z0-9]+", "_", col.lower().strip()).strip("_")


def _rules_mapping(columns: list[str]) -> tuple[dict[str, str], list[str]]:
    """
    Layer 1 — Rules-based column mapping.
    Uses synonym dict + fuzzy matching via rapidfuzz.

    Returns:
        matched   : {original_col: canonical_field}
        unmatched : list of columns not matched by rules
    """
    matched: dict[str, str] = {}
    unmatched: list[str] = []

    # Try rapidfuzz if available
    try:
        from rapidfuzz import process, fuzz
        use_fuzzy = True
    except ImportError:
        use_fuzzy = False
        print("[normalizer] Warning: rapidfuzz not installed. "
              "Fuzzy matching disabled — install with: pip install rapidfuzz")

    synonym_keys = list(SYNONYMS.keys())

    for col in columns:
        normalized = _normalize_col(col)

        # Exact synonym match
        if normalized in SYNONYMS:
            matched[col] = SYNONYMS[normalized]
            continue

        # Direct canonical field match
        if normalized in CANONICAL_FIELDS:
            matched[col] = normalized
            continue

        # Fuzzy match against synonym keys
        if use_fuzzy:
            result = process.extractOne(
                normalized,
                synonym_keys,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=85,
            )
            if result:
                matched[col] = SYNONYMS[result[0]]
                continue

        unmatched.append(col)

    return matched, unmatched


def _llm_mapping(
    unmatched_cols: list[str],
    df: pd.DataFrame,
    session: "MKSession",
) -> dict[str, str]:
    """
    Layer 2 — LLM column mapping for columns not matched by rules.

    Sends unmatched column names + 5 sample values to Claude Haiku.
    Returns a mapping dict {original_col: canonical_field_or_null}.

    Args:
        unmatched_cols : list of column names not matched by rules
        df             : full DataFrame (used for sample values)
        session        : active MKSession (for API key)

    Returns:
        Dict mapping unmatched columns to canonical fields (or None if
        the LLM cannot confidently map them).
    """
    if not unmatched_cols:
        return {}

    try:
        from mk_intel.utils import get_client, log_api_usage
    except ImportError:
        from utils import get_client, log_api_usage

    # Build column samples for context
    col_samples = {}
    for col in unmatched_cols:
        samples = df[col].dropna().head(5).tolist()
        col_samples[col] = [str(s) for s in samples]

    canonical_list = sorted(CANONICAL_FIELDS)

    prompt = f"""You are a data schema mapping assistant for a marketing intelligence platform.

Your task is to map company data column names to canonical schema field names.

## Canonical schema fields available:
{json.dumps(canonical_list, indent=2)}

## Columns to map (with sample values):
{json.dumps(col_samples, indent=2)}

## Instructions:
- For each column, identify the best matching canonical field name from the list above.
- If you are not confident (similarity < 70%), return null for that column.
- Return ONLY a JSON object mapping each column name to its canonical field (or null).
- Do not include any explanation, preamble, or markdown.

Example output format:
{{
  "CustomerAge": "age",
  "signup_dt": "customer_since",
  "weird_column_xyz": null
}}"""

    try:
        client   = get_client(session)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        log_api_usage(response, "column_mapping_inference", session)

        raw = response.content[0].text.strip()
        # Strip markdown fences if present
        raw = re.sub(r"```json|```", "", raw).strip()
        mapping = json.loads(raw)

        # Clean: only keep entries where value is a valid canonical field or None
        clean: dict[str, str] = {}
        for col, canonical in mapping.items():
            if canonical is None:
                clean[col] = None
            elif canonical in CANONICAL_FIELDS:
                clean[col] = canonical
            else:
                print(f"[normalizer] LLM returned unknown field '{canonical}' "
                      f"for column '{col}' — treating as unmatched")
                clean[col] = None

        return clean

    except Exception as e:
        print(f"[normalizer] Warning: LLM column mapping failed: {e}")
        return {col: None for col in unmatched_cols}


def build_column_mapping(
    df: pd.DataFrame,
    session: "MKSession",
    mapping_path: Path,
) -> dict:
    """
    Build a full column mapping for a company DataFrame.

    Runs rules layer first, then LLM layer for unmatched columns.
    Saves the mapping to mapping_path.

    Returns:
        Mapping dict:
        {
            "company_slug": str,
            "created_at":   str,
            "mappings": {
                "original_col": {
                    "canonical_field": str or null,
                    "method":          "rules" | "llm" | "unmatched",
                    "confidence":      "high" | "medium" | "low",
                }
            }
        }
    """
    columns = list(df.columns)
    print(f"[normalizer] Building column mapping for {len(columns)} columns...")

    # Layer 1 — rules
    rules_matched, unmatched = _rules_mapping(columns)
    print(f"[normalizer] Rules layer: {len(rules_matched)} matched, "
          f"{len(unmatched)} unmatched")

    # Layer 2 — LLM for unmatched
    llm_matched: dict[str, str] = {}
    if unmatched:
        print(f"[normalizer] LLM layer: inferring mapping for "
              f"{len(unmatched)} unmatched columns...")
        llm_matched = _llm_mapping(unmatched, df, session)

    # ── Collision detection ───────────────────────────────────────────────────
    # Two or more source columns mapping to the same canonical field.
    # Precedence: rules-matched columns win over LLM-matched.
    # Within same method: first column in original order wins.
    # Collisions are logged in the mapping file for analyst review.

    canonical_to_sources: dict[str, list[str]] = {}
    for col, canonical in rules_matched.items():
        canonical_to_sources.setdefault(canonical, []).append(col)
    for col in unmatched:
        canonical = llm_matched.get(col)
        if canonical:
            canonical_to_sources.setdefault(canonical, []).append(col)

    collisions = {
        canonical: sources
        for canonical, sources in canonical_to_sources.items()
        if len(sources) > 1
    }

    if collisions:
        print(f"\n[normalizer] ⚠ Column collisions detected "
              f"({len(collisions)} canonical fields mapped from multiple source columns):")
        for canonical, sources in collisions.items():
            winner = sources[0]
            losers = sources[1:]
            print(f"   '{canonical}' ← {sources}")
            print(f"   Winner: '{winner}' | Moved to custom_fields: {losers}")

    # Build final collision-resolved mapping
    # Winner per canonical field: first rules-matched, then first LLM-matched
    canonical_winner: dict[str, str] = {}  # canonical → winning source col
    collision_losers: set[str] = set()     # source cols demoted to custom_fields

    # Rules-matched get priority
    for col, canonical in rules_matched.items():
        if canonical not in canonical_winner:
            canonical_winner[canonical] = col
        else:
            collision_losers.add(col)

    # LLM-matched fill in remaining
    for col in unmatched:
        canonical = llm_matched.get(col)
        if canonical:
            if canonical not in canonical_winner:
                canonical_winner[canonical] = col
            else:
                collision_losers.add(col)

    # Assemble full mapping
    mappings = {}

    for col, canonical in rules_matched.items():
        if col in collision_losers:
            mappings[col] = {
                "canonical_field": None,
                "method":          "collision_loser",
                "confidence":      "low",
                "collision_note":  f"Lost to '{canonical_winner[canonical]}' "
                                   f"for canonical field '{canonical}'",
            }
        else:
            mappings[col] = {
                "canonical_field": canonical,
                "method":          "rules",
                "confidence":      "high",
            }

    for col in unmatched:
        canonical = llm_matched.get(col)
        if col in collision_losers:
            mappings[col] = {
                "canonical_field": None,
                "method":          "collision_loser",
                "confidence":      "low",
                "collision_note":  f"Lost to '{canonical_winner.get(canonical)}' "
                                   f"for canonical field '{canonical}'",
            }
        else:
            mappings[col] = {
                "canonical_field": canonical,
                "method":          "llm" if canonical else "unmatched",
                "confidence":      "medium" if canonical else "low",
            }

    result = {
        "company_slug":      make_company_slug(
            session.company.name if session.company else "unknown"
        ),
        "created_at":        datetime.now(timezone.utc).isoformat(),
        "source_columns":    list(df.columns),          # stored for stale-mapping detection
        "source_col_count":  len(df.columns),
        "collision_count":   len(collisions),
        "mappings":          mappings,
    }

    # Save to disk
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"[normalizer] Column mapping saved to: {mapping_path}")

    # Summary
    matched_total   = sum(1 for m in mappings.values() if m["canonical_field"])
    unmatched_total = sum(1 for m in mappings.values() if not m["canonical_field"])
    print(f"[normalizer] Mapping summary: {matched_total} mapped, "
          f"{unmatched_total} unmatched (stored as custom_fields)")

    return result


def load_or_create_mapping(
    df: pd.DataFrame,
    session: "MKSession",
    mapping_path: Path,
) -> dict:
    """
    Load existing column mapping or create a new one.

    If an existing mapping is found, validates it against the current
    DataFrame columns. If columns have changed materially (new columns
    added or existing columns removed), warns and rebuilds the mapping.

    Args:
        df           : raw company DataFrame
        session      : active MKSession
        mapping_path : path to column_mapping.json

    Returns:
        Column mapping dict.
    """
    if mapping_path.exists():
        print(f"[normalizer] Loading existing column mapping from: {mapping_path}")
        with open(mapping_path, "r", encoding="utf-8") as f:
            saved = json.load(f)

        # ── Stale mapping detection ───────────────────────────────────────────
        saved_cols   = set(saved.get("source_columns", []))
        current_cols = set(df.columns)

        new_cols     = current_cols - saved_cols
        removed_cols = saved_cols - current_cols

        if new_cols or removed_cols:
            print(f"\n[normalizer] ⚠ Column mismatch detected between saved mapping "
                  f"and current DataFrame:")
            if new_cols:
                print(f"   New columns (not in saved mapping): {sorted(new_cols)}")
            if removed_cols:
                print(f"   Removed columns (in saved mapping but not in data): "
                      f"{sorted(removed_cols)}")

            # If new columns appeared, rebuild mapping to capture them
            if new_cols:
                print(f"[normalizer] Rebuilding mapping to include new columns...")
                return build_column_mapping(df, session, mapping_path)
            else:
                # Only removals — saved mapping still valid, just warn
                print(f"[normalizer] Removed columns are harmless — "
                      f"proceeding with saved mapping.")
        else:
            print(f"[normalizer] ✓ Saved mapping matches current columns.")

        return saved
    else:
        print(f"[normalizer] No existing mapping found — building new mapping...")
        return build_column_mapping(df, session, mapping_path)


# Fields that represent proportions (0-1) — percentage strings are converted
RATE_FIELDS: set[str] = {
    "discount_usage_pct", "cart_abandonment_rate", "return_rate",
    "onboarding_completion_pct", "churn_risk_score", "credit_limit_utilization",
    "email_open_rate", "email_click_rate", "loan_to_value_ratio",
}

# Fields that represent durations in days — time shorthand strings are expanded
DAYS_FIELDS: set[str] = {
    "days_since_active", "days_since_purchase", "days_to_renewal",
    "delinquency_days",
}


def _expand_shorthand(value: str, canonical_field: str) -> Optional[str]:
    """
    Expand common shorthand notation before type coercion.

    Handles two categories:

    Magnitude shorthands (for numeric fields):
        "1.2k"  → "1200"
        "4.5m"  → "4500000"
        "2.1b"  → "2100000000"

    Time expression shorthands (for days fields):
        "4m"    → "120"   (4 months × 30 days)
        "2y"    → "730"   (2 years × 365 days)
        "6w"    → "42"    (6 weeks × 7 days)
        "30d"   → "30"    (30 days — explicit)
        "90d"   → "90"

    Percentage strings (for rate fields):
        "45%"   → "0.45"  (converted to proportion)
        "3.5%"  → "0.035"

    Args:
        value           : raw string value
        canonical_field : target canonical field name (for context)

    Returns:
        Expanded string ready for numeric parsing, or None if no expansion applied.
    """
    s = str(value).strip().lower()

    # ── Percentage → proportion (rate fields only) ────────────────────────────
    if s.endswith("%") and canonical_field in RATE_FIELDS:
        try:
            pct = float(s.rstrip("%").strip())
            return str(pct / 100.0)
        except ValueError:
            pass

    # ── Time expressions → days (days fields only) ────────────────────────────
    if canonical_field in DAYS_FIELDS:
        time_match = re.match(r"^(\d+(?:\.\d+)?)\s*([dwmy])$", s)
        if time_match:
            amount = float(time_match.group(1))
            unit   = time_match.group(2)
            multipliers = {"d": 1, "w": 7, "m": 30, "y": 365}
            return str(int(amount * multipliers[unit]))

    # ── Magnitude shorthands (all numeric fields) ─────────────────────────────
    mag_match = re.match(r"^(\d+(?:\.\d+)?)\s*([kmb])$", s)
    if mag_match:
        amount = float(mag_match.group(1))
        unit   = mag_match.group(2)
        multipliers = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}
        return str(int(amount * multipliers[unit]))

    return None  # no expansion applied


def _coerce_value(value, expected_type: str, canonical_field: str = ""):
    """
    Coerce a single value to the expected type.
    Returns None if coercion fails.

    Handles:
        - Magnitude shorthands: "1.2k" → 1200, "4.5m" → 4500000
        - Time expressions: "4m" → 120 days, "2y" → 730 days
        - Percentage strings for rate fields: "45%" → 0.45
        - Currency symbols: "$50,000" → 50000.0
        - Boolean variants: "yes"/"no"/"1"/"0"
        - Date strings via pandas.to_datetime
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None

    # ── Shorthand expansion (before type coercion) ────────────────────────────
    if isinstance(value, str) and expected_type in ("integer", "float"):
        expanded = _expand_shorthand(value, canonical_field)
        if expanded is not None:
            value = expanded

    if expected_type == "integer":
        try:
            return int(float(str(value).replace(",", "").strip()))
        except (ValueError, TypeError):
            return None

    if expected_type == "float":
        try:
            cleaned = str(value).replace("$", "").replace(",", "").strip()
            # Handle remaining % signs not caught by _expand_shorthand.
            # Only convert to proportion for known rate fields (0-1 range).
            # For non-rate fields, strip % and return raw numeric value.
            if cleaned.endswith("%"):
                raw = float(cleaned.rstrip("%").strip())
                if canonical_field in RATE_FIELDS:
                    cleaned = str(raw / 100.0)
                else:
                    cleaned = str(raw)
            result = float(cleaned)
            # Auto-rescale rate fields on 0-100 scale to 0-1.
            # Handles datasets like IBM Telco where churn_risk_score is 0-100.
            if canonical_field in RATE_FIELDS and result > 1.0:
                rescaled = result / 100.0
                if 0.0 <= rescaled <= 1.0:
                    print(f"[normalizer] Auto-rescaled {canonical_field}: {result} → {rescaled:.4f} (÷100)")
                    return rescaled
            return result
        except (ValueError, TypeError):
            return None

    if expected_type == "boolean":
        if isinstance(value, bool):
            return value
        s = str(value).lower().strip()
        if s in ("true", "yes", "1", "y", "t"):
            return True
        if s in ("false", "no", "0", "n", "f"):
            return False
        return None

    if expected_type == "date":
        try:
            parsed = pd.to_datetime(value, errors="coerce")
            if pd.isna(parsed):
                return None
            return parsed.date().isoformat()
        except Exception:
            return None

    if expected_type == "string":
        s = str(value).strip()
        return s if s else None

    if expected_type == "list":
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            # Try JSON parse first
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass
            # Fall back to comma-separated
            items = [i.strip() for i in value.split(",") if i.strip()]
            return items if items else None
        return None

    return str(value).strip() if value is not None else None


# Field type registry — maps canonical field names to expected types
FIELD_TYPES: dict[str, str] = {
    "customer_id": "string",
    "age": "integer", "age_bin": "string", "gender": "string",
    "income_annual": "float", "income_tier": "string",
    "education": "string", "marital_status": "string",
    "housing_tenure": "string", "zip_code": "string",
    "country": "string", "customer_since": "date",
    "sessions_last_7d": "integer", "sessions_last_30d": "integer",
    "sessions_last_90d": "integer", "last_active_date": "date",
    "days_since_active": "integer", "feature_adoption_count": "integer",
    "feature_adoption_tier": "string", "nps_score": "integer",
    "nps_tier": "string", "support_tickets_total": "integer",
    "support_tickets_90d": "integer", "cancellation_attempts": "integer",
    "digital_banking_sessions_30d": "integer", "branch_visits_90d": "integer",
    "atm_transactions_30d": "integer", "mobile_deposit_active": "boolean",
    "subscription_plan": "string", "subscription_status": "string",
    "mrr": "float", "arr": "float", "ltv": "float",
    "total_purchases": "integer", "purchases_last_30d": "integer",
    "purchases_last_90d": "integer", "avg_order_value": "float",
    "last_purchase_date": "date", "days_since_purchase": "integer",
    "payment_failures_total": "integer", "discount_usage_pct": "float",
    "cart_abandonment_rate": "float",
    "product_categories_purchased": "list", "return_rate": "float",
    "account_type": "string", "account_status": "string",
    "credit_score_tier": "string", "overdraft_frequency_90d": "integer",
    "direct_deposit_active": "boolean", "avg_monthly_balance": "float",
    "product_count": "integer", "cross_sell_products": "list",
    "lifecycle_stage": "string", "churn_risk_score": "float",
    "churn_risk_tier": "string", "days_to_renewal": "integer",
    "renewal_date": "date", "onboarding_completed": "boolean",
    "onboarding_completion_pct": "float", "upgrades_total": "integer",
    "downgrades_total": "integer", "referrals_made": "integer",
    "delinquency_days": "integer", "credit_limit_utilization": "float",
    "loan_to_value_ratio": "float",
    "email_open_rate": "float", "email_click_rate": "float",
    "push_opt_in": "boolean", "sms_opt_in": "boolean",
    "preferred_channel": "string", "content_categories": "list",
    "last_email_open_date": "date", "community_member": "boolean",
    "reviews_submitted": "integer", "avg_review_score": "float",
    "sentiment_overall": "string", "sentiment_score": "float",
    "top_themes": "list", "pain_points": "list", "motivations": "list",
    "source_type": "string", "source_count": "integer",
    "extraction_model": "string", "extraction_date": "date",
}


# ── Value standardization ─────────────────────────────────────────────────────

def _standardize_value(value, canonical_field: str) -> tuple:
    """
    Standardize a value to canonical form using VALUE_SYNONYMS.

    Returns (standardized_value, outcome, original_value) where outcome is:
        "canonical"    : value was already in canonical form, no change needed
        "standardized" : value was successfully mapped to canonical form
        "unrecognized" : value could not be mapped — returned as-is

    This three-state return allows the caller to distinguish between:
        - values that needed no transformation (not worth logging)
        - values that were successfully transformed (good signal)
        - values that are genuinely unrecognized (worth logging for review)
    """
    if value is None:
        return None, "canonical", None

    synonyms = VALUE_SYNONYMS.get(canonical_field)
    if not synonyms:
        return value, "canonical", value

    lookup = str(value).lower().strip().replace(" ", "_")

    # Check if already canonical (value matches a canonical output value)
    canonical_values = set(synonyms.values())
    if str(value) in canonical_values:
        return value, "canonical", value

    # Try synonym lookup
    if lookup in synonyms:
        return synonyms[lookup], "standardized", value

    # Try without underscores
    lookup_plain = lookup.replace("_", "")
    for key, canonical_val in synonyms.items():
        if key.replace("_", "") == lookup_plain:
            return canonical_val, "standardized", value

    # Could not map — return as-is
    return value, "unrecognized", value


# ── Derived field computation ─────────────────────────────────────────────────

def _compute_derived_fields(record: dict) -> dict:
    """
    Compute derived fields from raw values.
    Modifies record in-place. Returns the record.
    """
    today = date.today()

    # age → age_bin
    if record.get("age") is not None and not record.get("age_bin"):
        record["age_bin"] = _bin_age(record["age"])

    # income_annual → income_tier
    if record.get("income_annual") is not None and not record.get("income_tier"):
        record["income_tier"] = _bin_income(record["income_annual"])

    # last_active_date → days_since_active
    if record.get("last_active_date") and not record.get("days_since_active"):
        try:
            d = date.fromisoformat(str(record["last_active_date"]))
            record["days_since_active"] = (today - d).days
        except (ValueError, TypeError):
            pass

    # last_purchase_date → days_since_purchase
    if record.get("last_purchase_date") and not record.get("days_since_purchase"):
        try:
            d = date.fromisoformat(str(record["last_purchase_date"]))
            record["days_since_purchase"] = (today - d).days
        except (ValueError, TypeError):
            pass

    # renewal_date → days_to_renewal
    if record.get("renewal_date") and not record.get("days_to_renewal"):
        try:
            d = date.fromisoformat(str(record["renewal_date"]))
            record["days_to_renewal"] = (d - today).days
        except (ValueError, TypeError):
            pass

    # nps_score → nps_tier
    if record.get("nps_score") is not None and not record.get("nps_tier"):
        score = record["nps_score"]
        if isinstance(score, (int, float)):
            if score <= 6:
                record["nps_tier"] = "detractor"
            elif score <= 8:
                record["nps_tier"] = "passive"
            else:
                record["nps_tier"] = "promoter"

    # churn_risk_score → churn_risk_tier
    if record.get("churn_risk_score") is not None and not record.get("churn_risk_tier"):
        score = record["churn_risk_score"]
        if isinstance(score, float):
            if score < 0.33:
                record["churn_risk_tier"] = "low"
            elif score < 0.66:
                record["churn_risk_tier"] = "medium"
            else:
                record["churn_risk_tier"] = "high"

    # feature_adoption_count → feature_adoption_tier
    if (record.get("feature_adoption_count") is not None
            and not record.get("feature_adoption_tier")):
        count = record["feature_adoption_count"]
        if isinstance(count, int):
            if count <= 2:
                record["feature_adoption_tier"] = "low"
            elif count <= 5:
                record["feature_adoption_tier"] = "medium"
            else:
                record["feature_adoption_tier"] = "high"

    return record


# ── Validation ────────────────────────────────────────────────────────────────

def _validate_record(record: dict, row_index: int) -> list[dict]:
    """
    Validate a canonical record. Invalid values are nulled.
    Returns a list of validation issues found.
    """
    issues = []

    def flag(field, reason, original_value):
        issues.append({
            "row_index":     row_index,
            "field":         field,
            "reason":        reason,
            "original_value": str(original_value),
        })
        record[field] = None

    # age — must be 18+
    if record.get("age") is not None:
        try:
            age = float(record["age"])
            if age < 18:
                flag("age", "Age below 18 — not targetable in B2C context", age)
                record["age_bin"] = None  # invalidate derived field too
            elif age > 120:
                flag("age", "Age above 120 — likely data error", age)
        except (TypeError, ValueError):
            flag("age", "Age could not be parsed as a number", record["age"])

    # nps_score — must be 0-10
    if record.get("nps_score") is not None:
        try:
            nps = float(record["nps_score"])
            if not (0 <= nps <= 10):
                flag("nps_score", f"NPS score out of range (0-10): {nps}", nps)
        except (TypeError, ValueError):
            flag("nps_score", "NPS score could not be parsed", record["nps_score"])

    # Rate fields — must be 0-1
    rate_fields = [
        "discount_usage_pct", "cart_abandonment_rate", "return_rate",
        "onboarding_completion_pct", "churn_risk_score",
        "credit_limit_utilization", "email_open_rate", "email_click_rate",
    ]
    for field in rate_fields:
        val = record.get(field)
        if val is not None:
            try:
                f = float(val)
                if not (0 <= f <= 1):
                    flag(field, f"Rate field out of range (0-1): {f}", f)
            except (TypeError, ValueError):
                flag(field, "Rate field could not be parsed", val)

    # Non-negative integer fields
    non_negative_fields = [
        "sessions_last_7d", "sessions_last_30d", "sessions_last_90d",
        "days_since_active", "feature_adoption_count", "support_tickets_total",
        "support_tickets_90d", "cancellation_attempts", "total_purchases",
        "purchases_last_30d", "purchases_last_90d", "payment_failures_total",
        "upgrades_total", "downgrades_total", "referrals_made",
        "reviews_submitted", "delinquency_days", "product_count",
    ]
    for field in non_negative_fields:
        val = record.get(field)
        if val is not None:
            try:
                if float(val) < 0:
                    flag(field, f"Expected non-negative value, got: {val}", val)
            except (TypeError, ValueError):
                pass

    # avg_review_score — must be 1-5
    if record.get("avg_review_score") is not None:
        try:
            score = float(record["avg_review_score"])
            if not (1 <= score <= 5):
                flag("avg_review_score",
                     f"Review score out of range (1-5): {score}", score)
        except (TypeError, ValueError):
            flag("avg_review_score", "Review score could not be parsed",
                 record["avg_review_score"])

    # sentiment_score — must be -1 to 1
    if record.get("sentiment_score") is not None:
        try:
            score = float(record["sentiment_score"])
            if not (-1 <= score <= 1):
                flag("sentiment_score",
                     f"Sentiment score out of range (-1 to 1): {score}", score)
        except (TypeError, ValueError):
            pass

    return issues


# ── Main entry point ──────────────────────────────────────────────────────────

def normalize(
    df: pd.DataFrame,
    session: "MKSession",
    company_data_dir: Path,
) -> tuple[list[dict], list[dict]]:
    """
    Normalize a raw company DataFrame into canonical records.

    Args:
        df               : raw DataFrame from readers.read_file()
        session          : active MKSession (provides company info + API key)
        company_data_dir : path to data/company_data/{company_slug}/

    Returns:
        Tuple of:
            records          : list of canonical record dicts (one per row)
            validation_issues: list of validation issue dicts

    Side effects:
        - Saves column_mapping.json to company_data_dir
        - Saves validation_report.csv to company_data_dir
    """
    company_name = session.company.name if session.company else "unknown"
    slug         = make_company_slug(company_name)
    mapping_path = company_data_dir / "column_mapping.json"

    print(f"\n[normalizer] Starting normalization for: {company_name}")
    print(f"[normalizer] Input shape: {df.shape}")

    # ── Step 1: Column mapping ────────────────────────────────────────────────
    mapping_data = load_or_create_mapping(df, session, mapping_path)
    mappings     = mapping_data["mappings"]

    # Build rename dict and custom field list
    rename_map:     dict[str, str] = {}
    custom_columns: list[str]      = []

    for orig_col, info in mappings.items():
        canonical = info.get("canonical_field")
        if canonical:
            rename_map[orig_col] = canonical
        else:
            custom_columns.append(orig_col)

    # ── Step 2: Apply mapping, coerce types, standardize, derive, validate ────
    records:           list[dict] = []
    all_issues:        list[dict] = []
    unstandardized_log: list[dict] = []

    for row_index, row in df.iterrows():
        record:       dict = {}
        custom_fields: dict = {}

        # Map and coerce canonical fields
        for orig_col, canonical in rename_map.items():
            raw_val = row.get(orig_col)
            expected_type = FIELD_TYPES.get(canonical, "string")
            coerced = _coerce_value(raw_val, expected_type, canonical)

            # Standardize values for fields with synonym dicts
            if coerced is not None and canonical in VALUE_SYNONYMS:
                std_val, outcome, orig = _standardize_value(coerced, canonical)
                # outcome: "canonical" | "standardized" | "unrecognized"
                if outcome == "unrecognized":
                    unstandardized_log.append({
                        "row_index": row_index,
                        "field":     canonical,
                        "value":     str(coerced),
                    })
                record[canonical] = std_val
            else:
                record[canonical] = coerced

        # Store custom (unmapped) fields
        for col in custom_columns:
            val = row.get(col)
            if val is not None and not (isinstance(val, float) and np.isnan(val)):
                custom_fields[col] = str(val)

        if custom_fields:
            record["custom_fields"] = custom_fields

        # Ensure customer_id is present — use synthetic fallback if missing
        if not record.get("customer_id"):
            record["customer_id"]        = f"tmp_row_{row_index:06d}"
            record["customer_id_source"] = "fallback_row_index"
            # NOTE: synthetic IDs are unstable across re-ingestion runs.
            # If row order changes, the same customer gets a different ID.
            # Always provide a real customer_id in production data.

        # ── Step 3: Derived fields ────────────────────────────────────────────
        record = _compute_derived_fields(record)

        # ── Step 4: Validation ────────────────────────────────────────────────
        issues = _validate_record(record, row_index)
        all_issues.extend(issues)

        records.append(record)

    # ── Save validation report ────────────────────────────────────────────────
    company_data_dir.mkdir(parents=True, exist_ok=True)
    if all_issues:
        issues_df  = pd.DataFrame(all_issues)
        report_path = company_data_dir / "validation_report.csv"
        issues_df.to_csv(report_path, index=False)
        print(f"\n[normalizer] ⚠ Validation issues found: {len(all_issues)} "
              f"across {issues_df['row_index'].nunique()} rows")
        print(f"[normalizer] Validation report saved to: {report_path}")
        print(f"[normalizer] Affected fields: "
              f"{issues_df['field'].value_counts().to_dict()}")
    else:
        print("[normalizer] ✓ No validation issues found")

    # ── Log unrecognized values ───────────────────────────────────────────────
    if unstandardized_log:
        by_field: dict[str, list] = {}
        for entry in unstandardized_log:
            by_field.setdefault(entry["field"], []).append(entry["value"])
        print(f"\n[normalizer] ⚠ {len(unstandardized_log)} values could not be "
              f"mapped to canonical form (stored as-is — review column_mapping.json):")
        for field, values in by_field.items():
            unique_vals = list(set(values))[:5]
            print(f"   {field}: {unique_vals}")

    print(f"\n[normalizer] ✓ Normalization complete: "
          f"{len(records)} records produced")

    return records, all_issues

