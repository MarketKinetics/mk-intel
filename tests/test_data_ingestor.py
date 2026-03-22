"""
test_data_ingestor.py
=====================
MK Intel — Ingestion Pipeline Test Suite

Tests cover:
    Unit tests    : individual functions in isolation
    Integration   : full pipeline with synthetic data (LLM call mocked)

Run from project root:
    python -m pytest tests/test_data_ingestor.py -v

The LLM column mapping call (_llm_mapping) is mocked throughout.
No API key required. No network calls made.

──────────────────────────────────────────────────────────────────
Test classes
──────────────────────────────────────────────────────────────────

    TestIsPresent               coverage.is_present()
    TestComputeCoverage         coverage.compute_coverage()
    TestComplianceExclusions    coverage.get_compliance_excluded_fields()
    TestBinAge                  normalizer._bin_age()
    TestBinIncome               normalizer._bin_income()
    TestExpandShorthand         normalizer._expand_shorthand()
    TestCoerceValue             normalizer._coerce_value()
    TestStandardizeValue        normalizer._standardize_value()
    TestMakeCompanySlug         normalizer.make_company_slug()
    TestDetectFormat            readers.detect_format()
    TestReadCSV                 readers.read_csv()
    TestReadTSV                 readers.read_tsv()
    TestReadJSON                readers.read_json() — shapes A, B, C
    TestReadXLSX                readers.read_xlsx()
    TestReadParquet             readers.read_parquet()
    TestReadFileDispatch        readers.read_file() dispatch
    TestSyntheticGenerator      synthetic_data_generator scenarios
    TestNormalizerPipeline      normalizer.normalize() integration
    TestFullIngestionPipeline   MKDataIngestor.ingest() end-to-end
"""

import json
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

# ── Path setup ────────────────────────────────────────────────────────────────
# Allows running from project root without installing as a package

_PROJECT_ROOT   = Path(__file__).resolve().parents[1]
_INGESTION_DIR  = _PROJECT_ROOT / "mk-intel" / "ingestion"
_MKINTEL_DIR    = _PROJECT_ROOT / "mk-intel"

for p in [str(_PROJECT_ROOT), str(_INGESTION_DIR), str(_MKINTEL_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from coverage import (
    compute_coverage,
    get_compliance_excluded_fields,
    is_present,
    COMPLIANCE_EXCLUSIONS,
)
from normalizer import (
    _bin_age,
    _bin_income,
    _coerce_value,
    _expand_shorthand,
    _standardize_value,
    make_company_slug,
    normalize,
)
from readers import (
    detect_format,
    read_csv,
    read_file,
    read_json,
    read_parquet,
    read_tsv,
    read_xlsx,
)
from synthetic_data_generator import (
    SCENARIO_REGISTRY,
    generate_custom,
    generate_scenario,
    list_scenarios,
)
from mk_intel_session import (
    CompanyProfile,
    MKSession,
    SessionStatus,
)


# ── Shared fixtures ───────────────────────────────────────────────────────────

@pytest.fixture(scope="function")
def sample_session():
    """
    A fresh MKSession per test function.
    Function-scoped to prevent cross-test contamination — full pipeline
    tests mutate proprietary_data and session state.
    """
    session         = MKSession.new(session_mode="developer")
    session.company = CompanyProfile(
        name="Acme Corp",
        url="https://acme.com",
        customer_type="B2C",
        industry="SaaS",
    )
    session.advance(SessionStatus.COMPANY_IDENTIFIED)
    return session


@pytest.fixture(scope="session")
def saas_df():
    """saas_standard synthetic DataFrame (session-scoped — generated once)."""
    return generate_scenario("saas_standard", use_canonical_names=False)


@pytest.fixture(scope="session")
def saas_df_canonical():
    """saas_standard with canonical column names."""
    return generate_scenario("saas_standard", use_canonical_names=True)


@pytest.fixture
def minimal_canonical_record():
    """Minimal canonical record with just customer_id and a few fields."""
    return {
        "customer_id":    "CUST_000001",
        "age":            35,
        "age_bin":        "35-44",
        "income_annual":  75000.0,
        "income_tier":    "50-99k",
        "housing_tenure": "Owner",
        "country":        "US",
    }


@pytest.fixture
def full_canonical_record():
    """Full canonical record with all major domains populated."""
    today = date.today()
    return {
        "customer_id":              "CUST_000002",
        "age":                      42,
        "age_bin":                  "35-44",
        "gender":                   "Female",
        "income_annual":            95000.0,
        "income_tier":              "50-99k",
        "education":                "Bachelor",
        "marital_status":           "Married",
        "housing_tenure":           "Owner",
        "zip_code":                 "10001",
        "country":                  "US",
        "customer_since":           (today - timedelta(days=730)).isoformat(),
        "sessions_last_30d":        18,
        "days_since_active":        3,
        "nps_score":                9,
        "nps_tier":                 "promoter",
        "feature_adoption_count":   7,
        "support_tickets_90d":      1,
        "cancellation_attempts":    0,
        "subscription_plan":        "Professional",
        "subscription_status":      "active",
        "mrr":                      99.0,
        "ltv":                      2500.0,
        "churn_risk_score":         0.12,
        "churn_risk_tier":          "low",
        "lifecycle_stage":          "mature",
        "email_open_rate":          0.42,
        "email_click_rate":         0.09,
    }


# ── TestIsPresent ─────────────────────────────────────────────────────────────

class TestIsPresent:

    def test_none_is_not_present(self):
        assert is_present(None) is False

    def test_empty_string_is_not_present(self):
        assert is_present("") is False
        assert is_present("   ") is False

    def test_empty_list_is_not_present(self):
        assert is_present([]) is False

    def test_zero_is_present(self):
        assert is_present(0) is True

    def test_false_is_present(self):
        assert is_present(False) is True

    def test_zero_float_is_present(self):
        assert is_present(0.0) is True

    def test_non_empty_string_is_present(self):
        assert is_present("hello") is True
        assert is_present("0") is True

    def test_non_empty_list_is_present(self):
        assert is_present([1, 2, 3]) is True
        assert is_present(["a"]) is True

    def test_integer_is_present(self):
        assert is_present(42) is True

    def test_float_is_present(self):
        assert is_present(3.14) is True


# ── TestComputeCoverage ───────────────────────────────────────────────────────

class TestComputeCoverage:

    def test_returns_required_keys(self, minimal_canonical_record):
        result = compute_coverage(minimal_canonical_record)
        required = {
            "coverage_score", "identity_coverage", "behavioral_coverage",
            "transactional_coverage", "journey_coverage", "engagement_coverage",
            "structural_fields_present", "structural_weight_coverage",
            "confidence_tier", "bta_eligible", "compliance_mode",
            "compliance_excluded_fields",
        }
        assert required.issubset(result.keys())

    def test_coverage_score_between_0_and_1(self, full_canonical_record):
        result = compute_coverage(full_canonical_record)
        assert 0.0 <= result["coverage_score"] <= 1.0

    def test_empty_record_has_zero_coverage(self):
        result = compute_coverage({"customer_id": "X"})
        assert result["coverage_score"] == 0.0
        assert result["bta_eligible"] is False

    def test_bta_eligible_requires_structural_threshold(self):
        # age_bin has relative weight 0.40 out of total 1.10 = 0.3636 normalized.
        # This is above the 0.35 threshold so bta_eligible should be True.
        record = {"customer_id": "X", "age_bin": "35-44", "country": "US"}
        result = compute_coverage(record)
        assert result["bta_eligible"] is True
        assert result["structural_weight_coverage"] > 0.35
        # age_bin alone = 0.40/1.10 ≈ 0.3636
        assert result["structural_weight_coverage"] == pytest.approx(0.3636, abs=0.01)

    def test_bta_eligible_false_when_below_threshold(self):
        # marital_status alone = 0.10/1.10 ≈ 0.0909 — below 0.35 threshold
        record = {"customer_id": "X", "marital_status": "Married", "country": "US"}
        result = compute_coverage(record)
        assert result["bta_eligible"] is False
        assert result["structural_weight_coverage"] < 0.35

    def test_non_us_not_bta_eligible(self):
        record = {
            "customer_id": "X", "age_bin": "35-44",
            "income_tier": "50-99k", "country": "GB"
        }
        result = compute_coverage(record)
        assert result["bta_eligible"] is False

    def test_null_country_defaults_to_us_eligible(self):
        record = {
            "customer_id": "X",
            "age_bin": "35-44",
            "income_tier": "50-99k",
        }
        result = compute_coverage(record)
        assert result["bta_eligible"] is True

    def test_confidence_tier_low(self):
        result = compute_coverage({"customer_id": "X"})
        assert result["confidence_tier"] == "low"

    def test_confidence_tier_high(self, full_canonical_record):
        result = compute_coverage(full_canonical_record)
        assert result["confidence_tier"] in ("medium", "high")

    def test_banking_fields_excluded_for_non_banking(self):
        record = {"customer_id": "X", "avg_monthly_balance": 5000.0}
        result = compute_coverage(record, sector=None)
        # Banking field should not count toward coverage for general sector
        assert result["behavioral_coverage"] == 0.0 or \
               result["transactional_coverage"] == 0.0

    def test_banking_fields_counted_for_banking_sector(self):
        record = {
            "customer_id": "X",
            "avg_monthly_balance": 5000.0,
            "product_count": 3,
            "digital_banking_sessions_30d": 12,
        }
        result_general = compute_coverage(record, sector=None)
        result_banking = compute_coverage(record, sector="banking")
        assert result_banking["coverage_score"] >= result_general["coverage_score"]

    def test_compliance_mode_excludes_fields_from_structural_weight(self):
        # In banking_us, age_bin is excluded
        record = {
            "customer_id": "X",
            "age_bin": "35-44",
            "country": "US",
        }
        result_std     = compute_coverage(record, compliance_mode="standard")
        result_banking = compute_coverage(record, compliance_mode="banking_us")
        # age_bin contributes in standard but not banking_us
        assert result_std["structural_weight_coverage"] > \
               result_banking["structural_weight_coverage"]

    def test_compliance_excluded_fields_in_output(self):
        result = compute_coverage(
            {"customer_id": "X"},
            compliance_mode="banking_us"
        )
        assert "age_bin" in result["compliance_excluded_fields"]
        assert "gender" in result["compliance_excluded_fields"]
        assert "zip_code" in result["compliance_excluded_fields"]

    def test_invalid_compliance_mode_raises(self):
        with pytest.raises(ValueError):
            compute_coverage({"customer_id": "X"}, compliance_mode="invalid_mode")

    def test_structural_fields_present_list(self, full_canonical_record):
        result = compute_coverage(full_canonical_record)
        assert isinstance(result["structural_fields_present"], list)
        assert "age_bin" in result["structural_fields_present"]

    def test_text_signals_excluded_from_coverage(self):
        record = {
            "customer_id": "X",
            "sentiment_overall": "positive",
            "top_themes": ["pricing"],
            "pain_points": ["onboarding"],
        }
        result = compute_coverage(record)
        # Text signal fields should not inflate coverage
        assert result["coverage_score"] == 0.0


# ── TestComplianceExclusions ──────────────────────────────────────────────────

class TestComplianceExclusions:

    def test_standard_mode_has_only_descriptor_only(self):
        result = get_compliance_excluded_fields("standard")
        assert "credit_score_tier" in result
        assert "gender" not in result
        assert "age_bin" not in result

    def test_banking_us_excludes_expected_fields(self):
        result = get_compliance_excluded_fields("banking_us")
        for field in ["gender", "age_bin", "zip_code", "credit_score_tier"]:
            assert field in result

    def test_banking_eu_excludes_marital_status(self):
        result = get_compliance_excluded_fields("banking_eu")
        assert "marital_status" in result

    def test_eu_gdpr_excludes_gender_and_zip(self):
        result = get_compliance_excluded_fields("eu_gdpr")
        assert "gender" in result
        assert "zip_code" in result

    def test_result_is_sorted(self):
        result = get_compliance_excluded_fields("banking_us")
        assert result == sorted(result)

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError):
            get_compliance_excluded_fields("unknown_mode")

    def test_descriptor_only_always_present(self):
        for mode in COMPLIANCE_EXCLUSIONS:
            result = get_compliance_excluded_fields(mode)
            assert "credit_score_tier" in result


# ── TestBinAge ────────────────────────────────────────────────────────────────

class TestBinAge:

    def test_under_18_returns_none(self):
        assert _bin_age(17) is None
        assert _bin_age(0) is None

    def test_exactly_18(self):
        assert _bin_age(18) == "18-24"

    def test_age_bin_boundaries(self):
        assert _bin_age(24) == "18-24"
        assert _bin_age(25) == "25-34"
        assert _bin_age(34) == "25-34"
        assert _bin_age(35) == "35-44"
        assert _bin_age(44) == "35-44"
        assert _bin_age(45) == "45-54"
        assert _bin_age(54) == "45-54"
        assert _bin_age(55) == "55-64"
        assert _bin_age(64) == "55-64"
        assert _bin_age(65) == "65+"
        assert _bin_age(99) == "65+"

    def test_string_age_coerced(self):
        assert _bin_age("35") == "35-44"
        assert _bin_age("17") is None

    def test_float_age(self):
        assert _bin_age(35.9) == "35-44"

    def test_invalid_returns_none(self):
        assert _bin_age("not_a_number") is None
        assert _bin_age(None) is None


# ── TestBinIncome ─────────────────────────────────────────────────────────────

class TestBinIncome:

    def test_zero_income(self):
        assert _bin_income(0) == "0-19k"

    def test_income_boundaries(self):
        assert _bin_income(19999)  == "0-19k"
        assert _bin_income(20000)  == "20-49k"
        assert _bin_income(49999)  == "20-49k"
        assert _bin_income(50000)  == "50-99k"
        assert _bin_income(99999)  == "50-99k"
        assert _bin_income(100000) == "100-199k"
        assert _bin_income(199999) == "100-199k"
        assert _bin_income(200000) == "200k+"
        assert _bin_income(500000) == "200k+"

    def test_negative_income_returns_none(self):
        assert _bin_income(-1000) is None

    def test_currency_string(self):
        assert _bin_income("$75,000") == "50-99k"
        assert _bin_income("$200,000") == "200k+"

    def test_invalid_returns_none(self):
        assert _bin_income("not_income") is None
        assert _bin_income(None) is None


# ── TestExpandShorthand ───────────────────────────────────────────────────────

class TestExpandShorthand:

    def test_time_days(self):
        assert _expand_shorthand("30d", "days_since_active") == "30"
        assert _expand_shorthand("90d", "days_since_active") == "90"

    def test_time_weeks(self):
        assert _expand_shorthand("2w", "days_since_active") == "14"
        assert _expand_shorthand("4w", "days_to_renewal") == "28"

    def test_time_months(self):
        assert _expand_shorthand("1m", "days_since_active") == "30"
        assert _expand_shorthand("4m", "days_since_active") == "120"
        assert _expand_shorthand("3m", "days_to_renewal") == "90"

    def test_time_years(self):
        assert _expand_shorthand("1y", "days_since_active") == "365"
        assert _expand_shorthand("2y", "days_since_active") == "730"

    def test_time_shorthand_only_for_days_fields(self):
        # "4m" on a non-days field is treated as magnitude (4 million), not 4 months
        # Time-unit expansion only applies to DAYS_FIELDS
        # For non-days fields, "m" suffix = magnitude (million)
        result = _expand_shorthand("4m", "mrr")
        assert result == "4000000"  # 4 million, not 4 months

    def test_time_shorthand_not_applied_to_non_days_field_weeks(self):
        # Week suffix has no magnitude meaning — should return None for non-days fields
        result = _expand_shorthand("4w", "mrr")
        assert result is None

    def test_magnitude_k(self):
        assert _expand_shorthand("1.2k", "mrr") == "1200"
        assert _expand_shorthand("2.5k", "ltv") == "2500"
        assert _expand_shorthand("10k",  "ltv") == "10000"

    def test_magnitude_m(self):
        assert _expand_shorthand("1m",   "arr")   == "1000000"
        assert _expand_shorthand("2.5m", "ltv")   == "2500000"

    def test_magnitude_b(self):
        assert _expand_shorthand("1b", "arr") == "1000000000"

    def test_percentage_rate_field(self):
        result = _expand_shorthand("45%", "discount_usage_pct")
        assert result == str(0.45)

    def test_percentage_non_rate_field(self):
        # Non-rate field — percentage not auto-converted
        result = _expand_shorthand("45%", "mrr")
        assert result is None

    def test_no_expansion_plain_number(self):
        assert _expand_shorthand("42", "days_since_active") is None
        assert _expand_shorthand("100", "mrr") is None

    def test_case_insensitive(self):
        assert _expand_shorthand("4M", "days_since_active") == "120"
        assert _expand_shorthand("1.2K", "mrr") == "1200"


# ── TestCoerceValue ───────────────────────────────────────────────────────────

class TestCoerceValue:

    def test_integer_coercion(self):
        assert _coerce_value("35", "integer") == 35
        assert _coerce_value(35.9, "integer") == 35
        assert _coerce_value("1,000", "integer") == 1000

    def test_float_coercion(self):
        assert _coerce_value("3.14", "float") == pytest.approx(3.14)
        assert _coerce_value("$50,000", "float") == pytest.approx(50000.0)
        assert _coerce_value("99.99", "float") == pytest.approx(99.99)

    def test_percentage_rate_field_converted(self):
        result = _coerce_value("45%", "float", "discount_usage_pct")
        assert result == pytest.approx(0.45)

    def test_percentage_non_rate_field_not_converted(self):
        result = _coerce_value("45%", "float", "mrr")
        assert result == pytest.approx(45.0)

    def test_boolean_coercion(self):
        assert _coerce_value("true", "boolean") is True
        assert _coerce_value("yes", "boolean") is True
        assert _coerce_value("1", "boolean") is True
        assert _coerce_value("false", "boolean") is False
        assert _coerce_value("no", "boolean") is False
        assert _coerce_value("0", "boolean") is False
        assert _coerce_value(True, "boolean") is True
        assert _coerce_value(False, "boolean") is False

    def test_date_coercion(self):
        result = _coerce_value("2023-01-15", "date")
        assert result == "2023-01-15"

    def test_date_various_formats(self):
        formats = ["01/15/2023", "Jan 15 2023", "2023/01/15"]
        for fmt in formats:
            result = _coerce_value(fmt, "date")
            assert result is not None, f"Failed to parse: {fmt}"

    def test_none_returns_none(self):
        assert _coerce_value(None, "integer") is None
        assert _coerce_value(None, "float") is None
        assert _coerce_value(None, "string") is None

    def test_nan_returns_none(self):
        assert _coerce_value(float("nan"), "float") is None

    def test_shorthand_magnitude_coerced(self):
        result = _coerce_value("1.2k", "float", "mrr")
        assert result == pytest.approx(1200.0)

    def test_shorthand_time_coerced(self):
        result = _coerce_value("4m", "integer", "days_since_active")
        assert result == 120

    def test_list_from_string(self):
        result = _coerce_value("apparel,electronics,home", "list")
        assert isinstance(result, list)
        assert "apparel" in result
        assert len(result) == 3

    def test_list_from_json_string(self):
        result = _coerce_value('["a", "b", "c"]', "list")
        assert result == ["a", "b", "c"]

    def test_invalid_integer_returns_none(self):
        assert _coerce_value("not_a_number", "integer") is None

    def test_invalid_float_returns_none(self):
        assert _coerce_value("not_a_float", "float") is None


# ── TestStandardizeValue ──────────────────────────────────────────────────────

class TestStandardizeValue:

    def test_already_canonical_returns_canonical_outcome(self):
        val, outcome, _ = _standardize_value("Male", "gender")
        assert outcome == "canonical"
        assert val == "Male"

    def test_standardized_maps_correctly(self):
        val, outcome, _ = _standardize_value("M", "gender")
        assert outcome == "standardized"
        assert val == "Male"

    def test_unrecognized_returns_unrecognized_outcome(self):
        val, outcome, _ = _standardize_value("ALIEN", "gender")
        assert outcome == "unrecognized"
        assert val == "ALIEN"

    def test_case_insensitive(self):
        val, outcome, _ = _standardize_value("MALE", "gender")
        assert val == "Male"
        assert outcome in ("canonical", "standardized")

    def test_education_synonyms(self):
        cases = [
            ("hs_diploma", "HS_or_less"),
            ("bachelor", "Bachelor"),
            ("Graduate", "Graduate"),
            ("phd", "Graduate"),
            ("mba", "Graduate"),
        ]
        for input_val, expected in cases:
            val, _, _ = _standardize_value(input_val, "education")
            assert val == expected, f"Failed: {input_val} → {val} (expected {expected})"

    def test_subscription_status_synonyms(self):
        cases = [
            ("ACTIVE", "active"),
            ("churned", "cancelled"),
            ("trialing", "trial"),
            ("lapsed", "expired"),
        ]
        for input_val, expected in cases:
            val, _, _ = _standardize_value(input_val, "subscription_status")
            assert val == expected

    def test_no_synonyms_field_returns_canonical(self):
        val, outcome, _ = _standardize_value("anything", "mrr")
        assert outcome == "canonical"
        assert val == "anything"

    def test_none_returns_canonical(self):
        val, outcome, _ = _standardize_value(None, "gender")
        assert val is None
        assert outcome == "canonical"


# ── TestMakeCompanySlug ───────────────────────────────────────────────────────

class TestMakeCompanySlug:

    def test_simple_name(self):
        assert make_company_slug("Peloton") == "peloton"

    def test_spaces_become_underscores(self):
        assert make_company_slug("Acme Corp") == "acme_corp"

    def test_punctuation_removed(self):
        assert make_company_slug("Peloton Inc.") == "peloton_inc"

    def test_multiple_spaces_collapsed(self):
        assert make_company_slug("Big   Company  Name") == "big_company_name"

    def test_unicode_normalized(self):
        result = make_company_slug("Société Générale")
        assert result == "societe_generale"

    def test_all_lowercase(self):
        assert make_company_slug("ALLCAPS") == "allcaps"

    def test_numbers_preserved(self):
        assert make_company_slug("Company123") == "company123"

    def test_leading_trailing_stripped(self):
        assert make_company_slug("  Acme  ") == "acme"


# ── TestDetectFormat ──────────────────────────────────────────────────────────

class TestDetectFormat:

    def test_csv(self):
        assert detect_format(Path("data.csv")) == "csv"

    def test_tsv(self):
        assert detect_format(Path("data.tsv")) == "tsv"

    def test_txt_as_tsv(self):
        assert detect_format(Path("data.txt")) == "tsv"

    def test_json(self):
        assert detect_format(Path("data.json")) == "json"

    def test_jsonl(self):
        assert detect_format(Path("data.jsonl")) == "jsonl"

    def test_ndjson(self):
        assert detect_format(Path("data.ndjson")) == "jsonl"

    def test_xlsx(self):
        assert detect_format(Path("data.xlsx")) == "xlsx"

    def test_xls(self):
        assert detect_format(Path("data.xls")) == "xlsx"

    def test_parquet(self):
        assert detect_format(Path("data.parquet")) == "parquet"

    def test_case_insensitive(self):
        assert detect_format(Path("DATA.CSV")) == "csv"
        assert detect_format(Path("data.XLSX")) == "xlsx"

    def test_unsupported_raises(self):
        with pytest.raises(ValueError):
            detect_format(Path("data.pdf"))

    def test_unsupported_xml_raises(self):
        with pytest.raises(ValueError):
            detect_format(Path("data.xml"))


# ── TestReadCSV ───────────────────────────────────────────────────────────────

class TestReadCSV:

    def test_reads_basic_csv(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text("CustomerID,Age,MRR\nC001,35,99.99\nC002,42,149.99\n")
        df = read_csv(f)
        assert len(df) == 2
        assert list(df.columns) == ["CustomerID", "Age", "MRR"]

    def test_strips_column_whitespace(self, tmp_path):
        f = tmp_path / "test.csv"
        f.write_text(" CustomerID , Age , MRR \nC001,35,99.99\n")
        df = read_csv(f)
        assert "CustomerID" in df.columns
        assert " CustomerID " not in df.columns

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_csv(tmp_path / "nonexistent.csv")

    def test_empty_file_raises(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("")
        with pytest.raises(ValueError):
            read_csv(f)

    def test_headers_only_raises(self, tmp_path):
        f = tmp_path / "headers_only.csv"
        f.write_text("CustomerID,Age,MRR\n")
        with pytest.raises(ValueError):
            read_csv(f)

    def test_utf8_bom_encoding(self, tmp_path):
        f = tmp_path / "bom.csv"
        f.write_bytes(b"\xef\xbb\xbfCustomerID,Age\nC001,35\n")
        df = read_csv(f)
        assert "CustomerID" in df.columns


# ── TestReadTSV ───────────────────────────────────────────────────────────────

class TestReadTSV:

    def test_reads_basic_tsv(self, tmp_path):
        f = tmp_path / "test.tsv"
        f.write_text("CustomerID\tAge\tMRR\nC001\t35\t99.99\n")
        df = read_tsv(f)
        assert len(df) == 1
        assert "CustomerID" in df.columns

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_tsv(tmp_path / "nonexistent.tsv")


# ── TestReadJSON ──────────────────────────────────────────────────────────────

class TestReadJSON:

    def test_shape_a_array_of_records(self, tmp_path):
        data = [{"id": "C001", "age": 35}, {"id": "C002", "age": 42}]
        f = tmp_path / "test.json"
        f.write_text(json.dumps(data))
        df = read_json(f)
        assert len(df) == 2
        assert "id" in df.columns

    def test_shape_b_records_dict(self, tmp_path):
        data = {"id": {"0": "C001", "1": "C002"}, "age": {"0": 35, "1": 42}}
        f = tmp_path / "test.json"
        f.write_text(json.dumps(data))
        df = read_json(f)
        assert len(df) == 2

    def test_shape_c_jsonl(self, tmp_path):
        f = tmp_path / "test.jsonl"
        f.write_text(
            '{"id": "C001", "age": 35}\n{"id": "C002", "age": 42}\n'
        )
        df = read_json(f)
        assert len(df) == 2

    def test_empty_array_raises(self, tmp_path):
        f = tmp_path / "empty.json"
        f.write_text("[]")
        with pytest.raises(ValueError):
            read_json(f)

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_json(tmp_path / "nonexistent.json")

    def test_malformed_json_raises(self, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text("{not valid json}")
        with pytest.raises(ValueError):
            read_json(f)

    def test_jsonl_malformed_lines_skipped(self, tmp_path, capsys):
        f = tmp_path / "mixed.jsonl"
        f.write_text(
            '{"id": "C001"}\n'
            'NOT VALID JSON\n'
            '{"id": "C002"}\n'
        )
        df = read_json(f)
        assert len(df) == 2
        captured = capsys.readouterr()
        assert "malformed" in captured.out.lower() or "skipping" in captured.out.lower()


# ── TestReadXLSX ──────────────────────────────────────────────────────────────

class TestReadXLSX:

    def test_reads_basic_xlsx(self, tmp_path):
        f = tmp_path / "test.xlsx"
        pd.DataFrame({"CustomerID": ["C001", "C002"], "Age": [35, 42]}).to_excel(
            f, index=False
        )
        df = read_xlsx(f)
        assert len(df) == 2
        assert "CustomerID" in df.columns

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_xlsx(tmp_path / "nonexistent.xlsx")

    def test_sheet_index_out_of_range_raises(self, tmp_path):
        f = tmp_path / "test.xlsx"
        pd.DataFrame({"A": [1]}).to_excel(f, index=False)
        with pytest.raises(ValueError):
            read_xlsx(f, sheet_name=99)

    def test_multiple_sheets_warning(self, tmp_path, capsys):
        f = tmp_path / "multi.xlsx"
        with pd.ExcelWriter(f) as writer:
            pd.DataFrame({"A": [1]}).to_excel(writer, sheet_name="Sheet1", index=False)
            pd.DataFrame({"B": [2]}).to_excel(writer, sheet_name="Sheet2", index=False)
        df = read_xlsx(f)
        captured = capsys.readouterr()
        assert "Warning" in captured.out or "sheets" in captured.out.lower()
        assert len(df) == 1


# ── TestReadParquet ───────────────────────────────────────────────────────────

class TestReadParquet:

    def test_reads_basic_parquet(self, tmp_path):
        f = tmp_path / "test.parquet"
        pd.DataFrame({"CustomerID": ["C001"], "Age": [35]}).to_parquet(f, index=False)
        df = read_parquet(f)
        assert len(df) == 1
        assert "CustomerID" in df.columns

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_parquet(tmp_path / "nonexistent.parquet")


# ── TestReadFileDispatch ──────────────────────────────────────────────────────

class TestReadFileDispatch:

    def test_dispatches_csv(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("ID,Age\nC001,35\n")
        df = read_file(f)
        assert len(df) == 1

    def test_dispatches_json(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text('[{"ID": "C001", "Age": 35}]')
        df = read_file(f)
        assert len(df) == 1

    def test_dispatches_parquet(self, tmp_path):
        f = tmp_path / "data.parquet"
        pd.DataFrame({"ID": ["C001"]}).to_parquet(f, index=False)
        df = read_file(f)
        assert len(df) == 1

    def test_unsupported_format_raises(self, tmp_path):
        f = tmp_path / "data.pdf"
        f.write_text("not a valid format")
        with pytest.raises(ValueError):
            read_file(f)


# ── TestSyntheticGenerator ────────────────────────────────────────────────────

class TestSyntheticGenerator:

    @pytest.mark.parametrize("scenario_name", list(SCENARIO_REGISTRY.keys()))
    def test_all_scenarios_generate(self, scenario_name):
        df = generate_scenario(scenario_name)
        config = SCENARIO_REGISTRY[scenario_name]
        assert len(df) == config["n_customers"]
        assert len(df.columns) > 1
        assert not df.empty

    def test_realistic_column_names_used(self, saas_df):
        # Should NOT have canonical names like 'age', 'mrr'
        assert "age" not in saas_df.columns
        assert "mrr" not in saas_df.columns
        # Should have realistic names
        assert "CustomerAge" in saas_df.columns
        assert "MonthlyRevenue" in saas_df.columns

    def test_canonical_names_when_requested(self, saas_df_canonical):
        assert "age" in saas_df_canonical.columns
        assert "mrr" in saas_df_canonical.columns

    def test_deterministic_with_same_seed(self):
        df1 = generate_scenario("saas_standard", seed=42)
        df2 = generate_scenario("saas_standard", seed=42)
        pd.testing.assert_frame_equal(df1, df2)

    def test_different_seeds_produce_different_data(self):
        df1 = generate_scenario("saas_standard", seed=42)
        df2 = generate_scenario("saas_standard", seed=99)
        assert not df1.equals(df2)

    def test_edge_cases_scenario_has_under_18(self):
        df = generate_scenario("edge_cases", use_canonical_names=True)
        # Some ages should be under 18
        if "age" in df.columns:
            ages = df["age"].dropna()
            assert (ages < 18).any(), "Expected some under-18 ages in edge_cases scenario"

    def test_edge_cases_has_shorthand_values(self):
        df = generate_scenario("edge_cases", use_canonical_names=True)
        # days_since_active should have shorthand strings
        if "days_since_active" in df.columns:
            vals = df["days_since_active"].dropna().astype(str)
            has_shorthand = vals.str.contains(r"[mwdy]", regex=True).any()
            assert has_shorthand, "Expected shorthand time values in edge_cases"

    def test_non_us_scenario_has_foreign_countries(self):
        df = generate_scenario("non_us_mixed", use_canonical_names=True)
        if "country" in df.columns:
            non_us = df["country"].isin(["GB", "CA", "DE", "FR", "IT", "AU"])
            assert non_us.any()

    def test_banking_scenario_has_banking_fields(self):
        df = generate_scenario("banking_us", use_canonical_names=True)
        banking_fields = ["avg_monthly_balance", "product_count", "account_type"]
        for field in banking_fields:
            assert field in df.columns, f"Expected banking field: {field}"

    def test_no_duplicate_column_names(self):
        for scenario_name in SCENARIO_REGISTRY:
            df = generate_scenario(scenario_name)
            assert len(df.columns) == len(set(df.columns)), \
                f"Duplicate columns in scenario '{scenario_name}': " \
                f"{[c for c in df.columns if list(df.columns).count(c) > 1]}"

    def test_save_csv(self, tmp_path):
        path = generate_scenario(
            "small_company",
            output_path=tmp_path / "test",
            fmt="csv",
        )
        # Function returns df, file is saved to path
        assert (tmp_path / "test.csv").exists()

    def test_save_json(self, tmp_path):
        generate_scenario(
            "small_company",
            output_path=tmp_path / "test",
            fmt="json",
        )
        assert (tmp_path / "test.json").exists()

    def test_generate_custom(self):
        df = generate_custom(n_customers=100, seed=42)
        assert len(df) == 100

    def test_list_scenarios_runs(self, capsys):
        list_scenarios()
        captured = capsys.readouterr()
        assert "saas_standard" in captured.out

    def test_no_column_collision_subscription_vs_account_status(self):
        """
        Regression: subscription_status and account_status must not
        both map to 'AccountStatus' causing duplicate column names.
        """
        df = generate_scenario("banking_us")
        cols = list(df.columns)
        assert cols.count("AccountStatus") <= 1, \
            "Column collision: AccountStatus appears more than once"
        assert "SubscriptionStatus" in cols or "AccountStatus" in cols


# ── TestNormalizerPipeline ────────────────────────────────────────────────────

# Mock return value for _llm_mapping — maps remaining unmatched columns
# to None (they become custom_fields). Rules layer handles the known columns.
_MOCK_LLM_MAPPING_RETURN = {}


class TestNormalizerPipeline:
    """
    Integration tests for normalizer.normalize().
    LLM column mapping (_llm_mapping) is mocked throughout.
    """

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_normalize_returns_records_and_issues(self, mock_llm, saas_df_canonical, tmp_path, sample_session):
        records, issues = normalize(saas_df_canonical, sample_session, tmp_path)
        assert isinstance(records, list)
        assert isinstance(issues, list)
        assert len(records) == len(saas_df_canonical)

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_all_records_have_customer_id(self, mock_llm, saas_df_canonical, tmp_path, sample_session):
        records, _ = normalize(saas_df_canonical, sample_session, tmp_path)
        for rec in records:
            assert "customer_id" in rec
            assert rec["customer_id"] is not None

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_column_mapping_saved(self, mock_llm, saas_df_canonical, tmp_path, sample_session):
        normalize(saas_df_canonical, sample_session, tmp_path)
        assert (tmp_path / "column_mapping.json").exists()

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_validation_report_saved_when_issues(self, mock_llm, tmp_path, sample_session):
        # Edge case scenario contains bad data — should produce issues
        df = generate_scenario("edge_cases", use_canonical_names=True)
        _, issues = normalize(df, sample_session, tmp_path)
        if issues:
            assert (tmp_path / "validation_report.csv").exists()

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_age_binned_automatically(self, mock_llm, tmp_path, sample_session):
        df = pd.DataFrame([{
            "customer_id": "C001",
            "age": 35,
            "income_annual": 75000,
        }])
        records, _ = normalize(df, sample_session, tmp_path)
        rec = records[0]
        assert rec.get("age_bin") == "35-44"

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_income_binned_automatically(self, mock_llm, tmp_path, sample_session):
        df = pd.DataFrame([{
            "customer_id": "C001",
            "income_annual": 75000,
        }])
        records, _ = normalize(df, sample_session, tmp_path)
        assert records[0].get("income_tier") == "50-99k"

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_nps_tier_derived(self, mock_llm, tmp_path, sample_session):
        df = pd.DataFrame([
            {"customer_id": "C001", "nps_score": 9},
            {"customer_id": "C002", "nps_score": 7},
            {"customer_id": "C003", "nps_score": 4},
        ])
        records, _ = normalize(df, sample_session, tmp_path)
        tiers = {r["customer_id"]: r.get("nps_tier") for r in records}
        assert tiers["C001"] == "promoter"
        assert tiers["C002"] == "passive"
        assert tiers["C003"] == "detractor"

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_under_18_flagged_as_validation_issue(self, mock_llm, tmp_path, sample_session):
        df = pd.DataFrame([{"customer_id": "C001", "age": 16}])
        records, issues = normalize(df, sample_session, tmp_path)
        assert any(i["field"] == "age" for i in issues)
        assert records[0].get("age") is None

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_synthetic_fallback_id_marked(self, mock_llm, tmp_path, sample_session):
        df = pd.DataFrame([{"age": 35, "mrr": 99.0}])  # no customer_id
        records, _ = normalize(df, sample_session, tmp_path)
        assert records[0]["customer_id"].startswith("tmp_row_")
        assert records[0].get("customer_id_source") == "fallback_row_index"

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_existing_mapping_loaded_on_rerun(self, mock_llm, saas_df_canonical, tmp_path, sample_session):
        # First run — builds mapping
        normalize(saas_df_canonical, sample_session, tmp_path)
        call_count_after_first = mock_llm.call_count

        # Second run — should load from disk, not call LLM again
        normalize(saas_df_canonical, sample_session, tmp_path)
        assert mock_llm.call_count == call_count_after_first  # no new LLM calls

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_column_collision_handled(self, mock_llm, tmp_path, sample_session):
        # Two columns mapping to same canonical field
        df = pd.DataFrame([{
            "customer_id": "C001",
            "age": 35,
            "customer_age": 36,  # second column that maps to age
        }])
        records, _ = normalize(df, sample_session, tmp_path)
        # Should not crash, one value wins
        assert records[0].get("age") is not None

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_unmapped_columns_in_custom_fields(self, mock_llm, tmp_path, sample_session):
        df = pd.DataFrame([{
            "customer_id": "C001",
            "some_internal_crm_id": "XYZ123",
            "proprietary_score": 0.88,
        }])
        records, _ = normalize(df, sample_session, tmp_path)
        custom = records[0].get("custom_fields", {})
        assert len(custom) > 0


# ── TestFullIngestionPipeline ─────────────────────────────────────────────────

class TestFullIngestionPipeline:
    """
    End-to-end integration tests for MKDataIngestor.
    LLM column mapping mocked. Clustering uses sklearn.
    BTA baseline must exist at data/societal_processed/bta_cards/mk_bta_baseline.parquet
    — tests that require it are skipped if the file is missing.
    """

    @pytest.fixture(autouse=True)
    def check_bta_baseline(self):
        """Skip BTA-dependent tests if baseline not found."""
        bta_path = (
            _PROJECT_ROOT
            / "data"
            / "societal_processed"
            / "bta_cards"
            / "mk_bta_baseline.parquet"
        )
        if not bta_path.exists():
            pytest.skip(
                "BTA baseline not found — run notebooks 01-11 first. "
                f"Expected at: {bta_path}"
            )

    def _make_ingestor(self, session, tmp_path):
        """Helper: import and instantiate MKDataIngestor."""
        from mk_data_ingestor import MKDataIngestor
        return MKDataIngestor(
            session           = session,
            company_data_root = tmp_path / "company_data",
            compliance_mode   = "standard",
            sector            = None,
        )

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_saas_standard_full_pipeline(self, mock_llm, tmp_path, sample_session):
        df = generate_scenario("saas_standard", use_canonical_names=True)
        csv_path = tmp_path / "saas_standard.csv"
        df.to_csv(csv_path, index=False)

        ingestor = self._make_ingestor(sample_session, tmp_path)
        session  = ingestor.ingest(csv_path)

        assert session.proprietary_data is not None
        assert session.proprietary_data.normalized is True

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_pipeline_creates_expected_directories(self, mock_llm, tmp_path, sample_session):
        df = generate_scenario("small_company", use_canonical_names=True)
        csv_path = tmp_path / "small_company.csv"
        df.to_csv(csv_path, index=False)

        ingestor = self._make_ingestor(sample_session, tmp_path)
        ingestor.ingest(csv_path)

        assert ingestor.raw_dir.exists()
        assert ingestor.normalized_dir.exists()
        assert ingestor.clustering_dir.exists()
        assert ingestor.bta_dir.exists()
        assert ingestor.enriched_dir.exists()

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_normalized_parquet_saved(self, mock_llm, tmp_path, sample_session):
        df = generate_scenario("small_company", use_canonical_names=True)
        csv_path = tmp_path / "small_company.csv"
        df.to_csv(csv_path, index=False)

        ingestor = self._make_ingestor(sample_session, tmp_path)
        ingestor.ingest(csv_path)

        assert (ingestor.normalized_dir / "normalized_records.parquet").exists()

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_ta_cards_produced(self, mock_llm, tmp_path, sample_session):
        df = generate_scenario("saas_standard", use_canonical_names=True)
        csv_path = tmp_path / "saas.csv"
        df.to_csv(csv_path, index=False)

        ingestor = self._make_ingestor(sample_session, tmp_path)
        ingestor.ingest(csv_path)

        ta_path = ingestor.enriched_dir / "ta_cards.parquet"
        assert ta_path.exists()

        ta_df = pd.read_parquet(ta_path)
        assert len(ta_df) > 0
        assert "ta_id" in ta_df.columns
        assert "source_bta_id" in ta_df.columns

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_segment_mapping_only_valid_ta_ids(self, mock_llm, tmp_path, sample_session):
        df = generate_scenario("saas_standard", use_canonical_names=True)
        csv_path = tmp_path / "saas.csv"
        df.to_csv(csv_path, index=False)

        ingestor = self._make_ingestor(sample_session, tmp_path)
        ingestor.ingest(csv_path)

        prop_data  = sample_session.proprietary_data
        seg_map    = prop_data.segment_mapping or {}
        valid_ids  = {c["ta_id"] for c in (ingestor._ta_cards or [])}

        for customer_id, ta_id in seg_map.items():
            assert ta_id in valid_ids, \
                f"Customer {customer_id} mapped to non-existent TA: {ta_id}"

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_non_us_customers_not_bta_mapped(self, mock_llm, tmp_path, sample_session):
        df = generate_scenario("non_us_mixed", use_canonical_names=True)
        csv_path = tmp_path / "non_us.csv"
        df.to_csv(csv_path, index=False)

        ingestor = self._make_ingestor(sample_session, tmp_path)
        ingestor.ingest(csv_path)

        bta_df = pd.read_parquet(ingestor.bta_dir / "bta_assignments.parquet")
        skipped = bta_df[bta_df["match_method"] == "skipped_non_us"]
        assert len(skipped) > 0

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_pipeline_resumable(self, mock_llm, tmp_path, sample_session):
        df = generate_scenario("small_company", use_canonical_names=True)
        csv_path = tmp_path / "small.csv"
        df.to_csv(csv_path, index=False)

        # First run
        ingestor1 = self._make_ingestor(sample_session, tmp_path)
        ingestor1.ingest(csv_path)
        session_dir = ingestor1.session_dir

        # Second run — same session, should find same directory and resume
        ingestor2 = self._make_ingestor(sample_session, tmp_path)
        assert ingestor2.session_dir == session_dir

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_no_behavioral_features_fallback(self, mock_llm, tmp_path, sample_session):
        df = generate_scenario("no_behavioral_features", use_canonical_names=True)
        csv_path = tmp_path / "no_behav.csv"
        df.to_csv(csv_path, index=False)

        ingestor = self._make_ingestor(sample_session, tmp_path)
        ingestor.ingest(csv_path)

        cluster_df = pd.read_parquet(
            ingestor.clustering_dir / "cluster_assignments.parquet"
        )
        methods = cluster_df["clustering_method"].unique()
        assert any("no_clustering" in str(m) for m in methods)

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_session_mode_preserved(self, mock_llm, tmp_path, sample_session):
        df = generate_scenario("small_company", use_canonical_names=True)
        csv_path = tmp_path / "small.csv"
        df.to_csv(csv_path, index=False)

        ingestor = self._make_ingestor(sample_session, tmp_path)
        session  = ingestor.ingest(csv_path)

        assert session.session_mode == "developer"
        assert session.api_key is None  # never populated in test

    @patch("normalizer._llm_mapping", return_value=_MOCK_LLM_MAPPING_RETURN)
    def test_session_saved_excludes_api_key(self, mock_llm, tmp_path, sample_session):
        df = generate_scenario("small_company", use_canonical_names=True)
        csv_path = tmp_path / "small.csv"
        df.to_csv(csv_path, index=False)

        ingestor = self._make_ingestor(sample_session, tmp_path)
        ingestor.ingest(csv_path)

        # Save session and verify api_key not in JSON
        session_path = sample_session.save(str(tmp_path / "sessions"))
        with open(session_path) as f:
            saved = json.load(f)
        assert "api_key" not in saved
        assert "demo_token" not in saved
