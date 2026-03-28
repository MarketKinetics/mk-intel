"""
zip_enrichment.py
=================
MK Intel — ZIP Code Demographic Enrichment Module

Enriches normalized customer records with ZIP code-inferred
demographic signals using a pre-built ZCTA lookup table.

Adds two fields per customer where zip_code is present:
    zip_inferred_income_tier  : income_tier inferred from ZCTA median income
    zip_inferred_race_eth     : dominant race/eth inferred from ZCTA composition

These fields are used downstream for:
    1. BTA structural matching — zip_inferred_income_tier supplements
       directly reported income where not available
    2. BTA confidence validation — zip_inferred_race_eth cross-checks
       against matched BTA's dominant_race_eth

Compliance gating:
    standard mode  → enabled
    banking_us     → disabled (FCRA/ECOA proxy discrimination risk)
    banking_eu     → disabled (GDPR + EU anti-discrimination)
    eu_gdpr        → disabled (GDPR data minimization)

──────────────────────────────────────────────────────────────────
Public API
──────────────────────────────────────────────────────────────────

    enrich_with_zip(df, zcta_table_path, compliance_mode)
        Enrich a normalized DataFrame with ZIP-inferred signals.
        Returns enriched DataFrame.

    validate_bta_race_match(df_bta, df_norm, bta_baseline)
        Post-match validation: cross-check BTA dominant_race_eth
        against zip_inferred_race_eth per cluster.
        Returns df_bta with bta_match_confidence and
        bta_race_validation added.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


# ── Compliance modes where ZIP enrichment is allowed ─────────────────────────

ZIP_ENRICHMENT_ALLOWED_MODES = {"standard"}

# ── BTA confidence levels ─────────────────────────────────────────────────────

BTA_CONFIDENCE_HIGH   = "high"
BTA_CONFIDENCE_MEDIUM = "medium"
BTA_CONFIDENCE_LOW    = "low"

# Race/eth fields that are considered "close enough" for validation
# Maps canonical schema values to groups for fuzzy matching
RACE_ETH_GROUPS = {
    "White":    {"White"},
    "Black":    {"Black"},
    "Asian":    {"Asian"},
    "Hispanic": {"Hispanic"},
    "Mixed":    {"White", "Black", "Asian", "Hispanic", "Mixed", "Other"},
    "Other":    {"Other", "Mixed"},
}


# ── Public API ────────────────────────────────────────────────────────────────

def enrich_with_zip(
    df:               pd.DataFrame,
    zcta_table_path:  Path,
    compliance_mode:  str = "standard",
) -> pd.DataFrame:
    """
    Enrich normalized customer records with ZIP-inferred demographic signals.

    Adds columns:
        zip_inferred_income_tier : income_tier from ZCTA median income
        zip_inferred_race_eth    : dominant race/eth from ZCTA composition
        zip_enrichment_applied   : True if ZIP was found in lookup table

    Args:
        df               : normalized customer DataFrame
        zcta_table_path  : path to zcta_enrichment.parquet
        compliance_mode  : active compliance mode

    Returns:
        Enriched DataFrame. If compliance mode blocks enrichment,
        returns df unchanged with enrichment columns set to None.
    """
    # ── Compliance gate ───────────────────────────────────────────────────────
    if compliance_mode not in ZIP_ENRICHMENT_ALLOWED_MODES:
        print(f"[zip_enrichment] ZIP enrichment disabled for "
              f"compliance mode '{compliance_mode}'")
        df = df.copy()
        df["zip_inferred_income_tier"] = None
        df["zip_inferred_race_eth"]    = None
        df["zip_enrichment_applied"]   = False
        return df

    # ── Check zip_code column ─────────────────────────────────────────────────
    if "zip_code" not in df.columns:
        print(f"[zip_enrichment] No zip_code column found — skipping enrichment")
        df = df.copy()
        df["zip_inferred_income_tier"] = None
        df["zip_inferred_race_eth"]    = None
        df["zip_enrichment_applied"]   = False
        return df

    # ── Load ZCTA lookup table ────────────────────────────────────────────────
    zcta_path = Path(zcta_table_path)
    if not zcta_path.exists():
        print(f"[zip_enrichment] ⚠ ZCTA table not found at: {zcta_path}")
        print(f"[zip_enrichment] Run build_zcta_enrichment.py first")
        df = df.copy()
        df["zip_inferred_income_tier"] = None
        df["zip_inferred_race_eth"]    = None
        df["zip_enrichment_applied"]   = False
        return df

    zcta_df = pd.read_parquet(zcta_path)[
        ["zip_code", "income_tier", "dominant_race_eth", "race_eth_confidence"]
    ].rename(columns={
        "income_tier":       "zip_inferred_income_tier",
        "dominant_race_eth": "zip_inferred_race_eth",
        "race_eth_confidence": "zip_race_eth_confidence",
    })

    print(f"[zip_enrichment] ZCTA table loaded: {len(zcta_df):,} ZCTAs")

    # ── Normalize ZIP codes for matching ──────────────────────────────────────
    df = df.copy()
    df["_zip_normalized"] = (
        df["zip_code"]
        .astype(str)
        .str.strip()
        .str.zfill(5)
        .str[:5]  # take first 5 digits only (ignore +4 suffix)
    )

    # ── Merge ─────────────────────────────────────────────────────────────────
    df = df.merge(
        zcta_df.rename(columns={"zip_code": "_zip_normalized"}),
        on="_zip_normalized",
        how="left",
    )
    df = df.drop(columns=["_zip_normalized"])

    df["zip_enrichment_applied"] = df["zip_inferred_income_tier"].notna()

    # ── Summary ───────────────────────────────────────────────────────────────
    n_enriched  = df["zip_enrichment_applied"].sum()
    n_total     = len(df)
    n_zip_avail = df["zip_code"].notna().sum()

    print(f"[zip_enrichment] ✓ ZIP enrichment applied")
    print(f"  Customers with zip_code    : {n_zip_avail:,} ({n_zip_avail/n_total:.1%})")
    print(f"  Matched in ZCTA table      : {n_enriched:,} ({n_enriched/n_total:.1%})")

    if n_enriched > 0:
        income_dist = df["zip_inferred_income_tier"].value_counts().to_dict()
        race_dist   = df["zip_inferred_race_eth"].value_counts().to_dict()
        print(f"  Income tier distribution   : {income_dist}")
        print(f"  Race/eth distribution      : {race_dist}")

    return df


def validate_bta_race_match(
    df_bta:       pd.DataFrame,
    df_norm:      pd.DataFrame,
    bta_baseline: pd.DataFrame,
) -> pd.DataFrame:
    """
    Post-match BTA confidence validation.

    For each matched cluster, cross-checks:
        age_bin match    (from structural matching score)
        income match     (zip_inferred_income_tier vs BTA income_tier)
        race match       (zip_inferred_race_eth vs BTA dominant_race_eth)

    Three outcomes:
        Case A — full alignment (age + income + race match):
            bta_match_confidence: high
            bta_race_validation:  confirmed

        Case B — partial alignment (age + income match, race diverges):
            bta_match_confidence: medium
            bta_race_validation:  divergent

        Case C — conflict (race + income point to different BTA):
            bta_match_confidence: low
            bta_race_validation:  conflict
            → flags cluster for LLM custom archetype builder

    Args:
        df_bta        : BTA assignments DataFrame from match_btas()
        df_norm       : normalized records with zip_inferred_* fields
        bta_baseline  : BTA baseline DataFrame

    Returns:
        df_bta with added columns:
            bta_match_confidence
            bta_race_validation
            zip_inferred_income_tier  (cluster-level modal value)
            zip_inferred_race_eth     (cluster-level modal value)
            needs_custom_archetype    (True for Case C clusters)
    """
    df_bta  = df_bta.copy()
    df_norm = df_norm.copy()

    # ── Check if ZIP enrichment was applied ───────────────────────────────────
    has_zip_income = "zip_inferred_income_tier" in df_norm.columns
    has_zip_race   = "zip_inferred_race_eth" in df_norm.columns

    if not has_zip_income and not has_zip_race:
        print(f"[zip_validation] No ZIP enrichment fields found — "
              f"skipping BTA confidence validation")
        df_bta["bta_match_confidence"]    = BTA_CONFIDENCE_MEDIUM
        df_bta["bta_race_validation"]     = "not_available"
        df_bta["needs_custom_archetype"]  = False
        return df_bta

    # ── Merge ZIP signals into BTA assignments ────────────────────────────────
    zip_fields = ["customer_id"]
    if has_zip_income:
        zip_fields.append("zip_inferred_income_tier")
    if has_zip_race:
        zip_fields.append("zip_inferred_race_eth")

    df_merged = df_bta.merge(
        df_norm[zip_fields],
        on="customer_id",
        how="left",
    )

    # ── Build BTA profile lookup ───────────────────────────────────────────────
    bta_profiles = {}
    for _, row in bta_baseline.iterrows():
        bta_profiles[row["segment_id"]] = {
            "income_tier": row.get("dominant_household_income_tier"),
            "race_eth":    row.get("dominant_race_eth"),
        }

    # ── Validate per customer ─────────────────────────────────────────────────
    confidence_list     = []
    race_validation_list = []
    needs_custom_list   = []

    for _, row in df_merged.iterrows():
        bta_id = row.get("bta_id")

        # Non-matched customers — no validation possible
        if pd.isna(bta_id):
            confidence_list.append(None)
            race_validation_list.append("skipped")
            needs_custom_list.append(False)
            continue

        bta_profile = bta_profiles.get(bta_id, {})
        bta_income  = bta_profile.get("income_tier")
        bta_race    = bta_profile.get("race_eth")

        zip_income  = row.get("zip_inferred_income_tier")
        zip_race    = row.get("zip_inferred_race_eth")

        # ── Income match ──────────────────────────────────────────────────────
        income_match = (
            pd.isna(zip_income) or  # no ZIP income data — can't validate
            zip_income == bta_income
        )

        # ── Race match ────────────────────────────────────────────────────────
        race_match = _race_matches(zip_race, bta_race)

        # ── Case determination ────────────────────────────────────────────────
        if income_match and race_match:
            # Case A — full alignment
            confidence_list.append(BTA_CONFIDENCE_HIGH)
            race_validation_list.append("confirmed")
            needs_custom_list.append(False)

        elif income_match and not race_match:
            # Case B — income matches, race diverges
            confidence_list.append(BTA_CONFIDENCE_MEDIUM)
            race_validation_list.append("divergent")
            needs_custom_list.append(False)

        else:
            # Case C — income conflict (race + income disagree on BTA)
            confidence_list.append(BTA_CONFIDENCE_LOW)
            race_validation_list.append("conflict")
            needs_custom_list.append(True)

    df_bta["bta_match_confidence"]   = confidence_list
    df_bta["bta_race_validation"]    = race_validation_list
    df_bta["needs_custom_archetype"] = needs_custom_list

    # ── Add cluster-level modal ZIP signals ───────────────────────────────────
    if has_zip_income:
        cluster_income = (
            df_merged.groupby("cluster_id")["zip_inferred_income_tier"]
            .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
            .rename("cluster_zip_income_tier")
        )
        df_bta = df_bta.merge(
            cluster_income, on="cluster_id", how="left"
        )

    if has_zip_race:
        cluster_race = (
            df_merged.groupby("cluster_id")["zip_inferred_race_eth"]
            .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
            .rename("cluster_zip_race_eth")
        )
        df_bta = df_bta.merge(
            cluster_race, on="cluster_id", how="left"
        )

    # ── Summary ───────────────────────────────────────────────────────────────
    matched = df_bta[df_bta["bta_id"].notna()]
    conf_counts = matched["bta_match_confidence"].value_counts().to_dict()
    race_counts = matched["bta_race_validation"].value_counts().to_dict()
    n_custom    = matched["needs_custom_archetype"].sum()

    print(f"[zip_validation] ✓ BTA confidence validation complete")
    print(f"  Confidence distribution : {conf_counts}")
    print(f"  Race validation         : {race_counts}")
    print(f"  Case C (custom archetype needed): {n_custom:,}")

    return df_bta


def build_custom_archetype(
    cluster_id:        int,
    cluster_signals:   dict,
    session,
    bta_baseline:      pd.DataFrame,
) -> dict:
    """
    LLM-based custom archetype builder for Case C clusters.

    Invoked when ZIP-inferred signals conflict with the best available
    BTA match. Uses Claude to synthesize a custom audience profile
    from available signals.

    Args:
        cluster_id       : cluster identifier
        cluster_signals  : dict of available signals for this cluster:
            {
                "zip_inferred_race_eth":    "Hispanic",
                "zip_inferred_income_tier": "50-99k",
                "age_bin":                  "35-44",
                "behavioral_profile":       {...},
                "company_context":          "...",
            }
        session          : active MKSession
        bta_baseline     : BTA baseline for schema reference

    Returns:
        Custom TA profile dict in BTA card schema.
        Flagged as source_type="llm_inferred_custom_archetype".
    """
    try:
        from utils import get_client
    except ImportError:
        from mk_intel.ingestion.utils import get_client

    import json

    client = get_client(session)

    # Build reference schema from BTA baseline
    bta_sample = bta_baseline.iloc[0].to_dict()
    schema_keys = [k for k in bta_sample.keys()
                   if k not in ("segment_id", "cluster_id")]

    prompt = f"""You are an audience intelligence analyst building a Target Audience profile.

A customer cluster has been analyzed and the available demographic signals conflict
with the pre-built audience archetypes. Build a custom audience profile using the
available signals.

Available signals for this cluster:
{json.dumps(cluster_signals, indent=2)}

Reference BTA schema fields (your output must use these same fields):
{json.dumps(schema_keys, indent=2)}

Build a complete audience profile for this cluster. Use the available signals as
ground truth where provided. Infer reasonable values for missing fields based on
the demographic and behavioral context.

Return ONLY a valid JSON object with the schema fields above populated.
Do not include any explanation or preamble.
"""

    response = client.messages.create(
        model      = "claude-haiku-4-5-20251001",
        max_tokens = 2000,
        messages   = [{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()

    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        profile = json.loads(raw)
    except json.JSONDecodeError:
        print(f"[zip_validation] ⚠ LLM archetype parse failed for "
              f"cluster {cluster_id} — using fallback")
        profile = {}

    # Add provenance metadata
    profile["segment_id"] = f"CUSTOM_{cluster_id:02d}_{bta_id}"
    profile["source_type"]         = "llm_inferred_custom_archetype"
    profile["source_cluster_id"]   = cluster_id
    profile["source_signals"]      = cluster_signals
    profile["archetype_name"]      = profile.get(
        "archetype_name", f"Custom Archetype (Cluster {cluster_id})"
    )

    print(f"[zip_validation] ✓ Custom archetype built for cluster {cluster_id}: "
          f"{profile.get('archetype_name')}")

    return profile


# ── Internal helpers ──────────────────────────────────────────────────────────

def _race_matches(zip_race: str, bta_race: str) -> bool:
    """
    Check if ZIP-inferred race/eth is compatible with BTA dominant race/eth.

    Uses group-level matching to handle minor variations:
        - "Mixed" ZIP race matches any BTA race (insufficient signal)
        - "Other" ZIP race is treated as uncertain — returns True
          (can't confidently say it conflicts)
    """
    if pd.isna(zip_race) or pd.isna(bta_race):
        return True  # no data = can't validate = no conflict

    if zip_race in ("Mixed", "Other"):
        return True  # insufficient signal to claim conflict

    return zip_race == bta_race
