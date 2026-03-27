"""
build_zcta_enrichment.py
========================
MK Intel — ZCTA Demographic Enrichment Table Builder

Pulls ACS 5-year estimates at the ZIP Code Tabulation Area (ZCTA)
level from the Census API and builds a lookup table mapping ZIP codes
to income_tier and dominant_race_eth.

Tables pulled:
    B19013 — Median Household Income
    B03002 — Hispanic or Latino Origin by Race

Output:
    data/reference/zcta_enrichment.parquet

Usage:
    python build_zcta_enrichment.py --api-key YOUR_CENSUS_API_KEY

    Or set CENSUS_API_KEY environment variable and run:
    python build_zcta_enrichment.py

Notes:
    - Uses ACS 5-year estimates (most recent available)
    - ZCTA ≈ ZIP code (close correspondence, not exact)
    - Runtime: ~2-3 minutes for full US (~33,000 ZCTAs)
    - Requires: requests, pandas, pyarrow
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests


# ── Census API configuration ──────────────────────────────────────────────────

CENSUS_BASE_URL = "https://api.census.gov/data"
ACS_YEAR        = 2022   # most recent stable ACS 5-year
ACS_DATASET     = "acs/acs5"

# Variables to pull
VARIABLES = {
    # B19013 — Median household income
    "B19013_001E": "median_household_income",

    # B03002 — Race/ethnicity breakdown
    "B03002_001E": "pop_total",
    "B03002_003E": "pop_white_nh",       # White alone, not Hispanic
    "B03002_004E": "pop_black",          # Black or African American alone
    "B03002_006E": "pop_asian",          # Asian alone
    "B03002_012E": "pop_hispanic",       # Hispanic or Latino (any race)
}

# Income tier mapping (matches canonical schema)
INCOME_TIER_MAP = [
    (0,      19_999,  "0-19k"),
    (20_000, 49_999,  "20-49k"),
    (50_000, 99_999,  "50-99k"),
    (100_000, 199_999, "100-199k"),
    (200_000, float("inf"), "200k+"),
]

# Race/eth mapping — maps Census category to canonical schema value
# Uses plurality rule: largest group = dominant
RACE_ETH_CANONICAL = {
    "pop_white_nh":  "White",
    "pop_black":     "Black",
    "pop_asian":     "Asian",
    "pop_hispanic":  "Hispanic",
}

# Minimum confidence threshold for race assignment
RACE_CONFIDENCE_THRESHOLD = 0.35  # dominant group must be >= 35% of population


# ── Helper functions ──────────────────────────────────────────────────────────

def _map_income_tier(income: float) -> str:
    """Map median household income to canonical income_tier."""
    if income is None or income < 0:
        return None
    for low, high, tier in INCOME_TIER_MAP:
        if low <= income <= high:
            return tier
    return "200k+"


def _map_race_eth(row: dict) -> tuple[str, float]:
    """
    Determine dominant race/ethnicity and confidence score.

    Returns:
        (dominant_race_eth, confidence) where confidence is the
        proportion of the population belonging to the dominant group.
        Returns (None, 0.0) if population total is zero or missing.
    """
    total = row.get("pop_total", 0)
    if not total or total <= 0:
        return None, 0.0

    group_counts = {
        canonical: row.get(census_key, 0)
        for census_key, canonical in RACE_ETH_CANONICAL.items()
    }

    # Other = total - sum of tracked groups
    tracked_sum = sum(group_counts.values())
    group_counts["Other"] = max(0, total - tracked_sum)

    dominant      = max(group_counts, key=group_counts.get)
    dominant_pct  = group_counts[dominant] / total

    if dominant_pct < RACE_CONFIDENCE_THRESHOLD:
        return "Mixed", dominant_pct

    return dominant, round(dominant_pct, 4)


def _fetch_zcta_batch(
    api_key:   str,
    variables: list[str],
    year:      int = ACS_YEAR,
) -> pd.DataFrame:
    """
    Fetch ACS 5-year data for all ZCTAs in one API call.

    The Census API supports wildcard ZCTA queries:
        for=zip+code+tabulation+area:*
    """
    var_string = ",".join(["NAME"] + variables)
    url = (
        f"{CENSUS_BASE_URL}/{year}/{ACS_DATASET}"
        f"?get={var_string}"
        f"&for=zip+code+tabulation+area:*"
        f"&key={api_key}"
    )

    print(f"[census] Fetching {len(variables)} variables for all ZCTAs...")
    print(f"[census] URL: {url[:80]}...")

    response = requests.get(url, timeout=60)

    if response.status_code != 200:
        raise RuntimeError(
            f"Census API error {response.status_code}: {response.text[:200]}"
        )

    data = response.json()

    # First row is headers
    headers = data[0]
    rows    = data[1:]

    df = pd.DataFrame(rows, columns=headers)
    df = df.rename(columns={"zip code tabulation area": "zcta"})

    # Convert numeric columns
    for var in variables:
        if var in df.columns:
            df[var] = pd.to_numeric(df[var], errors="coerce")

    print(f"[census] ✓ Fetched {len(df):,} ZCTAs")
    return df


def build_zcta_enrichment(
    api_key:     str,
    output_path: Path,
    year:        int = ACS_YEAR,
) -> pd.DataFrame:
    """
    Build the ZCTA enrichment lookup table.

    Pulls income and race/ethnicity data for all US ZCTAs,
    maps to canonical schema values, and saves as parquet.

    Args:
        api_key     : Census API key
        output_path : path to save the parquet file
        year        : ACS 5-year estimate year (default: 2022)

    Returns:
        DataFrame with ZCTA enrichment data.
    """
    print(f"\n[census] Building ZCTA enrichment table (ACS {year} 5-year)")
    print(f"[census] Output: {output_path}")
    print()

    # ── Fetch data ────────────────────────────────────────────────────────────
    variables = list(VARIABLES.keys())

    # Some APIs split B19013 and B03002 — try combined first, fall back
    try:
        df_raw = _fetch_zcta_batch(api_key, variables, year)
    except RuntimeError as e:
        print(f"[census] Combined fetch failed: {e}")
        print(f"[census] Trying split fetch (income + race separately)...")
        time.sleep(2)

        income_vars = [v for v in variables if v.startswith("B19013")]
        race_vars   = [v for v in variables if v.startswith("B03002")]

        df_income = _fetch_zcta_batch(api_key, income_vars, year)
        time.sleep(1)
        df_race   = _fetch_zcta_batch(api_key, race_vars, year)

        df_raw = df_income.merge(df_race, on=["NAME", "zcta"], how="outer")
        print(f"[census] ✓ Split fetch complete — {len(df_raw):,} ZCTAs")

    # ── Rename variables to readable names ───────────────────────────────────
    rename_map = {k: v for k, v in VARIABLES.items() if k in df_raw.columns}
    df_raw = df_raw.rename(columns=rename_map)

    # ── Map to canonical values ───────────────────────────────────────────────
    print(f"[census] Mapping to canonical income_tier and race_eth...")

    results = []
    for _, row in df_raw.iterrows():
        zcta   = row["zcta"]
        income = row.get("median_household_income")

        income_tier = _map_income_tier(float(income)) if pd.notna(income) else None
        race_eth, race_conf = _map_race_eth(row.to_dict())

        results.append({
            "zip_code":               str(zcta).zfill(5),
            "zcta":                   str(zcta).zfill(5),
            "median_household_income": income,
            "income_tier":            income_tier,
            "dominant_race_eth":      race_eth,
            "race_eth_confidence":    race_conf,
            "pop_total":              row.get("pop_total"),
            "acs_year":               year,
        })

    df_enriched = pd.DataFrame(results)

    # ── Quality stats ─────────────────────────────────────────────────────────
    n_total       = len(df_enriched)
    n_income      = df_enriched["income_tier"].notna().sum()
    n_race        = df_enriched["dominant_race_eth"].notna().sum()
    race_dist     = df_enriched["dominant_race_eth"].value_counts().to_dict()

    print(f"\n[census] ✓ Enrichment table built")
    print(f"  Total ZCTAs       : {n_total:,}")
    print(f"  Income tier set   : {n_income:,} ({n_income/n_total:.1%})")
    print(f"  Race/eth set      : {n_race:,} ({n_race/n_total:.1%})")
    print(f"  Race distribution : {race_dist}")

    # ── Save ──────────────────────────────────────────────────────────────────
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_enriched.to_parquet(output_path, index=False)
    print(f"\n[census] ✓ Saved to: {output_path}")
    print(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")

    return df_enriched


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build MK Intel ZCTA enrichment table from Census API"
    )
    parser.add_argument(
        "--api-key", "-k",
        default=os.environ.get("CENSUS_API_KEY"),
        help="Census API key (or set CENSUS_API_KEY env var)",
    )
    parser.add_argument(
        "--output", "-o",
        default="data/reference/zcta_enrichment.parquet",
        help="Output path for parquet file",
    )
    parser.add_argument(
        "--year", "-y",
        type=int,
        default=ACS_YEAR,
        help=f"ACS 5-year estimate year (default: {ACS_YEAR})",
    )

    args = parser.parse_args()

    if not args.api_key:
        print("ERROR: Census API key required.")
        print("  Set CENSUS_API_KEY environment variable or pass --api-key")
        sys.exit(1)

    df = build_zcta_enrichment(
        api_key     = args.api_key,
        output_path = Path(args.output),
        year        = args.year,
    )

    print(f"\nSample output:")
    print(df.head(10).to_string(index=False))
