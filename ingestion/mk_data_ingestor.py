"""
mk_data_ingestor.py
===================
MK Intel — Proprietary Data Ingestion Orchestrator

Main entry point for ingesting company data into MK Intel.
Coordinates readers, normalizer, coverage scoring, clustering,
BTA matching, and TA card generation.

──────────────────────────────────────────────────────────────────
Pipeline steps
──────────────────────────────────────────────────────────────────

    Step 1 — load_and_normalize()
        Read file → normalize to canonical schema → validate
        Saves: raw/{file}, normalized/normalized_records.parquet,
               normalized/validation_report.csv

    Step 2 — compute_coverage()
        Compute coverage metadata per record
        Saves: normalized/normalized_records.parquet (updated)

    Step 3 — cluster()
        K-Means on behavioral features, auto k selection
        Saves: clustering/cluster_assignments.parquet,
               clustering/cluster_profiles.parquet,
               clustering/cluster_stats.json

    Step 4 — match_btas()
        Cluster-level structural BTA match + individual override
        Saves: bta_matching/bta_assignments.parquet,
               bta_matching/cross_tabulation.parquet,
               bta_matching/candidate_tas.json

    Step 4.5 — enrich_zip()  [optional, standard mode only]
        ZIP code enrichment + BTA confidence validation.
        Skipped if zcta_path not provided or compliance mode blocks it.
        Adds zip_inferred_income_tier, zip_inferred_race_eth to normalized records.
        Adds bta_match_confidence, bta_race_validation to BTA assignments.
        Case C clusters (income conflict) trigger LLM custom archetype builder.
        Saves: normalized/normalized_records.parquet (updated),
               bta_matching/bta_assignments.parquet (updated),
               enriched/custom_archetypes.json

    Step 5 — build_ta_cards()
        Generate TA cards for candidate cells
        Saves: enriched/ta_cards.parquet,
               enriched/ta_cards.csv,
               enriched/session_ta_corpus.jsonl

    Step 6 — save()
        Update session.proprietary_data, finalize outputs

──────────────────────────────────────────────────────────────────
Resumable pipeline
──────────────────────────────────────────────────────────────────

Each step checks if its output already exists before running.
Re-running a failed pipeline picks up from the last completed step.
Force re-run any step with force=True.

──────────────────────────────────────────────────────────────────
Public API
──────────────────────────────────────────────────────────────────

    MKDataIngestor(session, company_data_root, compliance_mode, sector)
        Initialize the ingestor for a session.

    ingestor.ingest(file_path, force)
        Run the full pipeline. Returns session with updated
        proprietary_data.

    Individual steps are also callable directly for debugging
    or partial re-runs.
"""

from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from mk_intel_session import MKSession


# ── Constants ─────────────────────────────────────────────────────────────────

# Behavioral features used for K-Means clustering (numeric only)
# Fields must be present and < 40% missing to be included
BEHAVIORAL_CLUSTER_FEATURES = [
    # Tier 1 — core signals (always preferred)
    "sessions_last_30d",
    "days_since_active",
    "ltv",
    "mrr",
    "churn_risk_score",
    # Tier 2 — include if present
    "nps_score",
    "feature_adoption_count",
    "support_tickets_90d",
    "total_purchases",
    "onboarding_completion_pct",
    # Tier 3 — banking sector
    "avg_monthly_balance",
    "product_count",
    "credit_limit_utilization",
    "overdraft_frequency_90d",
]

# Structural fields used for BTA matching (demographic)
STRUCTURAL_MATCH_FIELDS = [
    "age_bin",
    "income_tier",
    "housing_tenure",
    "education",
    "marital_status",
]

# Structural field weights for BTA matching (must mirror coverage.py)
STRUCTURAL_WEIGHTS = {
    "age_bin":        0.40,
    "income_tier":    0.40,
    "housing_tenure": 0.10,
    "education":      0.10,
    "marital_status": 0.10,
}

# Cell minimum threshold
MIN_CELL_PCT = 0.03   # 3% of total customers
MIN_CELL_ABS = 50     # absolute floor — whichever is smaller wins

# K range for automatic k selection
K_MIN = 2
K_MAX = 8

# Silhouette sample size threshold
SILHOUETTE_FULL_THRESHOLD = 10_000

# Missing value threshold — exclude features with > 40% missing
MAX_MISSING_PCT = 0.40

# BTA baseline path — loaded directly for structural matching
BTA_BASELINE_FILENAME = "mk_bta_baseline.parquet"

# ── Feature exclusion policy ──────────────────────────────────────────────────
#
# Gate 1 — ALWAYS excluded (outcome labels + pipeline metadata)
# These fields are never valid clustering inputs regardless of OBJ or sector.
CLUSTERING_ALWAYS_EXCLUDED: set[str] = {
    # Outcome / target labels
    "subscription_status",
    "churned",
    # Coverage and pipeline metadata
    "bta_eligible",
    "coverage_score",
    "confidence_tier",
    "customer_id_source",
    "compliance_mode",
    # Derived tier fields — compressed versions of numeric signals
    # already present in the clustering feature set
    "churn_risk_tier",
    "nps_tier",
    "feature_adoption_tier",
    "lifecycle_stage",
}

# Gate 2 — SOBJ-context exclusion keyword map
# Maps SOBJ vocabulary to fields that become outcome-adjacent in that context.
# Only the mapped field is excluded — nothing else.
SOBJ_CONTEXT_EXCLUSIONS: dict[str, list[str]] = {
    "churn":        ["churn_risk_score"],
    "cancel":       ["churn_risk_score"],
    "renew":        ["churn_risk_score"],
    "retain":       ["churn_risk_score"],
    "subscription": ["churn_risk_score"],
    "reactivat":    ["churn_risk_score"],
    "lapsed":       ["churn_risk_score"],
}

# SOBJ-to-behavioral Tier 1 field map for behavioral-only mode.
# Maps SOBJ keywords to the raw behavioral fields (non-aggregated) that
# are most informative for clustering in that SOBJ context.
# Used in _infer_behavioral_tier1() to dynamically rank clustering features.
SOBJ_BEHAVIORAL_TIER1: dict[str, list[str]] = {
    "churn":        ["churn_risk_score", "ltv", "mrr", "sessions_last_30d", "subscription_status"],
    "retain":       ["churn_risk_score", "ltv", "mrr", "sessions_last_30d", "subscription_status"],
    "cancel":       ["churn_risk_score", "ltv", "mrr", "sessions_last_30d", "subscription_status"],
    "renew":        ["ltv", "churn_risk_score", "mrr", "days_since_active", "subscription_status"],
    "reactivat":    ["days_since_active", "ltv", "mrr", "churn_risk_score", "total_purchases"],
    "lapsed":       ["days_since_active", "ltv", "mrr", "churn_risk_score", "total_purchases"],
    "upgrade":      ["ltv", "nps_score", "feature_adoption_count", "sessions_last_30d", "mrr"],
    "upsell":       ["ltv", "nps_score", "feature_adoption_count", "sessions_last_30d", "mrr"],
    "onboard":      ["onboarding_completion_pct", "feature_adoption_count", "sessions_last_30d", "days_since_active"],
    "adopt":        ["onboarding_completion_pct", "feature_adoption_count", "sessions_last_30d", "days_since_active"],
    "engag":        ["sessions_last_30d", "days_since_active", "email_open_rate", "ltv"],
    "refer":        ["nps_score", "ltv", "sessions_last_30d", "total_purchases"],
}

# Gate 3 — Low cardinality threshold for numeric fields
# Numeric fields with fewer unique non-null values than this threshold
# are excluded from clustering — they behave as step functions rather
# than continuous signals and will dominate cluster separation.
NUMERIC_MIN_CARDINALITY = 3


# ── Main class ────────────────────────────────────────────────────────────────

class MKDataIngestor:
    """
    Proprietary data ingestion orchestrator for MK Intel.

    Manages the full pipeline from raw company file to TA cards
    ready for TAR generation.

    Args:
        session           : active MKSession
        company_data_root : path to data/company_data/
        compliance_mode   : standard | banking_us | banking_eu | eu_gdpr
        sector            : None | banking | ecommerce
    """

    def __init__(
        self,
        session: "MKSession",
        company_data_root: Path,
        compliance_mode: str = "standard",
        sector: Optional[str] = None,
        zcta_path: Optional[Path] = None,
    ):
        self.session          = session
        self.company_data_root = Path(company_data_root)
        self.compliance_mode  = compliance_mode
        self.sector           = sector
        self.zcta_path        = Path(zcta_path) if zcta_path else None

        # Derive company slug and session directory
        company_name    = session.company.name if session.company else "unknown"
        self.slug       = _make_company_slug(company_name)
        self.session_dir = self._make_session_dir()

        # Sub-directories
        self.raw_dir         = self.session_dir / "raw"
        self.normalized_dir  = self.session_dir / "normalized"
        self.clustering_dir  = self.session_dir / "clustering"
        self.bta_dir         = self.session_dir / "bta_matching"
        self.enriched_dir    = self.session_dir / "enriched"

        # Pipeline state
        self._records:    Optional[list[dict]]   = None
        self._df_norm:    Optional[pd.DataFrame] = None
        self._df_cluster: Optional[pd.DataFrame] = None
        self._df_bta:     Optional[pd.DataFrame] = None
        self._ta_cards:   Optional[list[dict]]   = None
        self._bta_baseline: Optional[pd.DataFrame] = None

        print(f"[ingestor] Session directory: {self.session_dir}")


    # ── Public pipeline ───────────────────────────────────────────────────────

    def ingest(
        self,
        file_path: Path,
        force: bool = False,
    ) -> "MKSession":
        """
        Run the full ingestion pipeline.

        Args:
            file_path : path to the company data file
            force     : if True, re-run all steps even if outputs exist

        Returns:
            Updated MKSession with proprietary_data populated.
        """
        file_path = Path(file_path)
        print(f"\n[ingestor] ══════════════════════════════════════════")
        print(f"[ingestor] Starting ingestion: {file_path.name}")
        print(f"[ingestor] Company: {self.session.company.name if self.session.company else 'unknown'}")
        print(f"[ingestor] Compliance mode: {self.compliance_mode}")
        print(f"[ingestor] Sector: {self.sector or 'general'}")
        print(f"[ingestor] ══════════════════════════════════════════\n")

        self.load_and_normalize(file_path, force=force)
        self.compute_coverage(force=force)

        # ── Behavioral mode detection ─────────────────────────────────────────
        # After coverage scoring, check if any records are BTA-eligible.
        # If bta_eligible_count == 0, switch to behavioral-only mode and
        # skip BTA matching and ZIP enrichment entirely.
        if self._df_norm is not None and "bta_eligible" in self._df_norm.columns:
            bta_eligible_count = int(self._df_norm["bta_eligible"].sum())
        else:
            bta_eligible_count = 0

        if bta_eligible_count == 0:
            print(f"\n[ingestor] ⚠ No BTA-eligible records found.")
            print(f"[ingestor] Switching to behavioral-only analysis mode.")
            print(f"[ingestor] BTA matching and ZIP enrichment will be skipped.")
            self.session.analysis_mode = "behavioral"
        else:
            print(f"\n[ingestor] BTA mode: {bta_eligible_count:,} eligible records.")
            self.session.analysis_mode = "bta"

        self.cluster(force=force)

        if self.session.analysis_mode == "bta":
            self.match_btas(force=force)
            self.enrich_zip(force=force)
            self.build_ta_cards(force=force)
        else:
            self.build_behavioral_ta_cards(force=force)

        self.save()

        return self.session


    def load_and_normalize(
        self,
        file_path: Path,
        force: bool = False,
    ) -> pd.DataFrame:
        """
        Step 1 — Read file, normalize to canonical schema, validate.

        Args:
            file_path : path to the raw company data file
            force     : re-run even if normalized output exists

        Returns:
            DataFrame of normalized records.
        """
        norm_path = self.normalized_dir / "normalized_records.parquet"

        if norm_path.exists() and not force:
            print(f"[ingestor] Step 1: Loading existing normalized records...")
            self._df_norm = pd.read_parquet(norm_path)
            self._records = self._df_norm.to_dict("records")
            print(f"[ingestor] Step 1: ✓ Loaded {len(self._records):,} records")
            return self._df_norm

        print(f"[ingestor] Step 1: Reading and normalizing {file_path.name}...")

        # ── Read raw file ─────────────────────────────────────────────────────
        try:
            from readers import read_file
        except ImportError:
            from mk_intel.ingestion.readers import read_file

        df_raw = read_file(file_path)

        # ── Save raw copy ─────────────────────────────────────────────────────
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        raw_dest = self.raw_dir / file_path.name
        if Path(file_path).resolve() != Path(raw_dest).resolve():
            shutil.copy2(file_path, raw_dest)
        print(f"[ingestor] Raw file saved to: {raw_dest}")

        # ── Normalize ─────────────────────────────────────────────────────────
        try:
            from normalizer import normalize
        except ImportError:
            from mk_intel.ingestion.normalizer import normalize

        self.normalized_dir.mkdir(parents=True, exist_ok=True)
        self._records, issues = normalize(
            df_raw,
            self.session,
            self.normalized_dir,
        )

        # ── Save normalized parquet ───────────────────────────────────────────
        self._df_norm = pd.DataFrame(self._records)
        self._df_norm.to_parquet(norm_path, index=False)

        print(f"[ingestor] Step 1: ✓ {len(self._records):,} records normalized")
        if issues:
            print(f"[ingestor] Step 1: ⚠ {len(issues)} validation issues — "
                  f"see {self.normalized_dir / 'validation_report.csv'}")

        return self._df_norm


    def compute_coverage(self, force: bool = False) -> pd.DataFrame:
        """
        Step 2 — Compute coverage metadata per record.

        Adds coverage fields to each record and saves updated parquet.

        Returns:
            Updated DataFrame with coverage columns.
        """
        norm_path = self.normalized_dir / "normalized_records.parquet"
        cov_flag  = self.normalized_dir / ".coverage_computed"

        if cov_flag.exists() and not force:
            print(f"[ingestor] Step 2: Coverage already computed — skipping")
            if self._df_norm is None:
                self._df_norm = pd.read_parquet(norm_path)
                self._records = self._df_norm.to_dict("records")
            return self._df_norm

        print(f"[ingestor] Step 2: Computing coverage scores...")

        try:
            from coverage import compute_coverage
        except ImportError:
            from mk_intel.ingestion.coverage import compute_coverage

        if self._records is None:
            self._df_norm = pd.read_parquet(norm_path)
            self._records = self._df_norm.to_dict("records")

        # Compute coverage per record and merge back
        updated = []
        for record in self._records:
            cov = compute_coverage(
                record,
                compliance_mode=self.compliance_mode,
                sector=self.sector,
            )
            updated.append({**record, **cov})

        self._records = updated
        self._df_norm = pd.DataFrame(self._records)
        self._df_norm.to_parquet(norm_path, index=False)

        # Write flag file so we don't recompute on reload
        cov_flag.touch()

        # Summary
        eligible    = self._df_norm["bta_eligible"].sum()
        total       = len(self._df_norm)
        avg_cov     = self._df_norm["coverage_score"].mean()
        conf_counts = self._df_norm["confidence_tier"].value_counts().to_dict()

        print(f"[ingestor] Step 2: ✓ Coverage computed")
        print(f"[ingestor]   BTA eligible: {eligible:,} / {total:,} "
              f"({eligible/total:.1%})")
        print(f"[ingestor]   Avg coverage score: {avg_cov:.2f}")
        print(f"[ingestor]   Confidence tiers: {conf_counts}")

        return self._df_norm


    def cluster(self, force: bool = False) -> pd.DataFrame:
        """
        Step 3 — K-Means clustering on behavioral features.

        Automatically selects k using silhouette score (full data ≤10k)
        or stratified sample + inertia (>10k).

        Returns:
            DataFrame with cluster assignments.
        """
        assign_path = self.clustering_dir / "cluster_assignments.parquet"
        stats_path  = self.clustering_dir / "cluster_stats.json"

        if assign_path.exists() and not force:
            print(f"[ingestor] Step 3: Loading existing cluster assignments...")
            self._df_cluster = pd.read_parquet(assign_path)
            print(f"[ingestor] Step 3: ✓ Loaded — "
                  f"{self._df_cluster['cluster_id'].nunique()} clusters")
            return self._df_cluster

        print(f"[ingestor] Step 3: Clustering customers...")

        if self._df_norm is None:
            self._df_norm = pd.read_parquet(
                self.normalized_dir / "normalized_records.parquet"
            )

        # ── Select and scale features ─────────────────────────────────────────
        features, feature_names = self._behavioral_features(self._df_norm)

        if features is None or features.shape[1] < 2:
            print(f"[ingestor] Step 3: ⚠ Insufficient behavioral features for "
                  f"clustering. Minimum 2 Tier-1 features required.")
            print(f"[ingestor] Step 3: All customers will map directly to BTAs "
                  f"via structural matching only.")
            # Create single-cluster assignment
            self.clustering_dir.mkdir(parents=True, exist_ok=True)
            self._df_cluster = self._df_norm[["customer_id"]].copy()
            self._df_cluster["cluster_id"]        = 0
            self._df_cluster["clustering_method"] = "no_clustering_insufficient_features"
            self._df_cluster.to_parquet(assign_path, index=False)
            return self._df_cluster

        n = len(features)
        print(f"[ingestor] Step 3: {n:,} customers, "
              f"{features.shape[1]} features: {feature_names}")

        # ── Auto k selection ──────────────────────────────────────────────────
        k, stats = self._auto_k(features, n)
        print(f"[ingestor] Step 3: Selected k={k} "
              f"(silhouette={stats['best_silhouette']:.3f})")

        # ── Final K-Means with chosen k ───────────────────────────────────────
        from sklearn.cluster import KMeans
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(features)

        # ── Build assignments DataFrame ───────────────────────────────────────
        self._df_cluster = self._df_norm[["customer_id"]].copy()
        self._df_cluster["cluster_id"] = labels

        # Add scaled feature values for profile computation
        feat_df = pd.DataFrame(
            features,
            columns=[f"feat_{f}" for f in feature_names],
            index=self._df_norm.index,
        )
        self._df_cluster = pd.concat([self._df_cluster, feat_df], axis=1)
        self._df_cluster["clustering_method"] = "kmeans"

        # ── Save ──────────────────────────────────────────────────────────────
        self.clustering_dir.mkdir(parents=True, exist_ok=True)
        self._df_cluster.to_parquet(assign_path, index=False)

        # Save cluster profiles
        profiles = self._build_cluster_profiles(
            self._df_norm, self._df_cluster["cluster_id"]
        )
        profiles.to_parquet(self.clustering_dir / "cluster_profiles.parquet",
                            index=False)

        # Save stats
        gate1_2_excl = self._infer_excluded_features()
        _, all_excl  = self._check_feature_quality(
            self._df_norm,
            [f for f in BEHAVIORAL_CLUSTER_FEATURES
             if f in self._df_norm.columns and f not in gate1_2_excl],
            gate1_2_excl,
        )
        stats["k"]                 = k
        stats["feature_names"]     = feature_names
        stats["n_customers"]       = n
        stats["excluded_features"] = {
            field: info for field, info in all_excl.items()
            if field in self._df_norm.columns  # only log fields actually present
        }
        with open(stats_path, "w") as f:
            json.dump(stats, f, indent=2, default=str)

        # Summary
        cluster_sizes = self._df_cluster["cluster_id"].value_counts().sort_index()
        print(f"[ingestor] Step 3: ✓ Clustering complete")
        for cid, size in cluster_sizes.items():
            print(f"[ingestor]   Cluster {cid}: {size:,} customers "
                  f"({size/n:.1%})")

        return self._df_cluster


    def match_btas(self, force: bool = False) -> pd.DataFrame:
        """
        Step 4 — Match customers to BTAs.

        Two-level matching:
            Level 1 — cluster-level structural match
                Dominant demographic profile of each cluster is matched
                to the nearest BTA using weighted field comparison.
            Level 2 — individual override
                Customers with sufficient individual demographics
                (structural_weight_coverage >= 0.35) get their own BTA
                assignment, overriding the cluster-level default.

        Returns:
            DataFrame with BTA assignments per customer.
        """
        assign_path  = self.bta_dir / "bta_assignments.parquet"
        crosstab_path = self.bta_dir / "cross_tabulation.parquet"
        candidates_path = self.bta_dir / "candidate_tas.json"

        if assign_path.exists() and not force:
            print(f"[ingestor] Step 4: Loading existing BTA assignments...")
            self._df_bta = pd.read_parquet(assign_path)
            print(f"[ingestor] Step 4: ✓ Loaded")
            return self._df_bta

        print(f"[ingestor] Step 4: Matching customers to BTAs...")

        # ── Load dependencies ─────────────────────────────────────────────────
        if self._df_norm is None:
            self._df_norm = pd.read_parquet(
                self.normalized_dir / "normalized_records.parquet"
            )
        if self._df_cluster is None:
            self._df_cluster = pd.read_parquet(
                self.clustering_dir / "cluster_assignments.parquet"
            )

        bta_baseline = self._load_bta_baseline()
        total        = len(self._df_norm)

        # ── Level 1: Cluster-level BTA match ─────────────────────────────────
        cluster_profiles = self._build_cluster_profiles(
            self._df_norm, self._df_cluster["cluster_id"]
        )

        # cluster_bta_map: {cluster_id: [(bta_id, score), ...]}
        # Normally one BTA per cluster. Multiple when tied.
        cluster_bta_map: dict[int, list[dict]] = {}
        for _, row in cluster_profiles.iterrows():
            profile  = row.to_dict()
            matches  = self._structural_match(profile, bta_baseline)
            cid      = int(row["cluster_id"])
            is_tied  = len(matches) > 1
            cluster_bta_map[cid] = [
                {
                    "bta_id":          bta_id,
                    "match_score":     score,
                    "match_method":    "cluster_structural",
                    "is_tied":         is_tied,
                    "ambiguity_group": (
                        f"cluster_{cid}_" +
                        "_".join(
                            f"{f}_{profile.get(f)}"
                            for f in STRUCTURAL_MATCH_FIELDS
                            if profile.get(f) is not None
                        )
                        if is_tied else None
                    ),
                }
                for bta_id, score in matches
            ]
            if is_tied:
                tied_ids = [m["bta_id"] for m in cluster_bta_map[cid]]
                print(f"[ingestor]   Cluster {cid}: tied BTAs {tied_ids} — "
                      f"ambiguous assignment, all will be generated")

        print(f"[ingestor] Step 4: Cluster-level BTA assignments:")
        for cid, matches in cluster_bta_map.items():
            if len(matches) == 1:
                print(f"[ingestor]   Cluster {cid} → {matches[0]['bta_id']} "
                      f"(score={matches[0]['match_score']:.2f})")
            else:
                tied = [(m['bta_id'], m['match_score']) for m in matches]
                print(f"[ingestor]   Cluster {cid} → TIED {tied} "
                      f"— dual assignment, both flagged low confidence")

        # ── Level 2: Individual override ──────────────────────────────────────
        # Customers with structural_weight_coverage >= 0.35 get individual match
        assignments = []
        overrides   = 0

        df_merged = self._df_norm.merge(
            self._df_cluster[["customer_id", "cluster_id"]],
            on="customer_id",
            how="left",
        )

        for _, row in df_merged.iterrows():
            customer_id = row["customer_id"]
            cluster_id  = int(row.get("cluster_id", 0))
            country     = row.get("country")

            # Use pre-computed bta_eligible from coverage.py
            # This centralises the US-check and structural coverage threshold
            # in one place — coverage.py — rather than duplicating logic here.
            bta_eligible = row.get("bta_eligible", False)

            if not bta_eligible:
                # Non-US or insufficient structural coverage
                country     = row.get("country")
                is_us       = (country is None or
                               str(country).strip().upper() in
                               ("US", "USA", "UNITED STATES"))
                skip_reason = "skipped_non_us" if not is_us else "skipped_low_structural_coverage"
                assignments.append({
                    "customer_id":  customer_id,
                    "cluster_id":   cluster_id,
                    "bta_id":       None,
                    "match_method": skip_reason,
                    "match_score":  None,
                    "match_level":  "skipped",
                })
                continue

            # Individual has enough structural signal for a BTA match
            struct_cov = row.get("structural_weight_coverage", 0.0)
            if struct_cov >= 0.35:
                # Individual-level match — may return multiple tied BTAs
                profile  = {f: row.get(f) for f in STRUCTURAL_MATCH_FIELDS}
                matches  = self._structural_match(profile, bta_baseline)
                is_tied  = len(matches) > 1
                all_bta_ids = [m[0] for m in matches]
                ambiguity_group = (
                    f"individual_{customer_id}_" +
                    "_".join(
                        f"{f}_{profile.get(f)}"
                        for f in STRUCTURAL_MATCH_FIELDS
                        if profile.get(f) is not None
                    )
                    if is_tied else None
                )
                for bta_id, score in matches:
                    assignments.append({
                        "customer_id":        customer_id,
                        "cluster_id":         cluster_id,
                        "bta_id":             bta_id,
                        "match_method":       "individual_structural",
                        "match_score":        score,
                        "match_level":        "individual",
                        "is_tied":            is_tied,
                        "ambiguity_group":    ambiguity_group,
                        "competing_bta_ids":  all_bta_ids if is_tied else None,
                        "bta_match_confidence": "low" if is_tied else "high",
                    })
                overrides += 1
            else:
                # Inherit cluster-level assignments (may be multiple if tied)
                cluster_matches = cluster_bta_map.get(cluster_id, [])
                all_bta_ids     = [m["bta_id"] for m in cluster_matches]
                for cluster_info in cluster_matches:
                    assignments.append({
                        "customer_id":        customer_id,
                        "cluster_id":         cluster_id,
                        "bta_id":             cluster_info.get("bta_id"),
                        "match_method":       "cluster_inherited",
                        "match_score":        cluster_info.get("match_score"),
                        "match_level":        "cluster",
                        "is_tied":            cluster_info.get("is_tied", False),
                        "ambiguity_group":    cluster_info.get("ambiguity_group"),
                        "competing_bta_ids":  all_bta_ids if cluster_info.get("is_tied") else None,
                        "bta_match_confidence": "low" if cluster_info.get("is_tied") else "medium",
                    })

        self._df_bta = pd.DataFrame(assignments)

        print(f"[ingestor] Step 4: Individual overrides: "
              f"{overrides:,} / {total:,} ({overrides/total:.1%})")

        # ── Cross-tabulation ──────────────────────────────────────────────────
        crosstab = (
            self._df_bta[self._df_bta["bta_id"].notna()]
            .groupby(["cluster_id", "bta_id"])
            .size()
            .reset_index(name="customer_count")
        )

        # Apply cell minimum threshold
        min_cell = min(MIN_CELL_ABS, int(total * MIN_CELL_PCT))
        print(f"[ingestor] Step 4: Minimum cell size: {min_cell} customers "
              f"(min({MIN_CELL_ABS}, {MIN_CELL_PCT:.0%} of {total:,}))")

        crosstab["passes_threshold"] = crosstab["customer_count"] >= min_cell
        crosstab["pct_of_total"]     = crosstab["customer_count"] / total

        candidate_tas = crosstab[crosstab["passes_threshold"]].to_dict("records")
        thin_cells    = crosstab[~crosstab["passes_threshold"]]

        print(f"[ingestor] Step 4: Cross-tabulation complete")
        print(f"[ingestor]   Total cells: {len(crosstab)}")
        print(f"[ingestor]   Candidate TAs (pass threshold): {len(candidate_tas)}")
        print(f"[ingestor]   Thin cells (below threshold): {len(thin_cells)}")

        # ── Save ──────────────────────────────────────────────────────────────
        self.bta_dir.mkdir(parents=True, exist_ok=True)
        self._df_bta.to_parquet(assign_path, index=False)
        crosstab.to_parquet(crosstab_path, index=False)

        with open(candidates_path, "w") as f:
            json.dump(candidate_tas, f, indent=2)

        return self._df_bta



    def enrich_zip(self, force: bool = False) -> "Optional[pd.DataFrame]":
        """
        Step 4.5 — ZIP code enrichment and BTA confidence validation.

        Optional step — only runs when:
            1. self.zcta_path is set (passed at ingestor init)
            2. compliance_mode allows ZIP enrichment (standard mode only)
            3. zip_code column is present in normalized records

        Adds zip_inferred_income_tier and zip_inferred_race_eth to each
        normalized record, then cross-checks against matched BTA household
        income tier and race/eth to assign BTA confidence (high/medium/low).

        Case C clusters (income conflict) trigger the LLM custom archetype
        builder — one API call per conflicted cluster x BTA cell.

        Saves:
            normalized/normalized_records.parquet (updated with ZIP fields)
            bta_matching/bta_assignments.parquet (updated with confidence)
            enriched/custom_archetypes.json (Case C archetypes, if any)

        Returns:
            Updated df_bta with confidence fields, or None if skipped.
        """
        enrichment_flag = self.normalized_dir / ".zip_enrichment_applied"

        if enrichment_flag.exists() and not force:
            print(f"[ingestor] Step 4.5: ZIP enrichment already applied — skipping")
            return self._df_bta

        # ── No ZCTA path — skip silently ──────────────────────────────────────
        if not self.zcta_path:
            print(f"[ingestor] Step 4.5: No ZCTA path provided — skipping ZIP enrichment")
            return self._df_bta

        try:
            from zip_enrichment import (
                enrich_with_zip,
                validate_bta_race_match,
                build_custom_archetype,
            )
        except ImportError:
            from mk_intel.ingestion.zip_enrichment import (
                enrich_with_zip,
                validate_bta_race_match,
                build_custom_archetype,
            )

        print(f"[ingestor] Step 4.5: ZIP enrichment starting...")

        # ── Load normalized records if needed ─────────────────────────────────
        norm_path = self.normalized_dir / "normalized_records.parquet"
        if self._df_norm is None:
            self._df_norm = pd.read_parquet(norm_path)

        # ── Run ZIP enrichment ────────────────────────────────────────────────
        df_enriched = enrich_with_zip(
            self._df_norm,
            self.zcta_path,
            compliance_mode=self.compliance_mode,
        )
        df_enriched.to_parquet(norm_path, index=False)
        self._df_norm = df_enriched

        # ── Load BTA assignments if needed ────────────────────────────────────
        assign_path = self.bta_dir / "bta_assignments.parquet"
        if self._df_bta is None:
            self._df_bta = pd.read_parquet(assign_path)

        # ── Load BTA baseline ─────────────────────────────────────────────────
        bta_baseline = self._load_bta_baseline()

        # ── Run BTA confidence validation ─────────────────────────────────────
        df_validated = validate_bta_race_match(
            self._df_bta,
            df_enriched,
            bta_baseline,
        )

        # Merge confidence back into df_bta
        confidence_map = df_validated[
            ["customer_id", "bta_id", "bta_match_confidence",
             "bta_race_validation", "needs_custom_archetype"]
        ].drop_duplicates(subset=["customer_id", "bta_id"])

        self._df_bta = self._df_bta.merge(
            confidence_map,
            on=["customer_id", "bta_id"],
            how="left",
            suffixes=("", "_zip"),
        )
        if "bta_match_confidence_zip" in self._df_bta.columns:
            mask = self._df_bta["bta_match_confidence_zip"].notna()
            self._df_bta.loc[mask, "bta_match_confidence"] = \
                self._df_bta.loc[mask, "bta_match_confidence_zip"]
            self._df_bta.drop(columns=["bta_match_confidence_zip"], inplace=True)

        self._df_bta.to_parquet(assign_path, index=False)

        # ── Case C — LLM custom archetype builder ─────────────────────────────
        case_c = df_validated[
            (df_validated["needs_custom_archetype"] == True) &
            (df_validated["bta_id"].notna())
        ]

        case_c_cells = (
            case_c.groupby(["cluster_id", "bta_id"])
            .agg(
                n_customers=("customer_id", "count"),
                zip_income=("cluster_zip_income_tier",
                            lambda x: x.mode().iloc[0] if not x.mode().empty else None),
                zip_race=("cluster_zip_race_eth",
                          lambda x: x.mode().iloc[0] if not x.mode().empty else None),
            )
            .reset_index()
        )

        custom_archetypes = []
        if len(case_c_cells) > 0:
            print(f"[ingestor] Step 4.5: {len(case_c_cells)} Case C cells — building LLM custom archetypes...")

            for _, cell in case_c_cells.iterrows():
                cluster_id = int(cell["cluster_id"])
                bta_id     = cell["bta_id"]

                cluster_signals = {
                    "zip_inferred_race_eth":    cell.get("zip_race"),
                    "zip_inferred_income_tier": cell.get("zip_income"),
                    "cluster_id":               cluster_id,
                    "matched_bta":              bta_id,
                    "conflict_type":            "income_conflict",
                    "company_context": (
                        f"{self.session.company.name if self.session.company else 'Unknown'} — "
                        f"{self.sector or 'general'} sector"
                    ),
                }

                archetype = build_custom_archetype(
                    cluster_id      = cluster_id,
                    cluster_signals = cluster_signals,
                    session         = self.session,
                    bta_baseline    = bta_baseline,
                )
                custom_archetypes.append(archetype)
                print(f"[ingestor]   ✓ Cluster {cluster_id} x {bta_id}: {archetype.get('archetype_name')}")

        # Save custom archetypes
        self.enriched_dir.mkdir(parents=True, exist_ok=True)
        custom_path = self.enriched_dir / "custom_archetypes.json"
        with open(custom_path, "w") as f:
            json.dump(custom_archetypes, f, indent=2, default=_json_serializer)

        # Write flag
        enrichment_flag.touch()

        # Summary
        if df_validated["bta_id"].notna().any():
            matched    = df_validated[df_validated["bta_id"].notna()]
            conf_counts = matched["bta_match_confidence"].value_counts().to_dict()
            print(f"[ingestor] Step 4.5: ✓ ZIP enrichment complete")
            print(f"[ingestor]   Confidence distribution : {conf_counts}")
            print(f"[ingestor]   Case C archetypes built : {len(custom_archetypes)}")

        return self._df_bta

    def build_ta_cards(self, force: bool = False) -> list[dict]:
        """
        Step 5 — Build TA cards for candidate cells.

        One TA card per (cluster × BTA) cell that passed the threshold.
        Each TA card has the same schema as a BTA card but is enriched
        with company-specific behavioral signals.

        Returns:
            List of TA card dicts.
        """
        ta_path = self.enriched_dir / "ta_cards.parquet"

        if ta_path.exists() and not force:
            print(f"[ingestor] Step 5: Loading existing TA cards...")
            df_ta = pd.read_parquet(ta_path)
            self._ta_cards = df_ta.to_dict("records")
            print(f"[ingestor] Step 5: ✓ Loaded {len(self._ta_cards)} TA cards")
            return self._ta_cards

        print(f"[ingestor] Step 5: Building TA cards...")

        # ── Load dependencies ─────────────────────────────────────────────────
        candidates_path = self.bta_dir / "candidate_tas.json"
        with open(candidates_path) as f:
            candidate_tas = json.load(f)

        if self._df_norm is None:
            self._df_norm = pd.read_parquet(
                self.normalized_dir / "normalized_records.parquet"
            )
        if self._df_bta is None:
            self._df_bta = pd.read_parquet(
                self.bta_dir / "bta_assignments.parquet"
            )

        bta_baseline = self._load_bta_baseline()

        # ── Build one TA card per candidate cell ──────────────────────────────
        self._ta_cards = []
        df_merged = self._df_norm.merge(
            self._df_bta[["customer_id", "cluster_id", "bta_id", "match_method",
                           "match_score", "match_level"]],
            on="customer_id",
            how="left",
        )

        for cell in candidate_tas:
            cluster_id = cell["cluster_id"]
            bta_id     = cell["bta_id"]
            cell_size  = cell["customer_count"]

            # Customers in this cell
            mask    = (
                (df_merged["cluster_id"] == cluster_id) &
                (df_merged["bta_id"]     == bta_id)
            )
            cell_df = df_merged[mask]

            if cell_df.empty:
                continue

            ta_card = self._build_single_ta_card(
                cluster_id  = cluster_id,
                bta_id      = bta_id,
                cell_df     = cell_df,
                cell_size   = cell_size,
                bta_baseline = bta_baseline,
            )
            self._ta_cards.append(ta_card)

        # ── Save ──────────────────────────────────────────────────────────────
        self.enriched_dir.mkdir(parents=True, exist_ok=True)

        df_ta = pd.DataFrame(self._ta_cards)
        df_ta.to_parquet(ta_path, index=False)
        df_ta.to_csv(self.enriched_dir / "ta_cards.csv", index=False)

        # JSONL for ChromaDB session-scoped collection
        # Use _json_serializer to handle numpy types from parquet-loaded BTA fields
        jsonl_path = self.enriched_dir / "session_ta_corpus.jsonl"
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for card in self._ta_cards:
                f.write(json.dumps(card, default=_json_serializer) + "\n")

        print(f"[ingestor] Step 5: ✓ {len(self._ta_cards)} TA cards built")
        for card in self._ta_cards:
            print(f"[ingestor]   {card['ta_id']} — "
                  f"{card['archetype_name']} | "
                  f"n={card['cell_size']} | "
                  f"coverage={card['coverage_score']:.2f} | "
                  f"confidence={card['confidence_tier']}")

        return self._ta_cards


    def build_behavioral_ta_cards(self, force: bool = False) -> list[dict]:
        """
        Step 5 (behavioral mode) — Build TA cards from behavioral clusters only.

        Used when analysis_mode == "behavioral" (bta_eligible_count == 0).
        No BTA matching. All clusters get confidence_case = "BEH".
        One TA card per cluster that passes the minimum cell size threshold.

        Returns:
            List of behavioral TA card dicts.
        """
        ta_path = self.enriched_dir / "ta_cards.parquet"

        if ta_path.exists() and not force:
            print(f"[ingestor] Step 5 (behavioral): Loading existing TA cards...")
            df_ta = pd.read_parquet(ta_path)
            self._ta_cards = df_ta.to_dict("records")
            print(f"[ingestor] Step 5 (behavioral): ✓ Loaded {len(self._ta_cards)} TA cards")
            return self._ta_cards

        print(f"[ingestor] Step 5 (behavioral): Building behavioral TA cards...")

        if self._df_norm is None:
            self._df_norm = pd.read_parquet(
                self.normalized_dir / "normalized_records.parquet"
            )
        if self._df_cluster is None:
            self._df_cluster = pd.read_parquet(
                self.clustering_dir / "cluster_assignments.parquet"
            )

        total = len(self._df_norm)
        min_cell = min(MIN_CELL_ABS, int(total * MIN_CELL_PCT))

        # Merge cluster assignments into normalized records
        df_merged = self._df_norm.merge(
            self._df_cluster[["customer_id", "cluster_id"]],
            on="customer_id",
            how="left",
        )

        self._ta_cards = []
        cluster_ids = sorted(df_merged["cluster_id"].dropna().unique())

        for cluster_id in cluster_ids:
            cluster_id = int(cluster_id)
            cell_df = df_merged[df_merged["cluster_id"] == cluster_id]
            cell_size = len(cell_df)

            if cell_size < min_cell:
                print(f"[ingestor]   Cluster {cluster_id}: below threshold "
                      f"({cell_size} < {min_cell}) — skipped")
                continue

            ta_card = self._build_behavioral_ta_card(
                cluster_id = cluster_id,
                cell_df    = cell_df,
                cell_size  = cell_size,
                total      = total,
            )
            self._ta_cards.append(ta_card)

        # ── Save ──────────────────────────────────────────────────────────────
        self.enriched_dir.mkdir(parents=True, exist_ok=True)

        # Also write a stub candidate_tas.json so downstream code doesn't break
        self.bta_dir.mkdir(parents=True, exist_ok=True)
        stub_candidates = [
            {"cluster_id": card["cluster_id"], "bta_id": None,
             "customer_count": card["cell_size"]}
            for card in self._ta_cards
        ]
        with open(self.bta_dir / "candidate_tas.json", "w") as f:
            json.dump(stub_candidates, f, indent=2)

        df_ta = pd.DataFrame(self._ta_cards)
        df_ta.to_parquet(ta_path, index=False)
        df_ta.to_csv(self.enriched_dir / "ta_cards.csv", index=False)

        jsonl_path = self.enriched_dir / "session_ta_corpus.jsonl"
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for card in self._ta_cards:
                f.write(json.dumps(card, default=_json_serializer) + "\n")

        print(f"[ingestor] Step 5 (behavioral): ✓ {len(self._ta_cards)} TA cards built")
        for card in self._ta_cards:
            print(f"[ingestor]   {card['ta_id']} — "
                  f"{card['archetype_name']} | "
                  f"n={card['cell_size']} | "
                  f"confidence={card['confidence_tier']}")

        return self._ta_cards


    def _build_behavioral_ta_card(
        self,
        cluster_id: int,
        cell_df:    pd.DataFrame,
        cell_size:  int,
        total:      int,
    ) -> dict:
        """
        Build a single behavioral TA card from a cluster — no BTA matching.

        Uses the same schema as a standard TA card but:
        - source_bta_id = None
        - archetype_name = behavioral cluster description
        - confidence_case = "BEH"
        - bta_match_confidence = "behavioral"
        - structural_profile = derived from behavioral medians/modes
        - psych/media fields = None (will be LLM-generated in prefilter)
        """
        from datetime import datetime, timezone

        # ── Behavioral signal aggregates ──────────────────────────────────────
        behavioral_signals = {}
        numeric_fields = [
            "ltv", "mrr", "arr", "churn_risk_score", "sessions_last_30d",
            "sessions_last_90d", "days_since_active", "nps_score",
            "feature_adoption_count", "total_purchases", "purchases_last_30d",
            "avg_order_value", "support_tickets_total", "support_tickets_90d",
            "onboarding_completion_pct", "email_open_rate", "email_click_rate",
            "discount_usage_pct", "avg_review_score", "days_since_purchase",
        ]
        for field in numeric_fields:
            if field in cell_df.columns:
                vals = cell_df[field].dropna()
                if not vals.empty:
                    behavioral_signals[f"{field}_median"] = round(float(vals.median()), 4)
                    behavioral_signals[f"{field}_mean"]   = round(float(vals.mean()), 4)

        # Categorical dominants
        categorical_fields = [
            "subscription_status", "subscription_plan", "preferred_channel",
            "churn_risk_tier", "lifecycle_stage",
        ]
        for field in categorical_fields:
            if field in cell_df.columns:
                vals = cell_df[field].dropna()
                if not vals.empty:
                    behavioral_signals[f"{field}_dominant"] = str(vals.mode().iloc[0])

        # ── Structural profile (behavioral) ───────────────────────────────────
        # Use available demographic fields if present, otherwise note absence
        structural_parts = []
        for field, label in [
            ("marital_status", "marital"), ("gender", "sex"),
            ("education", "education"), ("housing_tenure", "tenure"),
        ]:
            if field in cell_df.columns:
                vals = cell_df[field].dropna()
                if not vals.empty:
                    structural_parts.append(f"{label}={vals.mode().iloc[0]}")

        structural_profile = (
            "Behavioral cluster — demographic baseline not available. "
            + (f"Available signals: {', '.join(structural_parts)}." if structural_parts
               else "Analysis based on behavioral signals only.")
        )

        # ── Coverage ──────────────────────────────────────────────────────────
        coverage_score = 0.0
        if "coverage_score" in cell_df.columns:
            coverage_score = round(float(cell_df["coverage_score"].mean()), 4)

        # ── Archetype name (plain behavioral description) ─────────────────────
        churn_risk = behavioral_signals.get("churn_risk_score_mean")
        ltv        = behavioral_signals.get("ltv_median")
        mrr        = behavioral_signals.get("mrr_median")
        sub_status = behavioral_signals.get("subscription_status_dominant", "unknown")

        desc_parts = []
        if churn_risk is not None:
            tier = "high" if churn_risk > 0.6 else ("medium" if churn_risk > 0.3 else "low")
            desc_parts.append(f"{tier} churn risk")
        if mrr is not None:
            desc_parts.append(f"MRR ${mrr:.0f}")
        if ltv is not None:
            desc_parts.append(f"LTV ${ltv:.0f}")
        if sub_status != "unknown":
            desc_parts.append(sub_status)

        archetype_name = (
            f"Behavioral Cluster {cluster_id}"
            + (f" — {', '.join(desc_parts)}" if desc_parts else "")
        )

        ta_id = f"CS{cluster_id:02d}_BEH"

        return {
            "ta_id":                    ta_id,
            "source_bta_id":            None,
            "cluster_id":               cluster_id,
            "archetype_name":           archetype_name,
            "cell_size":                cell_size,
            "pct_of_dataset":           round(cell_size / total, 4),
            "dominant_age_bin":         None,
            "dominant_sex_label":       None,
            "dominant_race_eth":        None,
            "dominant_edu_tier":        None,
            "dominant_emp_tier":        None,
            "dominant_household_income_tier": None,
            "dominant_income_tier":     None,
            "dominant_mar_tier":        None,
            "dominant_tenure":          None,
            "structural_profile":       structural_profile,
            "psych_signals":            None,
            "psych_summary":            None,
            "motivational_drivers":     None,
            "key_barriers":             None,
            "trust_cues":               None,
            "susceptibility_notes":     None,
            "media_signals":            None,
            "media_summary":            None,
            "channel_implications":     None,
            "behavioral_signals":       behavioral_signals,
            "messaging_implications":   None,
            "offer_implications":       None,
            "coverage_score":           coverage_score,
            "confidence_tier":          "low",
            "bta_match_score":          0.0,
            "match_methods":            {"behavioral_only": cell_size},
            "data_source":              "company_data_only",
            "has_business_behavioral":  True,
            "created_at":               datetime.now(timezone.utc).isoformat(),
            "session_id":               self.session.session_id,
            "bta_match_confidence":     "behavioral",
            "is_ambiguous_bta":         False,
            "ambiguity_group":          None,
            "competing_bta_ids":        None,
            "ambiguity_note":           None,
            "zip_inferred_income_tier": None,
            "zip_inferred_race_eth":    None,
            "bta_race_validation":      None,
            # Behavioral mode flag — read by prefilter and generator
            "confidence_case":          "BEH",
        }


    def save(self) -> None:
        """
        Step 6 — Update session.proprietary_data and finalize.
        """
        print(f"[ingestor] Step 6: Finalizing session...")

        if self._ta_cards is None:
            print(f"[ingestor] Step 6: ⚠ No TA cards found — "
                  f"run build_ta_cards() first")
            return

        # Load cluster stats
        stats_path = self.clustering_dir / "cluster_stats.json"
        cluster_stats = {}
        if stats_path.exists():
            with open(stats_path) as f:
                cluster_stats = json.load(f)

        # Coverage stats per BTA
        coverage_stats: dict[str, dict] = {}
        for card in self._ta_cards:
            coverage_stats[card["ta_id"]] = {
                "cell_size":      card["cell_size"],
                "coverage_score": card["coverage_score"],
                "confidence_tier": card["confidence_tier"],
            }

        # Segment mapping — customer → TA id
        # Only map to TA IDs that actually exist as generated TA cards.
        # Customers in cells that failed the threshold have no valid TA card
        # and must not reference a non-existent TA ID.
        valid_ta_ids: set[str] = {
            card["ta_id"] for card in (self._ta_cards or [])
        }

        segment_mapping: dict[str, str] = {}
        if self._df_bta is not None:
            for _, row in self._df_bta.iterrows():
                cid   = int(row.get("cluster_id", -1))
                bta   = row.get("bta_id")
                ta_id = f"CS{cid:02d}_{bta}" if bta else None
                if ta_id and ta_id in valid_ta_ids:
                    segment_mapping[str(row["customer_id"])] = ta_id
                # Customers in thin cells or non-US: no segment mapping entry
        elif self.session.analysis_mode == "behavioral" and self._df_cluster is not None:
            # Behavioral mode — map customers to behavioral TA IDs
            for _, row in self._df_cluster.iterrows():
                cid   = int(row.get("cluster_id", -1))
                ta_id = f"CS{cid:02d}_BEH"
                if ta_id in valid_ta_ids:
                    segment_mapping[str(row["customer_id"])] = ta_id

        try:
            from mk_intel_session import ProprietaryDataset
        except ImportError:
            from mk_intel.mk_intel_session import ProprietaryDataset

        self.session.proprietary_data = ProprietaryDataset(
            uploaded_files    = [str(p) for p in self.raw_dir.iterdir()],
            normalized        = True,
            compliance_mode   = self.compliance_mode,
            sector            = self.sector,
            segment_mapping   = segment_mapping,
            coverage_stats    = coverage_stats,
            ingest_notes      = (
                f"Ingestion complete. "
                f"Analysis mode: {self.session.analysis_mode}. "
                f"{cluster_stats.get('k', '?')} behavioral clusters. "
                f"{len(self._ta_cards)} TA cards generated. "
                f"Session dir: {self.session_dir}"
            ),
            confidence        = self._overall_confidence(),
        )

        print(f"[ingestor] Step 6: ✓ Session updated")
        print(f"[ingestor] ══════════════════════════════════════════")
        print(f"[ingestor] Ingestion complete")
        print(f"[ingestor]   TA cards: {len(self._ta_cards)}")
        print(f"[ingestor]   Session dir: {self.session_dir}")
        print(f"[ingestor] ══════════════════════════════════════════\n")


    # ── Internal helpers ──────────────────────────────────────────────────────

    def _make_session_dir(self) -> Path:
        """
        Create and return the session-scoped company data directory.

        Tied deterministically to session.session_id so a crashed pipeline
        can always resume — re-instantiate with the same session and the
        ingestor finds the same directory automatically.

        Format: data/company_data/{slug}_{session_id[:8]}/
        """
        short_sid = self.session.session_id[:8]
        dirname   = f"{self.slug}_{short_sid}"
        path      = self.company_data_root / dirname
        path.mkdir(parents=True, exist_ok=True)
        return path


    def _load_bta_baseline(self) -> pd.DataFrame:
        """Load BTA baseline parquet for structural matching."""
        if self._bta_baseline is not None:
            return self._bta_baseline

        # Search for BTA baseline relative to project root
        bta_path = Path("/app/bta_data") / BTA_BASELINE_FILENAME
        if not bta_path.exists():
            project_root = Path(__file__).resolve().parents[2]
            bta_path = project_root / "data" / "societal_processed" / "bta_cards" / BTA_BASELINE_FILENAME

        if not bta_path.exists():
            raise FileNotFoundError(
                f"BTA baseline not found at: {bta_path}\n"
                f"Run notebooks 01-11 first to generate the BTA baseline."
            )

        self._bta_baseline = pd.read_parquet(bta_path)
        return self._bta_baseline


    def _infer_excluded_features(self) -> dict[str, dict]:
        """
        Infer which fields to exclude from clustering based on session OBJ/SOBJs.

        Three gates applied in order:

        Gate 1 — Always excluded (outcome labels + pipeline metadata)
            Hard rule, no exceptions, no analyst override.

        Gate 2 — SOBJ-context exclusion
            Reads approved SOBJs. If churn/retention vocabulary detected,
            excludes churn_risk_score. Other context-specific exclusions
            follow the SOBJ_CONTEXT_EXCLUSIONS map.
            If no SOBJs set, Gate 2 does not apply — no assumptions made.

        Gate 3 — Numeric low-cardinality exclusion
            Applied in _check_feature_quality() on actual data.
            Documented here for completeness.

        Returns:
            Dict of {field: {"gate": int, "reason": str}}
            for all excluded fields across Gates 1 and 2.
            Gate 3 exclusions are added by _check_feature_quality().
        """
        excluded: dict[str, dict] = {}

        # Gate 1 — always excluded
        for field in CLUSTERING_ALWAYS_EXCLUDED:
            excluded[field] = {
                "gate":   1,
                "reason": "outcome_label_or_pipeline_metadata",
            }

        # Gate 2 — SOBJ-context exclusion
        sobjs = []
        if self.session and hasattr(self.session, "get_approved_sobjs"):
            sobjs = self.session.get_approved_sobjs()

        if sobjs:
            statements = " ".join([s.statement.lower() for s in sobjs])
            gate2_excluded: set[str] = set()

            for keyword, fields in SOBJ_CONTEXT_EXCLUSIONS.items():
                if keyword in statements:
                    for field in fields:
                        if field not in excluded:
                            gate2_excluded.add(field)

            for field in gate2_excluded:
                excluded[field] = {
                    "gate":   2,
                    "reason": "sobj_context_outcome_adjacent",
                    "sobj_keywords_matched": [
                        kw for kw, flds in SOBJ_CONTEXT_EXCLUSIONS.items()
                        if kw in statements and field in flds
                    ],
                    "post_hoc_use": "cluster_profiling",
                }
                print(f"[ingestor]   Gate 2: excluding '{field}' — "
                      f"outcome-adjacent given OBJ context. "
                      f"Will be used post-hoc for cluster profiling.")
        else:
            print(f"[ingestor]   Gate 2: no approved SOBJs — "
                  f"context exclusions skipped")

        return excluded


    def _check_feature_quality(
        self,
        df: pd.DataFrame,
        features: list[str],
        gate1_2_excluded: dict[str, dict],
    ) -> tuple[list[str], dict[str, dict]]:
        """
        Gate 3 — Numeric low-cardinality check.

        Excludes numeric fields that have fewer than NUMERIC_MIN_CARDINALITY
        unique non-null values. These behave as step functions rather than
        continuous signals and will dominate cluster separation without
        adding meaningful behavioral nuance.

        Note: this applies to NUMERIC fields only. Categorical fields are
        already excluded from the clustering candidate list upstream.

        Args:
            df              : normalized DataFrame
            features        : candidate feature list (after Gates 1+2)
            gate1_2_excluded: exclusions already applied by Gates 1 and 2

        Returns:
            (eligible_features, all_excluded_dict)
        """
        all_excluded = dict(gate1_2_excluded)
        eligible     = []

        for feat in features:
            if feat not in df.columns:
                continue
            n_unique = df[feat].dropna().nunique()
            if n_unique < NUMERIC_MIN_CARDINALITY:
                all_excluded[feat] = {
                    "gate":     3,
                    "reason":   f"low_cardinality_numeric_{n_unique}_unique_values",
                    "post_hoc_use": "cluster_profiling",
                }
                print(f"[ingestor]   Gate 3: excluding '{feat}' — "
                      f"numeric field with only {n_unique} unique values "
                      f"(threshold: {NUMERIC_MIN_CARDINALITY}). "
                      f"Will be used post-hoc for cluster profiling.")
            else:
                eligible.append(feat)

        return eligible, all_excluded


    def _infer_behavioral_tier1(self) -> list[str]:
        """
        Dynamically infer Tier 1 behavioral features from approved SOBJs.

        Reads the SOBJ statements, matches against SOBJ_BEHAVIORAL_TIER1 keywords,
        and returns an ordered list of fields most relevant for clustering given
        the campaign objective.

        Falls back to the default BEHAVIORAL_CLUSTER_FEATURES[:5] if no match found.

        Returns:
            Ordered list of field names — most important first.
        """
        sobjs = []
        if self.session and hasattr(self.session, "get_approved_sobjs"):
            sobjs = self.session.get_approved_sobjs()

        if not sobjs:
            return BEHAVIORAL_CLUSTER_FEATURES[:5]

        statements = " ".join([s.statement.lower() for s in sobjs])

        # Find first matching keyword and return its Tier 1 list
        for keyword, tier1_fields in SOBJ_BEHAVIORAL_TIER1.items():
            if keyword in statements:
                print(f"[ingestor]   Behavioral Tier 1 inferred from SOBJ keyword '{keyword}': {tier1_fields}")
                return tier1_fields

        # No match — fall back to static Tier 1
        print(f"[ingestor]   Behavioral Tier 1: no SOBJ keyword match, using default Tier 1")
        return BEHAVIORAL_CLUSTER_FEATURES[:5]

    def _behavioral_features(
        self,
        df: pd.DataFrame,
    ) -> tuple[Optional[np.ndarray], list[str]]:
        """
        Select and z-score scale behavioral features for K-Means.

        Only includes features that:
            - exist as columns in the DataFrame
            - have <= 40% missing values
            - are numeric

        In behavioral-only mode, Tier 1 is dynamically inferred from the SOBJ
        signal map. In BTA mode, the static BEHAVIORAL_CLUSTER_FEATURES[:5] is used.

        Returns:
            (scaled_array, feature_names) or (None, []) if < 2 features available.
        """
        from sklearn.preprocessing import StandardScaler
        from sklearn.impute import SimpleImputer

        # ── Gates 1 + 2: infer excluded features from OBJ/SOBJ context ─────────
        gate1_2_excluded = self._infer_excluded_features()

        # ── Determine Tier 1 — static or SOBJ-dynamic ────────────────────────
        is_behavioral_mode = (
            hasattr(self.session, "analysis_mode") and
            self.session.analysis_mode == "behavioral"
        )
        tier1_fields = (
            self._infer_behavioral_tier1() if is_behavioral_mode
            else BEHAVIORAL_CLUSTER_FEATURES[:5]
        )

        # ── Missingness filter + Gate 1+2 exclusions ──────────────────────────
        available = []
        for feat in BEHAVIORAL_CLUSTER_FEATURES:
            if feat not in df.columns:
                continue
            if feat in gate1_2_excluded:
                continue  # Gate 1 or 2 exclusion — skip silently (already logged)
            missing_pct = df[feat].isna().mean()
            if missing_pct > MAX_MISSING_PCT:
                print(f"[ingestor]   Excluding '{feat}': "
                      f"{missing_pct:.1%} missing (> {MAX_MISSING_PCT:.0%} threshold)")
                continue
            available.append(feat)

        # ── Gate 3: numeric low-cardinality check ─────────────────────────────
        available, all_excluded = self._check_feature_quality(
            df, available, gate1_2_excluded
        )

        # ── Require at least 2 Tier-1 features ───────────────────────────────
        tier1 = [
            f for f in available
            if f in tier1_fields
        ]
        if len(tier1) < 2:
            return None, []

        print(f"[ingestor]   Clustering features: {available}")
        if all_excluded:
            post_hoc = [f for f, info in all_excluded.items()
                        if info.get('post_hoc_use') == 'cluster_profiling'
                        and f in df.columns]
            if post_hoc:
                print(f"[ingestor]   Post-hoc profiling fields: {post_hoc}")

        X = df[available].values.astype(float)

        # Impute missing values with median before scaling
        imputer = SimpleImputer(strategy="median")
        X       = imputer.fit_transform(X)

        # Z-score normalization
        scaler = StandardScaler()
        X      = scaler.fit_transform(X)

        return X, available


    def _auto_k(
        self,
        features: np.ndarray,
        n: int,
    ) -> tuple[int, dict]:
        """
        Automatically select k using silhouette score.

        Strategy:
            n <= 10k : silhouette on full data, k=2..8
            n > 10k  : silhouette on stratified sample (10k)
                       + inertia on full data
                       → choose k using both, sanity-check interpretability

        Returns:
            (best_k, stats_dict)
        """
        from sklearn.cluster import KMeans
        from sklearn.metrics import silhouette_score

        k_range = range(K_MIN, min(K_MAX, n // 10) + 1)
        if len(k_range) < 1:
            return 2, {"best_silhouette": 0.0, "method": "fallback_small_dataset"}

        silhouette_scores: dict[int, float] = {}
        inertias:          dict[int, float] = {}

        if n <= SILHOUETTE_FULL_THRESHOLD:
            # Full silhouette
            print(f"[ingestor]   Auto-k: silhouette on full data (n={n:,})")
            for k in k_range:
                km     = KMeans(n_clusters=k, random_state=42, n_init=10)
                labels = km.fit_predict(features)
                sil    = silhouette_score(features, labels,
                                          sample_size=min(n, 5000),
                                          random_state=42)
                silhouette_scores[k] = sil
                inertias[k]          = km.inertia_
                print(f"[ingestor]   k={k}: silhouette={sil:.3f}, "
                      f"inertia={km.inertia_:.0f}")
            method = "silhouette_full"
        else:
            # Stratified sample for silhouette + full data for inertia
            sample_size = min(SILHOUETTE_FULL_THRESHOLD, n)
            idx_sample  = np.random.RandomState(42).choice(n, sample_size, replace=False)
            X_sample    = features[idx_sample]

            print(f"[ingestor]   Auto-k: silhouette on sample (n={sample_size:,}) "
                  f"+ inertia on full (n={n:,})")

            for k in k_range:
                # Silhouette on sample
                km_s   = KMeans(n_clusters=k, random_state=42, n_init=10)
                lbl_s  = km_s.fit_predict(X_sample)
                sil    = silhouette_score(X_sample, lbl_s,
                                          sample_size=min(sample_size, 5000),
                                          random_state=42)
                silhouette_scores[k] = sil

                # Inertia on full data
                km_f   = KMeans(n_clusters=k, random_state=42, n_init=10)
                km_f.fit(features)
                inertias[k] = km_f.inertia_

                print(f"[ingestor]   k={k}: silhouette={sil:.3f}, "
                      f"inertia={km_f.inertia_:.0f}")
            method = "silhouette_sample_inertia_full"

        best_k   = max(silhouette_scores, key=silhouette_scores.get)
        best_sil = silhouette_scores[best_k]

        stats = {
            "method":             method,
            "best_silhouette":    best_sil,
            "silhouette_scores":  silhouette_scores,
            "inertias":           inertias,
        }

        return best_k, stats


    def _build_cluster_profiles(
        self,
        df_norm: pd.DataFrame,
        cluster_labels: pd.Series,
    ) -> pd.DataFrame:
        """
        Compute dominant structural profile for each cluster.

        For each cluster:
            - Numeric structural fields: median
            - Categorical structural fields: mode (most common value)

        Returns:
            DataFrame with one row per cluster.
        """
        df = df_norm.copy()
        df["cluster_id"] = cluster_labels.values

        profiles = []
        for cid, group in df.groupby("cluster_id"):
            profile = {"cluster_id": cid, "cluster_size": len(group)}

            for field in STRUCTURAL_MATCH_FIELDS:
                if field not in group.columns:
                    profile[field] = None
                    continue
                non_null = group[field].dropna()
                if non_null.empty:
                    profile[field] = None
                else:
                    # All structural fields are categorical
                    profile[field] = non_null.mode().iloc[0]

            profiles.append(profile)

        return pd.DataFrame(profiles)


    def _structural_match(
        self,
        profile: dict,
        bta_baseline: pd.DataFrame,
    ) -> list[tuple[str, float]]:
        """
        Match a demographic profile to the nearest BTA(s).

        Uses weighted field comparison:
            - For each structural field present in profile,
              add its weight if it matches the BTA dominant value.
            - Normalize by total weight of present fields.
            - Return ALL BTAs that tied at the maximum score.

        When multiple BTAs tie (e.g. both BTA_00 and BTA_03 match
        age_bin=35-44 with no income data), all are returned.
        The caller creates one assignment row per tied BTA — same
        customer appears in multiple cells, all flagged as ambiguous.

        Args:
            profile      : dict with structural field values
            bta_baseline : BTA baseline DataFrame

        Returns:
            List of (bta_id, match_score) tuples.
            Normally one item. Multiple items when tied.
        """
        scores: dict[str, float] = {}

        for _, bta_row in bta_baseline.iterrows():
            bta_id         = bta_row["segment_id"]
            weighted_match = 0.0
            total_weight   = 0.0

            for field, weight in STRUCTURAL_WEIGHTS.items():
                profile_val = profile.get(field)
                if profile_val is None:
                    continue

                bta_field = f"dominant_{field}"
                bta_val   = bta_row.get(bta_field)

                if bta_val is None:
                    continue

                total_weight += weight
                if str(profile_val).strip() == str(bta_val).strip():
                    weighted_match += weight

            scores[bta_id] = (
                weighted_match / total_weight if total_weight > 0 else 0.0
            )

        # Fallback — no structural fields present
        if not scores or max(scores.values()) == 0.0:
            fallback_id = bta_baseline.iloc[0]["segment_id"]
            return [(fallback_id, 0.0)]

        best_score = max(scores.values())
        tied_btas  = [
            (bta_id, round(score, 4))
            for bta_id, score in scores.items()
            if score == best_score
        ]

        return tied_btas


    def _build_single_ta_card(
        self,
        cluster_id:   int,
        bta_id:       str,
        cell_df:      pd.DataFrame,
        cell_size:    int,
        bta_baseline: pd.DataFrame,
    ) -> dict:
        """
        Build one TA card for a (cluster × BTA) cell.

        The TA card has the same schema as a BTA card with:
            - BTA baseline fields inherited as the prior
            - Business behavioral signals overriding/augmenting
              where company data is present and richer
            - Coverage and confidence metadata
            - Source BTA reference

        TA ID format: CS{cluster_id:02d}_{bta_id}
        e.g. CS02_BTA_03
        """
        ta_id = f"CS{cluster_id:02d}_{bta_id}"

        # ── Get BTA baseline row ──────────────────────────────────────────────
        bta_row = bta_baseline[bta_baseline["segment_id"] == bta_id]
        if bta_row.empty:
            bta_data = {}
        else:
            bta_data = bta_row.iloc[0].to_dict()

        # ── Aggregate behavioral signals from cell customers ──────────────────
        behavioral = self._aggregate_behavioral_signals(cell_df)

        # ── Coverage ──────────────────────────────────────────────────────────
        avg_coverage = cell_df["coverage_score"].mean() if "coverage_score" in cell_df.columns else 0.0
        conf_counts  = (
            cell_df["confidence_tier"].value_counts().to_dict()
            if "confidence_tier" in cell_df.columns else {}
        )
        dominant_conf = max(conf_counts, key=conf_counts.get) if conf_counts else "low"

        # ── Match quality ─────────────────────────────────────────────────────
        avg_match = cell_df["match_score"].mean() if "match_score" in cell_df.columns else 0.0
        methods   = (
            cell_df["match_level"].value_counts().to_dict()
            if "match_level" in cell_df.columns else {}
        )

        # ── Build TA card ─────────────────────────────────────────────────────
        # ── Ambiguity metadata from cell ─────────────────────────────────────
        cell_bta_rows = (
            self._df_bta[
                (self._df_bta["cluster_id"] == cluster_id) &
                (self._df_bta["bta_id"] == bta_id)
            ]
            if self._df_bta is not None else pd.DataFrame()
        )
        is_tied         = bool(cell_bta_rows["is_tied"].any()) if not cell_bta_rows.empty and "is_tied" in cell_bta_rows.columns else False
        ambiguity_group = cell_bta_rows["ambiguity_group"].iloc[0] if not cell_bta_rows.empty and "ambiguity_group" in cell_bta_rows.columns else None
        competing_btas  = cell_bta_rows["competing_bta_ids"].iloc[0] if not cell_bta_rows.empty and "competing_bta_ids" in cell_bta_rows.columns else None

        # If the cell is tied, confidence is always low — regardless of
        # individual match scores. A tied cell is ambiguous by definition.
        if is_tied:
            match_confidence = "low"
        elif not cell_bta_rows.empty and "bta_match_confidence" in cell_bta_rows.columns:
            match_confidence = cell_bta_rows["bta_match_confidence"].mode().iloc[0]
        else:
            match_confidence = "medium"

        ta_card = {
            # ── Identification ────────────────────────────────────────────────
            "ta_id":             ta_id,
            "source_bta_id":     bta_id,
            "cluster_id":        cluster_id,
            "archetype_name":    (
                f"{bta_data.get('archetype_name', bta_id)} "
                f"[Cluster {cluster_id}]"
                + (" ⚠ AMBIGUOUS" if is_tied else "")
            ),

            # ── Population metadata ───────────────────────────────────────────
            "cell_size":         cell_size,
            "pct_of_dataset":    round(cell_size / len(self._df_norm), 4),

            # ── Structural profile (from BTA baseline) ────────────────────────
            "dominant_age_bin":         bta_data.get("dominant_age_bin"),
            "dominant_sex_label":       bta_data.get("dominant_sex_label"),
            "dominant_race_eth":        bta_data.get("dominant_race_eth"),
            "dominant_edu_tier":        bta_data.get("dominant_edu_tier"),
            "dominant_emp_tier":        bta_data.get("dominant_emp_tier"),
            "dominant_household_income_tier": bta_data.get("dominant_household_income_tier"),
            "dominant_income_tier":      bta_data.get("dominant_income_tier"),
            "dominant_mar_tier":        bta_data.get("dominant_mar_tier"),
            "dominant_tenure":          bta_data.get("dominant_tenure"),
            "structural_profile":       bta_data.get("structural_profile"),

            # ── Psychological layer (from BTA — business data may refine) ─────
            "psych_signals":        bta_data.get("psych_signals"),
            "psych_summary":        bta_data.get("psych_summary"),
            "motivational_drivers": bta_data.get("motivational_drivers"),
            "key_barriers":         bta_data.get("key_barriers"),
            "trust_cues":           bta_data.get("trust_cues"),
            "susceptibility_notes": bta_data.get("susceptibility_notes"),

            # ── Media layer (from BTA baseline) ──────────────────────────────
            "media_signals":          bta_data.get("media_signals"),
            "media_summary":          bta_data.get("media_summary"),
            "channel_implications":   bta_data.get("channel_implications"),

            # ── Business behavioral signals (from company data) ───────────────
            # These override BTA defaults where present and richer
            "behavioral_signals":     behavioral,

            # Messaging and offer implications enriched by business data
            "messaging_implications": bta_data.get("messaging_implications"),
            "offer_implications":     bta_data.get("offer_implications"),

            # ── Coverage and confidence ───────────────────────────────────────
            "coverage_score":         round(avg_coverage, 4),
            "confidence_tier":        dominant_conf,
            "bta_match_score":        round(avg_match, 4),
            "match_methods":          methods,

            # ── Data source flags ─────────────────────────────────────────────
            "data_source":            "bta_baseline_plus_business_data",
            "has_business_behavioral": len(behavioral) > 0,
            "created_at":             datetime.now(timezone.utc).isoformat(),
            "session_id":             self.session.session_id,

            # ── BTA assignment confidence and ambiguity ───────────────────────
            "bta_match_confidence":   match_confidence,
            "is_ambiguous_bta":       is_tied,
            "ambiguity_group":        ambiguity_group,
            "competing_bta_ids":      competing_btas,
            "ambiguity_note":         (
                f"BTA assignment is ambiguous — customer profile matches "
                f"{competing_btas} equally. "
                f"Income or housing data would resolve this. "
                f"Both TAs are generated — review TAR scores before acting."
                if is_tied else None
            ),

            # ── ZIP enrichment signals (cell-level modal values) ──────────────
            # Carried forward from normalized records for TAR pre-filter use.
            "zip_inferred_income_tier": (
                cell_df["zip_inferred_income_tier"].mode().iloc[0]
                if "zip_inferred_income_tier" in cell_df.columns
                and cell_df["zip_inferred_income_tier"].notna().any()
                else None
            ),
            "zip_inferred_race_eth": (
                cell_df["zip_inferred_race_eth"].mode().iloc[0]
                if "zip_inferred_race_eth" in cell_df.columns
                and cell_df["zip_inferred_race_eth"].notna().any()
                else None
            ),
            "bta_race_validation": (
                cell_bta_rows["bta_race_validation"].mode().iloc[0]
                if not cell_bta_rows.empty
                and "bta_race_validation" in cell_bta_rows.columns
                else "not_available"
            ),
        }

        return ta_card


    def _aggregate_behavioral_signals(self, cell_df: pd.DataFrame) -> dict:
        """
        Aggregate behavioral signals for a cell's customers.

        Returns a dict of aggregated signals that characterize the
        behavioral cluster — these override BTA baseline defaults
        where company data is present.

        Aggregations:
            Numeric fields: median (robust to outliers)
            Categorical fields: mode (most common value)
            Rate fields: mean
        """
        signals = {}

        numeric_fields = [
            "sessions_last_30d", "sessions_last_90d", "days_since_active",
            "ltv", "mrr", "arr", "total_purchases", "avg_order_value",
            "feature_adoption_count", "nps_score", "support_tickets_90d",
            "churn_risk_score", "onboarding_completion_pct",
            "email_open_rate", "email_click_rate",
            "avg_monthly_balance", "product_count", "credit_limit_utilization",
        ]
        rate_fields = [
            "churn_risk_score", "onboarding_completion_pct",
            "email_open_rate", "email_click_rate", "credit_limit_utilization",
        ]
        categorical_fields = [
            "subscription_status", "subscription_plan",
            "lifecycle_stage", "churn_risk_tier", "nps_tier",
            "feature_adoption_tier", "account_type", "account_status",
        ]

        for field in numeric_fields:
            if field not in cell_df.columns:
                continue
            series = cell_df[field].dropna()
            if series.empty:
                continue
            agg_fn = "mean" if field in rate_fields else "median"
            signals[f"{field}_{agg_fn}"] = round(
                float(series.mean() if agg_fn == "mean" else series.median()), 4
            )

        for field in categorical_fields:
            if field not in cell_df.columns:
                continue
            series = cell_df[field].dropna()
            if series.empty:
                continue
            signals[f"{field}_dominant"] = series.mode().iloc[0]

        return signals


    def _overall_confidence(self) -> str:
        """Derive overall ingestion confidence from TA card coverage scores."""
        if not self._ta_cards:
            return "low"
        avg_cov = sum(c.get("coverage_score", 0) for c in self._ta_cards) / len(self._ta_cards)
        if avg_cov >= 0.6:
            return "high"
        elif avg_cov >= 0.3:
            return "medium"
        return "low"


# ── Module-level helpers ──────────────────────────────────────────────────────

def _make_company_slug(name: str) -> str:
    """Convert company name to snake_case slug."""
    import re
    import unicodedata
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name).strip("_")
    name = re.sub(r"_+", "_", name)
    return name


def _json_serializer(obj):
    """
    JSON serializer for types not handled by the default encoder.

    Handles:
        numpy scalars  (np.float64, np.int64 etc.) → native Python float/int
        numpy arrays   (ndarray) → Python list
        pandas NA/NaT  → None
        date/datetime  → ISO string

    Used as the default= argument to json.dumps() when writing JSONL.
    """
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    try:
        import pandas as pd
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
