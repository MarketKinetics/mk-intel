"""
mk_tar_generator.py
===================
MK Intel — TAR Pre-Filter and Profile Refinement

Orchestrates the full TAR generation pipeline for a session:

    Stage 1 — BTA profile refinement (A/B/C cases)
        Refines each TA card's descriptive layer based on ZIP confidence
        case and company context. Company data always wins. LLM fills
        gaps and contextualizes — never overrides real signals.

        Case A — Full alignment:
            LLM refines psych/media/messaging to company/product context.
            All structural fields locked from data.

        Case B.1 — Income diverges:
            LLM adjusts income-related descriptors only (spending capacity,
            price sensitivity, financial motivations) using ZIP-inferred
            household income as ground truth. All other fields locked.

        Case B.2 — Race diverges:
            LLM adjusts cultural/media/psychographic layer only using
            ZIP-inferred demographic context. All structural fields locked.

        Case C — Full conflict:
            Already LLM-generated during ingestion. Skip refinement.
            Apply confidence penalty. Pass through as-is.

    Stage 2 — Pre-filter (rule-based + LLM fallback)
        For each (refined TA x SOBJ) combination, score likelihood that
        this TA will perform the desired behavior. Rule engine maps SOBJ
        vocabulary to behavioral and structural signals already on the
        TA card. LLM fallback for unmatched SOBJ patterns.

        Only (TA x SOBJ) pairs that pass the likelihood threshold proceed
        to full TAR generation.

──────────────────────────────────────────────────────────────────
Ground truth hierarchy
──────────────────────────────────────────────────────────────────

    1. Company data  — age, income, structural fields from business dataset
    2. ZIP signals   — household income, race/eth from ZCTA (standard mode only)
    3. BTA baseline  — population-level prior from ACS/GSS/Pew
    4. LLM inference — fills gaps only; stays at segment level

LLM output stays at population segment level. No price points,
no specific product references, no tactical predictions.
Those belong in the TAR, not the profile.

──────────────────────────────────────────────────────────────────
Compliance
──────────────────────────────────────────────────────────────────

    standard    : all signals available
    banking_us  : age, income, ZIP signals excluded from rules and prompts
    banking_eu  : age, income, ZIP, marital excluded from rules and prompts
    eu_gdpr     : ZIP signals excluded from rules and prompts
    all modes   : race/eth never used as direct targeting signal
                  gender never used as filtering signal in any mode

──────────────────────────────────────────────────────────────────
Public API
──────────────────────────────────────────────────────────────────

    MKTARGenerator(session, compliance_mode, sector)
        Initialize the generator for a session.

    generator.run(ta_cards, sobjs, company_context)
        Run the full pre-filter pipeline.
        Returns list of TARCandidate objects ready for TAR generation.

    Individual stages also callable directly:
        generator.refine_profiles(ta_cards, company_context)
        generator.prefilter(refined_profiles, sobjs)

    build_company_context(session)
        Helper to build company context string from session.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from mk_intel_session import MKSession


# ── Compliance signal exclusions ──────────────────────────────────────────────
# Fields excluded from rule-based scoring and LLM prompts per compliance mode.
# Race/eth is never a direct targeting signal in any mode.
# Gender is never a filtering signal in any mode.

COMPLIANCE_EXCLUDED_SIGNALS: dict[str, set[str]] = {
    "standard":   set(),
    "banking_us": {
        "age_bin", "dominant_age_bin",
        "income_tier", "dominant_income_tier", "dominant_household_income_tier",
        "zip_inferred_income_tier", "zip_inferred_race_eth", "zip_race_eth_confidence",
    },
    "banking_eu": {
        "age_bin", "dominant_age_bin",
        "income_tier", "dominant_income_tier", "dominant_household_income_tier",
        "zip_inferred_income_tier", "zip_inferred_race_eth", "zip_race_eth_confidence",
        "marital_status", "dominant_mar_tier",
    },
    "eu_gdpr": {
        "zip_inferred_income_tier", "zip_inferred_race_eth", "zip_race_eth_confidence",
    },
}

# Never used as direct targeting signals regardless of compliance mode
NEVER_TARGET_SIGNALS: set[str] = {
    "gender", "dominant_sex_label",
    "dominant_race_eth", "zip_inferred_race_eth",
}

# ── SOBJ vocabulary to signal map ─────────────────────────────────────────────
# Maps SOBJ keyword patterns to behavioral/structural signals on the TA card.
# direction: "higher_better" = high value means higher likelihood for this SOBJ
#            "lower_better"  = low value means higher likelihood for this SOBJ
# match: categorical match — score 1.0 if value equals match, else 0.0

SOBJ_SIGNAL_MAP: dict[str, list[dict]] = {
    # Retention — active customers at risk of churning
    # No hard gate: churn_risk_score may be binary (churn rate) or continuous
    # (propensity score) — too ambiguous to gate. Use as soft signal only.
    "churn": [
        {"field": "churn_risk_score_mean",       "direction": "higher_better", "weight": 0.35},
        {"field": "ltv_median",                  "direction": "higher_better", "weight": 0.25},
        {"field": "sessions_last_30d_median",    "direction": "higher_better", "weight": 0.20},
        {"field": "subscription_status_dominant","match": "active",            "weight": 0.20},
    ],
    "retain":          "churn",
    # Cancellation attempts — retention SOBJ for active at-risk customers
    # Separated from reactivation (cancelled customers) to avoid keyword collision
    "cancellation":    "churn",
    "cancel attempt":  "churn",
    # Renewal — active customers approaching renewal decision
    "renew": [
        {"field": "subscription_status_dominant","match": "active",            "weight": 0.40},
        {"field": "ltv_median",                  "direction": "higher_better", "weight": 0.30},
        {"field": "churn_risk_score_mean",       "direction": "higher_better", "weight": 0.30},
    ],
    # Reactivation — customers who have already cancelled
    "reactivat": [
        {"field": "subscription_status_dominant","match": "cancelled",         "weight": 0.50},
        {"field": "days_since_active_median",    "direction": "higher_better", "weight": 0.30},
        {"field": "ltv_median",                  "direction": "higher_better", "weight": 0.20},
    ],
    "lapsed":    "reactivat",
    # Upgrade / upsell — active customers with growth potential
    "upgrade": [
        {"field": "ltv_median",                  "direction": "higher_better", "weight": 0.35},
        {"field": "nps_score_median",            "direction": "higher_better", "weight": 0.25},
        {"field": "feature_adoption_count_median","direction": "higher_better","weight": 0.25},
        {"field": "sessions_last_30d_median",    "direction": "higher_better", "weight": 0.15},
    ],
    "upsell":    "upgrade",
    "premium":   "upgrade",
    # Onboarding / adoption — new or under-activated customers
    "onboard": [
        {"field": "onboarding_completion_pct_mean","direction": "lower_better","weight": 0.50},
        {"field": "feature_adoption_count_median", "direction": "lower_better","weight": 0.30},
        {"field": "sessions_last_30d_median",     "direction": "higher_better","weight": 0.20},
    ],
    "adopt":     "onboard",
    "activat":   "onboard",
    # Engagement — dormant or low-engagement customers
    "engag": [
        {"field": "sessions_last_30d_median",    "direction": "lower_better",  "weight": 0.35},
        {"field": "email_open_rate_mean",        "direction": "lower_better",  "weight": 0.30},
        {"field": "days_since_active_median",    "direction": "higher_better", "weight": 0.35},
    ],
    # Referral / advocacy — satisfied high-value customers
    "refer": [
        {"field": "nps_score_median",            "direction": "higher_better", "weight": 0.50},
        {"field": "community_member_dominant",   "match": True,                "weight": 0.30},
        {"field": "ltv_median",                  "direction": "higher_better", "weight": 0.20},
    ],
    "advocat":   "refer",
    "promot":    "refer",
}



# ── Pre-filter thresholds ─────────────────────────────────────────────────────

PREFILTER_MIN_SCORE       = 0.30
PREFILTER_MAX_CANDIDATES  = 4
PREFILTER_MIN_CANDIDATES  = 2
CONFIDENCE_PENALTY_CASE_C = 0.20
AMBIGUITY_PENALTY         = 0.05

# ── Field normalization bounds ────────────────────────────────────────────────

FIELD_BOUNDS: dict[str, tuple[float, float]] = {
    "churn_risk_score_mean":          (0.0, 1.0),
    "ltv_median":                     (0.0, 10000.0),
    "sessions_last_30d_median":       (0.0, 50.0),
    "days_since_active_median":       (0.0, 365.0),
    "nps_score_median":               (0.0, 10.0),
    "feature_adoption_count_median":  (0.0, 12.0),
    "onboarding_completion_pct_mean": (0.0, 1.0),
    "email_open_rate_mean":           (0.0, 1.0),
    "support_tickets_90d_median":     (0.0, 20.0),
    "total_purchases_median":         (0.0, 100.0),
}


# ── Data classes ──────────────────────────────────────────────────────────────

class RefinedTAProfile:
    """
    A TA card refined for a specific company/product context.
    Stored separately from the original TA card.
    """

    def __init__(
        self,
        ta_id:           str,
        source_ta_card:  dict,
        refinement_case: str,
        refined_fields:  dict,
        locked_fields:   dict,
        company_context: str,
        compliance_mode: str,
        created_at:      str,
    ):
        self.ta_id           = ta_id
        self.source_ta_card  = source_ta_card
        self.refinement_case = refinement_case
        self.refined_fields  = refined_fields
        self.locked_fields   = locked_fields
        self.company_context = company_context
        self.compliance_mode = compliance_mode
        self.created_at      = created_at

        # Merge: locked fields take precedence over refined fields
        self.profile = {**source_ta_card, **refined_fields, **locked_fields}
        self.profile["refinement_case"]       = refinement_case
        self.profile["refinement_context"]    = company_context
        self.profile["company_specific_name"] = refined_fields.get("company_specific_name", "")

    def to_dict(self) -> dict:
        return {
            "ta_id":           self.ta_id,
            "refinement_case": self.refinement_case,
            "locked_fields":   list(self.locked_fields.keys()),
            "refined_fields":  list(self.refined_fields.keys()),
            "company_context": self.company_context,
            "compliance_mode": self.compliance_mode,
            "created_at":      self.created_at,
            "profile":         self.profile,
        }


class TARCandidate:
    """
    A (refined TA profile x SOBJ) pair that passed the pre-filter.
    Ready for full TAR generation.
    """

    def __init__(
        self,
        ta_id:             str,
        sobj_id:           str,
        sobj_statement:    str,
        sobj_direction:    str,
        refined_profile:   RefinedTAProfile,
        prefilter_score:   float,
        prefilter_method:  str,
        prefilter_signals: dict,
        confidence_case:   str,
    ):
        self.ta_id             = ta_id
        self.sobj_id           = sobj_id
        self.sobj_statement    = sobj_statement
        self.sobj_direction    = sobj_direction
        self.refined_profile   = refined_profile
        self.prefilter_score   = prefilter_score
        self.prefilter_method  = prefilter_method
        self.prefilter_signals = prefilter_signals
        self.confidence_case   = confidence_case
        self.tar_id            = f"TAR-{sobj_id}-{ta_id}"

    def to_dict(self) -> dict:
        return {
            "tar_id":            self.tar_id,
            "ta_id":             self.ta_id,
            "sobj_id":           self.sobj_id,
            "sobj_statement":    self.sobj_statement,
            "sobj_direction":    self.sobj_direction,
            "prefilter_score":   self.prefilter_score,
            "prefilter_method":  self.prefilter_method,
            "prefilter_signals": self.prefilter_signals,
            "confidence_case":   self.confidence_case,
            "refined_profile":   self.refined_profile.to_dict(),
        }


# ── Main class ────────────────────────────────────────────────────────────────

class MKTARGenerator:
    """
    TAR pre-filter and profile refinement orchestrator.

    Args:
        session         : active MKSession
        compliance_mode : standard | banking_us | banking_eu | eu_gdpr
        sector          : None | banking | ecommerce
    """

    def __init__(
        self,
        session:         "MKSession",
        compliance_mode: str = "standard",
        sector:          Optional[str] = None,
    ):
        self.session         = session
        self.compliance_mode = compliance_mode
        self.sector          = sector

        self._excluded_signals = (
            COMPLIANCE_EXCLUDED_SIGNALS.get(compliance_mode, set()) |
            NEVER_TARGET_SIGNALS
        )

        print(f"[tar_generator] Initialized")
        print(f"[tar_generator] Compliance mode  : {compliance_mode}")
        print(f"[tar_generator] Excluded signals : {sorted(self._excluded_signals)}")


    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        ta_cards:        list[dict],
        sobjs:           list,
        company_context: str,
        output_dir:      Optional[Path] = None,
    ) -> list[TARCandidate]:
        """
        Run the full pre-filter pipeline.

        Args:
            ta_cards        : TA cards from MKDataIngestor (list of dicts)
            sobjs           : approved SOBJs from session
            company_context : company + sector + OBJ context for LLM prompts
            output_dir      : if provided, save outputs to disk

        Returns:
            List of TARCandidate objects ready for TAR generation.
        """
        print(f"\n[tar_generator] ══════════════════════════════════════")
        print(f"[tar_generator] Starting pre-filter pipeline")
        print(f"[tar_generator]   TA cards : {len(ta_cards)}")
        print(f"[tar_generator]   SOBJs    : {len(sobjs)}")
        print(f"[tar_generator] ══════════════════════════════════════\n")

        refined_profiles = self.refine_profiles(ta_cards, company_context)
        candidates       = self.prefilter(refined_profiles, sobjs)

        if output_dir:
            self._save_outputs(refined_profiles, candidates, output_dir)

        print(f"\n[tar_generator] ══════════════════════════════════════")
        print(f"[tar_generator] Pre-filter complete")
        print(f"[tar_generator]   Refined profiles : {len(refined_profiles)}")
        print(f"[tar_generator]   TAR candidates   : {len(candidates)}")
        print(f"[tar_generator] ══════════════════════════════════════\n")

        return candidates


    def refine_profiles(
        self,
        ta_cards:        list[dict],
        company_context: str,
    ) -> list[RefinedTAProfile]:
        """
        Stage 1 — Refine TA card profiles by confidence case.

        Case A — full alignment    : LLM refines psych/media/messaging to context
        Case B1 — income diverges  : LLM adjusts income-related descriptors only
        Case B2 — race diverges    : LLM adjusts cultural/media/psych layer only
        Case C — full conflict     : skip refinement, pass through as-is

        Returns list of RefinedTAProfile objects.
        """
        print(f"[tar_generator] Stage 1: Profile refinement...")

        refined     = []
        case_counts = {"A": 0, "B1": 0, "B2": 0, "C": 0, "BEH": 0}

        for card in ta_cards:
            ta_id = card.get("ta_id", "unknown")
            case  = self._determine_refinement_case(card)
            case_counts[case] = case_counts.get(case, 0) + 1

            print(f"[tar_generator]   {ta_id} → Case {case}")

            if case == "BEH":
                # Behavioral-only mode — no BTA baseline.
                # Generate company-specific name AND a full behavioral profile
                # using the extended LLM call that works from signals only.
                beh_name, beh_profile = self._generate_profile_for_beh(card, company_context)
                rp = RefinedTAProfile(
                    ta_id           = ta_id,
                    source_ta_card  = card,
                    refinement_case = "BEH",
                    refined_fields  = {
                        "company_specific_name":  beh_name,
                        "psych_summary":          beh_profile.get("psych_summary"),
                        "media_summary":          beh_profile.get("media_summary"),
                        "channel_implications":   beh_profile.get("channel_implications"),
                        "motivational_drivers":   beh_profile.get("motivational_drivers"),
                        "key_barriers":           beh_profile.get("key_barriers"),
                        "trust_cues":             beh_profile.get("trust_cues"),
                        "susceptibility_notes":   beh_profile.get("susceptibility_notes"),
                        "messaging_implications": beh_profile.get("messaging_implications"),
                    },
                    locked_fields   = {},
                    company_context = company_context,
                    compliance_mode = self.compliance_mode,
                    created_at      = datetime.now(timezone.utc).isoformat(),
                )
            elif case == "C":
                # Already LLM-generated — skip profile refinement but generate company-specific name
                case_c_name = self._generate_name_for_case_c(card, company_context)
                rp = RefinedTAProfile(
                    ta_id           = ta_id,
                    source_ta_card  = card,
                    refinement_case = "C",
                    refined_fields  = {"company_specific_name": case_c_name},
                    locked_fields   = {},
                    company_context = company_context,
                    compliance_mode = self.compliance_mode,
                    created_at      = datetime.now(timezone.utc).isoformat(),
                )
            else:
                rp = self._refine_with_llm(card, case, company_context)

            refined.append(rp)

        print(f"[tar_generator] Stage 1 complete — case distribution: {case_counts}")
        return refined


    def prefilter(
        self,
        refined_profiles: list[RefinedTAProfile],
        sobjs:            list,
    ) -> list[TARCandidate]:
        """
        Stage 2 — Pre-filter refined profiles by SOBJ likelihood.

        For each SOBJ:
            1. Match SOBJ text to signal rule map
            2. Score each refined TA profile using matched rules
            3. Fall back to LLM scoring if no rules matched
            4. Apply confidence penalties (Case C, ambiguous BTA)
            5. Return top PREFILTER_MAX_CANDIDATES above threshold

        Returns list of TARCandidate objects sorted by score descending.
        """
        print(f"[tar_generator] Stage 2: Pre-filtering...")

        all_candidates = []

        for sobj in sobjs:
            sobj_id        = getattr(sobj, "id",        str(sobj))
            sobj_statement = getattr(sobj, "statement", str(sobj))
            sobj_direction = getattr(sobj, "direction", "increase")

            print(f"\n[tar_generator]   SOBJ {sobj_id}: {sobj_statement[:70]}...")

            signal_rules, matched_keyword = self._match_sobj_to_rules(sobj_statement)

            scored = []
            for rp in refined_profiles:
                if signal_rules:
                    score, signals = self._score_rules(rp.profile, signal_rules)
                    method = "rules"
                else:
                    score, signals = self._score_llm_fallback(rp, sobj_statement)
                    method = "llm_fallback"

                # Confidence penalties
                if rp.refinement_case == "C":
                    score = max(0.0, score - CONFIDENCE_PENALTY_CASE_C)
                    signals["confidence_penalty"] = f"-{CONFIDENCE_PENALTY_CASE_C} (Case C)"

                if rp.refinement_case == "BEH":
                    score = max(0.0, score - CONFIDENCE_PENALTY_CASE_C)
                    signals["confidence_penalty"] = f"-{CONFIDENCE_PENALTY_CASE_C} (Behavioral profile — no census baseline)"

                if rp.profile.get("is_ambiguous_bta"):
                    score = max(0.0, score - AMBIGUITY_PENALTY)
                    signals["ambiguity_penalty"] = f"-{AMBIGUITY_PENALTY} (ambiguous BTA)"

                scored.append({
                    "profile": rp,
                    "score":   round(score, 4),
                    "method":  method,
                    "signals": signals,
                })

            scored.sort(key=lambda x: x["score"], reverse=True)

            # Apply threshold — always return at least PREFILTER_MIN_CANDIDATES
            passing = [s for s in scored if s["score"] >= PREFILTER_MIN_SCORE]
            if len(passing) < PREFILTER_MIN_CANDIDATES:
                passing = scored[:PREFILTER_MIN_CANDIDATES]
            passing = passing[:PREFILTER_MAX_CANDIDATES]

            print(f"[tar_generator]   Keyword matched  : {matched_keyword or 'none → LLM fallback'}")
            print(f"[tar_generator]   Candidates       : {len(passing)} / {len(scored)} passed threshold")

            for s in passing:
                candidate = TARCandidate(
                    ta_id            = s["profile"].ta_id,
                    sobj_id          = sobj_id,
                    sobj_statement   = sobj_statement,
                    sobj_direction   = sobj_direction,
                    refined_profile  = s["profile"],
                    prefilter_score  = s["score"],
                    prefilter_method = s["method"],
                    prefilter_signals= s["signals"],
                    confidence_case  = s["profile"].refinement_case,
                )
                all_candidates.append(candidate)
                print(f"[tar_generator]   ✓ {candidate.tar_id} | "
                      f"score={s['score']:.3f} | "
                      f"case={s['profile'].refinement_case} | "
                      f"method={s['method']}")

        print(f"\n[tar_generator] Stage 2 complete: {len(all_candidates)} TAR candidates")
        return all_candidates


    # ── Internal helpers ──────────────────────────────────────────────────────

    def _determine_refinement_case(self, card: dict) -> str:
        """
        Determine the A/B1/B2/C/BEH refinement case for a TA card.

        BEH: behavioral-only mode — no BTA matching, confidence_case = "BEH"
        C  : source_type = llm_inferred_custom_archetype
        A  : bta_match_confidence = high
        B2 : bta_race_validation = divergent (race diverges, income matches)
        B1 : bta_race_validation = conflict AND not full C
             (income diverges; age+race gave structural match)
        Default → A (no ZIP enrichment applied or not_available)
        """
        # Behavioral-only mode — detected from confidence_case field on card
        if card.get("confidence_case") == "BEH" or card.get("bta_match_confidence") == "behavioral":
            return "BEH"

        if card.get("source_type") == "llm_inferred_custom_archetype":
            return "C"

        confidence = card.get("bta_match_confidence", "medium")
        race_valid = card.get("bta_race_validation",  "not_available")

        if confidence == "high":
            return "A"
        if race_valid == "divergent":
            return "B2"
        if race_valid == "conflict":
            return "B1"

        # No ZIP enrichment or ambiguous — treat as Case A
        return "A"


    def _build_locked_fields(self, card: dict, case: str) -> dict:
        """
        Build the set of structural fields locked from real data.
        These are passed to the LLM as immutable and must not be changed.
        Compliance-excluded fields are never included.
        """
        candidate_lock_fields = [
            "dominant_age_bin",
            "dominant_income_tier",
            "dominant_household_income_tier",
            "dominant_tenure",
            "dominant_edu_tier",
            "dominant_mar_tier",
            "dominant_emp_tier",
        ]

        locked = {}
        for field in candidate_lock_fields:
            if field in self._excluded_signals:
                continue
            val = card.get(field)
            if val is not None:
                locked[field] = val

        return locked


    def _build_refinement_prompt(
        self,
        card:            dict,
        case:            str,
        company_context: str,
        locked_fields:   dict,
    ) -> str:
        """
        Build a compliance-aware LLM prompt for profile refinement.
        Excluded signals are never included in the prompt.
        """
        archetype_name = card.get("archetype_name", "Unknown")
        structural     = card.get("structural_profile", "")
        psych_summary  = card.get("psych_summary", "")
        media_summary  = card.get("media_summary", "")

        behavioral = {
            k: v for k, v in card.get("behavioral_signals", {}).items()
            if not any(excl in k for excl in self._excluded_signals)
        }

        zip_income = (
            card.get("zip_inferred_income_tier")
            if "zip_inferred_income_tier" not in self._excluded_signals
            else None
        )
        zip_race = (
            card.get("zip_inferred_race_eth")
            if "zip_inferred_race_eth" not in self._excluded_signals
            else None
        )

        base_constraints = f"""IMPORTANT CONSTRAINTS:
- Stay at the population segment level. No price points, no specific product
  features, no tactical predictions. Those belong in the TAR, not the profile.
- Do not invent demographic facts. Only refine the descriptive/contextual layer.
- Keep descriptions generic enough to apply to the whole segment.
- The following structural fields are LOCKED and must appear unchanged:
{json.dumps(locked_fields, indent=2)}

Company/product context:
{company_context}"""

        if case == "A":
            return f"""Refine the audience profile below for the company context provided.
Make the psychographic, media, and messaging descriptions more relevant to this
specific business context — without changing any structural or demographic facts.

{base_constraints}

Current profile:
- Archetype       : {archetype_name}
- Structural      : {structural}
- Psychographics  : {psych_summary}
- Media           : {media_summary}
- Behavioral data : {json.dumps(behavioral, indent=2)}

Return a JSON object with these refined fields:
{{
    "company_specific_name": "3-6 word name describing this audience segment's behavioral role in the context of this specific company and campaign objective. Must be professional, specific, and action-oriented. Reflect what this audience DOES relative to the business goal — not just who they are demographically. IMPORTANT: if multiple segments share the same demographic archetype OR similar behavioral signals, you MUST differentiate the name. First try behavioral signals (churn risk, LTV, MRR, engagement). If behavioral signals are also similar, use the locked structural fields (dominant_age_bin, dominant_sex_label, dominant_edu_tier) to differentiate. Examples: 'High-Churn Premium Subscribers', 'Mid-50s Male Low-Risk Renewers', 'Established Female Homeowner Subscribers'.",
    "psych_summary": "...",
    "media_summary": "...",
    "channel_implications": "...",
    "messaging_implications": "...",
    "motivational_drivers": ["...", "..."],
    "key_barriers": ["...", "..."],
    "trust_cues": ["...", "..."],
    "susceptibility_notes": "..."
}}
Return ONLY the JSON object."""

        elif case == "B1":
            return f"""The audience profile below was matched to a BTA archetype based on
age and demographic signals, but the ZIP-inferred household income ({zip_income}) diverges from the BTA baseline household income tier.
Adjust ONLY the income-related descriptors — spending capacity, price sensitivity,
financial motivations, economic constraints. Do not touch anything else.

{base_constraints}

Current profile:
- Archetype       : {archetype_name}
- Structural      : {structural}
- Psychographics  : {psych_summary}
- Behavioral data : {json.dumps(behavioral, indent=2)}

Return a JSON object adjusting ONLY income-related content:
{{
    "company_specific_name": "3-6 word name describing this audience segment's behavioral role in the context of this specific company and campaign objective. Must be professional, specific, and action-oriented. Reflect what this audience DOES relative to the business goal. IMPORTANT: if multiple segments share the same demographic archetype OR similar behavioral signals, differentiate first using behavioral signals (churn risk, LTV, MRR), then using locked structural fields (dominant_age_bin, dominant_sex_label, dominant_edu_tier) if needed. Examples: 'High-Churn Low-Spend Segment', 'Mid-50s Male Low-Risk Renewers'.",
    "psych_summary": "...",
    "motivational_drivers": ["...", "..."],
    "key_barriers": ["...", "..."],
    "susceptibility_notes": "..."
}}
Return ONLY the JSON object."""

        elif case == "B2":
            cultural_context = ""
            if zip_race:
                cultural_context = (
                    f"ZIP data suggests a higher proportion of {zip_race} residents "
                    f"than the BTA baseline. Adjust cultural references, media preferences, "
                    f"and community signals accordingly — without making race a targeting "
                    f"criterion and without being overly specific."
                )

            return f"""The audience profile below has a structural BTA match but the ZIP-inferred
demographic composition diverges from the BTA baseline race/ethnicity profile.
Adjust ONLY the cultural, media, and psychographic layer. Do not change structural
or economic descriptors.

{base_constraints}
{cultural_context}

Current profile:
- Archetype       : {archetype_name}
- Psychographics  : {psych_summary}
- Media           : {media_summary}

Return a JSON object adjusting ONLY cultural/media/psych content:
{{
    "company_specific_name": "3-6 word name describing this audience segment's behavioral role in the context of this specific company and campaign objective. Must be professional, specific, and action-oriented. Reflect what this audience DOES relative to the business goal. IMPORTANT: if multiple segments share the same demographic archetype OR similar behavioral signals, differentiate first using behavioral signals (churn risk, LTV, MRR), then using locked structural fields (dominant_age_bin, dominant_sex_label, dominant_edu_tier) if needed. Examples: 'Culturally-Distinct High-Churn Risk', 'Established Female Community Renewers'.",
    "psych_summary": "...",
    "media_summary": "...",
    "channel_implications": "...",
    "trust_cues": ["...", "..."],
    "susceptibility_notes": "..."
}}
Return ONLY the JSON object."""

        return ""


    def _refine_with_llm(
        self,
        card:            dict,
        case:            str,
        company_context: str,
    ) -> RefinedTAProfile:
        """Call Claude to refine the TA profile for the given case."""
        try:
            from utils import get_client, log_api_usage
        except ImportError:
            from mk_intel.ingestion.utils import get_client, log_api_usage

        locked_fields = self._build_locked_fields(card, case)
        prompt        = self._build_refinement_prompt(card, case, company_context, locked_fields)

        client   = get_client(self.session)
        response = client.messages.create(
            model      = "claude-haiku-4-5-20251001",
            max_tokens = 1500,
            temperature = 0,
            messages   = [{"role": "user", "content": prompt}],
        )
        log_api_usage(response, f"profile_refinement_case_{case}", self.session)

        raw = response.content[0].text.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()

        try:
            refined_fields = json.loads(raw)
        except json.JSONDecodeError:
            print(f"[tar_generator] ⚠ Refinement parse failed for "
                  f"{card.get('ta_id')} — using original profile")
            refined_fields = {}

        return RefinedTAProfile(
            ta_id           = card.get("ta_id", "unknown"),
            source_ta_card  = card,
            refinement_case = case,
            refined_fields  = refined_fields,
            locked_fields   = locked_fields,
            company_context = company_context,
            compliance_mode = self.compliance_mode,
            created_at      = datetime.now(timezone.utc).isoformat(),
        )


    def _generate_name_for_case_c(self, card: dict, company_context: str) -> str:
        """
        Generate a company-specific audience name for Case C profiles.
        Case C skips full refinement so we make a dedicated small LLM call
        just for the name.
        """
        try:
            from utils import get_client, log_api_usage
        except ImportError:
            from mk_intel.ingestion.utils import get_client, log_api_usage

        archetype = card.get("archetype_name", "Unknown")
        behavioral = {
            k: v for k, v in card.get("behavioral_signals", {}).items()
            if v is not None and not any(excl in k for excl in self._excluded_signals)
        }

        prompt = f"""Given this audience profile and company context, generate a short 3-6 word name
that describes this audience segment's behavioral role for this specific company and campaign.
The name must be professional, specific, and action-oriented.
Reflect what this audience DOES relative to the business goal — not just who they are demographically.
Examples: "Mobile-First Value-Conscious Renewers", "High-LTV Passive Churn Risk", "Budget-Sensitive Upgrade Candidates"

Archetype base: {archetype}
Behavioral signals: {json.dumps({k: v for k, v in list(behavioral.items())[:10]}, indent=2)}
Company context: {company_context[:400]}

Return ONLY a JSON object:
{{
    "company_specific_name": "3-6 word audience name here"
}}"""

        try:
            client   = get_client(self.session)
            response = client.messages.create(
                model      = "claude-haiku-4-5-20251001",
                max_tokens = 100,
                messages   = [{"role": "user", "content": prompt}],
            )
            log_api_usage(response, "case_c_name_generation", self.session)
            raw = response.content[0].text.strip().replace("```json", "").replace("```", "").strip()
            result = json.loads(raw)
            name = result.get("company_specific_name", "").strip()
            if name:
                print(f"[tar_generator]   Case C name generated: {name}")
                return name
        except Exception as e:
            print(f"[tar_generator] ⚠ Case C name generation failed: {e}")

        # Fallback to archetype name
        return archetype


    def _generate_profile_for_beh(
        self,
        card:            dict,
        company_context: str,
    ) -> tuple[str, dict]:
        """
        Generate a full behavioral profile for a BEH (behavioral-only) TA card.

        Unlike Case C (which just generates a name), BEH profiles need a full
        psychographic/media/channel layer because there is no BTA baseline to
        draw from. One LLM call generates both the name and all profile fields.

        Returns:
            (company_specific_name, profile_dict)
        """
        try:
            from utils import get_client, log_api_usage
        except ImportError:
            from mk_intel.utils import get_client, log_api_usage

        behavioral = {
            k: v for k, v in card.get("behavioral_signals", {}).items()
            if v is not None and not any(excl in k for excl in self._excluded_signals)
        }
        structural = card.get("structural_profile", "No demographic baseline available.")

        prompt = f"""You are analyzing a customer audience segment identified purely from behavioral data.
There is no demographic or census baseline available for this segment.
Generate a complete audience profile based only on the behavioral signals provided.

Company context: {company_context[:500]}

Behavioral signals from company data:
{json.dumps({k: v for k, v in list(behavioral.items())[:15]}, indent=2)}

Structural note: {structural}

Generate a professional audience profile. Base ALL claims on the behavioral signals above.
Do NOT invent demographic details. Do NOT reference census data or population archetypes.
Use "llm_inference" framing for any claims not directly supported by the signals.

Return ONLY a JSON object with these fields:
{{
    "company_specific_name": "3-6 word name reflecting behavioral role for this company and campaign. Must be specific and action-oriented. IMPORTANT: if multiple segments may share similar behavioral patterns, differentiate using signal levels (churn risk, LTV, MRR). Examples: 'High-Churn Low-Spend Customers', 'At-Risk High-Value Users', 'Low-Engagement Premium Subscribers'.",
    "psych_summary": "2-3 sentences describing likely psychological profile inferred from behavioral patterns. Frame as inference, not fact.",
    "media_summary": "1-2 sentences on likely media/channel preferences inferred from engagement signals.",
    "channel_implications": "1-2 sentences on best channels to reach this audience based on behavioral data.",
    "motivational_drivers": ["driver 1", "driver 2", "driver 3"],
    "key_barriers": ["barrier 1", "barrier 2"],
    "trust_cues": ["cue 1", "cue 2"],
    "susceptibility_notes": "1-2 sentences on likely persuasion levers given behavioral signals.",
    "messaging_implications": "1-2 sentences on messaging tone and approach."
}}
Return ONLY the JSON object."""

        fallback_name = card.get("archetype_name", f"Behavioral Cluster {card.get('cluster_id', '?')}")
        fallback_profile = {
            "psych_summary": "Behavioral profile — demographic baseline not available. Profile based on company data signals only.",
            "media_summary": None,
            "channel_implications": None,
            "motivational_drivers": None,
            "key_barriers": None,
            "trust_cues": None,
            "susceptibility_notes": None,
            "messaging_implications": None,
        }

        try:
            client   = get_client(self.session)
            response = client.messages.create(
                model      = "claude-haiku-4-5-20251001",
                max_tokens = 800,
                messages   = [{"role": "user", "content": prompt}],
            )
            log_api_usage(response, "beh_profile_generation", self.session)
            raw    = response.content[0].text.strip().replace("```json", "").replace("```", "").strip()
            result = json.loads(raw)
            name   = result.pop("company_specific_name", "").strip() or fallback_name
            print(f"[tar_generator]   BEH profile generated: {name}")
            return name, result
        except Exception as e:
            print(f"[tar_generator] ⚠ BEH profile generation failed: {e}")
            return fallback_name, fallback_profile


    def _match_sobj_to_rules(
        self,
        sobj_statement: str,
    ) -> tuple[Optional[list[dict]], Optional[str]]:
        """
        Match a SOBJ statement to the signal rule map.
        Returns (rules, matched_keyword) or (None, None) if no match.
        Resolves string aliases to their canonical rule lists.
        """
        statement_lower = sobj_statement.lower()

        for keyword, rules in SOBJ_SIGNAL_MAP.items():
            if keyword in statement_lower:
                if isinstance(rules, str):
                    canonical = SOBJ_SIGNAL_MAP.get(rules, [])
                    return canonical, keyword
                return rules, keyword

        return None, None


    def _score_rules(
        self,
        profile: dict,
        rules:   list[dict],
    ) -> tuple[float, dict]:
        """
        Score a TA profile against a set of signal rules.
        Numeric fields normalized to 0-1. Categorical fields matched.
        Compliance-excluded fields skipped.

        Note: hard gate logic was evaluated and deferred to the TAR scoring
        algorithm (NB15), which has richer context to evaluate TA eligibility
        per SOBJ. The pre-filter uses weighted signals only.
        """
        signals_used = {}
        weighted_sum = 0.0
        total_weight = 0.0

        # Flatten behavioral signals to top level for lookup
        behavioral   = profile.get("behavioral_signals", {})
        flat_profile = {**profile, **behavioral}

        for rule in rules:
            field     = rule["field"]
            weight    = rule["weight"]
            direction = rule.get("direction")
            match_val = rule.get("match")

            # Skip compliance-excluded fields
            base_field = (field
                          .replace("_median", "")
                          .replace("_mean", "")
                          .replace("_dominant", ""))
            if base_field in self._excluded_signals:
                continue

            val = flat_profile.get(field)
            if val is None:
                continue

            total_weight += weight

            if match_val is not None:
                score = 1.0 if val == match_val else 0.0
                signals_used[field] = {"value": val, "match": match_val, "score": score}
            elif direction:
                score = self._normalize_signal(field, val, direction)
                signals_used[field] = {"value": val, "direction": direction, "score": round(score, 4)}
            else:
                continue

            weighted_sum += score * weight

        final_score = weighted_sum / total_weight if total_weight > 0 else 0.0
        return round(final_score, 4), signals_used


    def _normalize_signal(self, field: str, value, direction: str) -> float:
        """Normalize a signal value to 0-1 using field-specific bounds."""
        bounds = FIELD_BOUNDS.get(field)
        if bounds is None:
            return 0.5

        lo, hi = bounds
        if hi == lo:
            return 0.5

        try:
            val = float(value)
        except (TypeError, ValueError):
            return 0.5

        normalized = max(0.0, min(1.0, (val - lo) / (hi - lo)))
        return normalized if direction == "higher_better" else 1.0 - normalized


    def _score_llm_fallback(
        self,
        refined_profile: RefinedTAProfile,
        sobj_statement:  str,
    ) -> tuple[float, dict]:
        """
        LLM fallback scoring for SOBJs not matched by the rule engine.
        Returns likelihood score (0-1) and rationale.
        """
        try:
            from utils import get_client, log_api_usage
        except ImportError:
            from mk_intel.ingestion.utils import get_client, log_api_usage

        profile = refined_profile.profile

        profile_summary = {
            "archetype_name":     profile.get("archetype_name"),
            "structural_profile": profile.get("structural_profile"),
            "psych_summary":      profile.get("psych_summary"),
            "media_summary":      profile.get("media_summary"),
            "behavioral_signals": {
                k: v for k, v in profile.get("behavioral_signals", {}).items()
                if not any(excl in k for excl in self._excluded_signals)
            },
        }
        for excl in self._excluded_signals:
            profile_summary.pop(excl, None)

        prompt = f"""You are a marketing intelligence analyst.

Rate the likelihood (0.0 to 1.0) that the following customer segment will
perform this desired behavior:

DESIRED BEHAVIOR: {sobj_statement}

AUDIENCE PROFILE:
{json.dumps(profile_summary, indent=2)}

Return ONLY a JSON object:
{{
    "likelihood_score": <float 0.0-1.0>,
    "rationale": "<one sentence>"
}}"""

        client   = get_client(self.session)
        response = client.messages.create(
            model      = "claude-haiku-4-5-20251001",
            max_tokens = 200,
            messages   = [{"role": "user", "content": prompt}],
        )
        log_api_usage(response, "prefilter_llm_fallback", self.session)

        raw = response.content[0].text.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()

        try:
            result  = json.loads(raw)
            score   = max(0.0, min(1.0, float(result.get("likelihood_score", 0.5))))
            signals = {"llm_rationale": result.get("rationale", "")}
        except (json.JSONDecodeError, ValueError):
            score   = 0.5
            signals = {"llm_rationale": "parse_failed"}

        return score, signals


    def _save_outputs(
        self,
        refined_profiles: list[RefinedTAProfile],
        candidates:       list[TARCandidate],
        output_dir:       Path,
    ) -> None:
        """Save refined profiles and TAR candidates to disk."""
        output_dir.mkdir(parents=True, exist_ok=True)

        with open(output_dir / "refined_ta_profiles.json", "w") as f:
            json.dump([rp.to_dict() for rp in refined_profiles],
                      f, indent=2, default=str)

        with open(output_dir / "tar_candidates.json", "w") as f:
            json.dump([c.to_dict() for c in candidates],
                      f, indent=2, default=str)

        print(f"[tar_generator] Outputs saved to: {output_dir}")
        print(f"[tar_generator]   refined_ta_profiles.json : {len(refined_profiles)}")
        print(f"[tar_generator]   tar_candidates.json       : {len(candidates)}")


# ── Module-level helper ───────────────────────────────────────────────────────

def build_company_context(session: "MKSession") -> str:
    """
    Build a company context string from the session for LLM prompts.
    """
    company = session.company
    if not company:
        return "Unknown company — general B2C context"

    obj_statement = ""
    if hasattr(session, "objective") and session.objective:
        obj_statement = f"\nCampaign objective: {session.objective.statement}"

    return (
        f"Company: {company.name}\n"
        f"Industry: {getattr(company, 'industry', 'Unknown')}\n"
        f"Customer type: {getattr(company, 'customer_type', 'B2C')}\n"
        f"Description: {getattr(company, 'description_input', '')}"
        f"{obj_statement}"
    ).strip()
