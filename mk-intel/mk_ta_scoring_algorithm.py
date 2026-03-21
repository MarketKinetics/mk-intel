"""
MK Target Audience Prioritization Algorithm
============================================
Scores each (TA, SOBJ) pair and returns a ranked list.

Design principles:
- Scoring is always relative to a specific SOBJ, not a TA in isolation
- Every score is decomposed so the reasoning is auditable
- Weights are placeholders — to be calibrated with domain expertise
- The algorithm enforces hard gates before scoring begins
"""

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


# ---------------------------------------------------------------------------
# Data contracts (lightweight versions of the full TAAW schema objects)
# These would be populated from parsed TAAW JSON in the real system
# ---------------------------------------------------------------------------

class AlignmentDirection(Enum):
    ALIGNED   = "aligned"
    NEUTRAL   = "neutral"
    CONFLICTED = "conflicted"
    OPPOSED   = "opposed"

class SOBJDirection(Enum):
    INCREASE  = "increase"
    DECREASE  = "decrease"
    MAINTAIN  = "maintain"
    INITIATE  = "initiate"
    STOP      = "stop"


@dataclass
class EffectivenessInput:
    """Derived from TAAW.effectiveness section"""
    rating: int                        # 0-5, analyst-assigned
    decision_rights_score: int         # 0-3: 0=none, 1=partial, 2=full, 3=full+influence
    resource_access_score: int         # 0-2: 0=blocked, 1=partial, 2=available
    restriction_count: int             # number of high-severity restrictions
    desired_behavior_quality: int      # 0-3: sum of is_specific + is_observable + is_measurable
    sobj_contribution_estimated: bool  # analyst confirmed TA materially contributes to SOBJ


@dataclass
class SusceptibilityInput:
    """Derived from TAAW.susceptibility section"""
    rating: int                         # 1-5, analyst-assigned
    reward_count: int                   # number of perceived rewards identified
    reward_salience: float              # avg salience: low=1, medium=2, high=3
    risk_count: int                     # number of perceived risks identified
    risk_severity: float                # avg severity: low=1, medium=2, high=3
    alignment_direction: AlignmentDirection


@dataclass
class VulnerabilityInput:
    """Derived from TAAW.vulnerabilities section"""
    motive_count: int                   # total motives identified
    critical_motive_count: int          # motives with priority='critical'
    sourced_motive_count: int           # motives with a source or condition link
    psychographic_count: int
    sourced_psychographic_count: int
    demographic_count: int
    symbol_count: int
    sourced_symbol_count: int           # symbols where recognized_by_ta=True and sourced


@dataclass
class AccessibilityInput:
    """Derived from TAAW.accessibility section"""
    channels: list                      # list of dicts: {reach_quality: int, violates_restrictions: bool}


@dataclass
class AudienceSizeInput:
    """Derived from TAAW.header.target_audience.audience_size_estimate"""
    value: Optional[float] = None
    unit: str = "individuals"
    confidence: str = "low"


@dataclass
class TAInput:
    """Full input bundle for one (TA, SOBJ) pair"""
    ta_id: str
    sobj_id: str
    sobj_direction: SOBJDirection
    effectiveness: EffectivenessInput
    susceptibility: SusceptibilityInput
    vulnerabilities: VulnerabilityInput
    accessibility: AccessibilityInput
    audience_size: AudienceSizeInput = field(default_factory=AudienceSizeInput)


# ---------------------------------------------------------------------------
# Scoring config
# Weights are explicit and named so they can be tuned without touching logic
# ---------------------------------------------------------------------------

@dataclass
class ScoringWeights:
    effectiveness:  float = 0.30
    susceptibility: float = 0.30
    vulnerability:  float = 0.25
    accessibility:  float = 0.15
    # audience_size is a modifier, not a dimension weight — applied at the end


DEFAULT_WEIGHTS = ScoringWeights()


# ---------------------------------------------------------------------------
# Hard gates — must pass before scoring begins
# ---------------------------------------------------------------------------

@dataclass
class GateResult:
    passed: bool
    reason: str


def check_gates(ta: TAInput) -> GateResult:
    """
    Hard gates that disqualify a TA before scoring.
    A failed gate means the analysis should stop and a different TA selected.
    """

    # Gate 1: Effectiveness rating threshold
    # Doctrine: rating <= 2 means TA cannot meaningfully accomplish SOBJ
    if ta.effectiveness.rating <= 2:
        return GateResult(
            passed=False,
            reason=f"Effectiveness rating {ta.effectiveness.rating}/5 is below threshold (>2 required). "
                   f"TA lacks sufficient authority/power/control to accomplish SOBJ."
        )

    # Gate 2: Desired behavior must be minimally well-defined
    if ta.effectiveness.desired_behavior_quality < 2:
        return GateResult(
            passed=False,
            reason="Desired behavior is insufficiently defined. "
                   "Must be at least specific + observable OR specific + measurable before scoring."
        )

    # Gate 3: TA must have at least one vulnerability identified
    total_vulnerabilities = (
        ta.vulnerabilities.motive_count +
        ta.vulnerabilities.psychographic_count +
        ta.vulnerabilities.symbol_count
    )
    if total_vulnerabilities == 0:
        return GateResult(
            passed=False,
            reason="No vulnerabilities identified. Cannot construct persuasion logic without at least one lever."
        )

    # Gate 4: At least one non-restricted channel must exist
    usable_channels = [c for c in ta.accessibility.channels if not c.get("violates_restrictions", False)]
    if len(usable_channels) == 0:
        return GateResult(
            passed=False,
            reason="No accessible channels available that comply with TA restrictions. TA cannot be reached."
        )

    return GateResult(passed=True, reason="All gates passed.")


# ---------------------------------------------------------------------------
# Dimension scorers — each returns a float 0.0–1.0
# ---------------------------------------------------------------------------

def score_effectiveness(e: EffectivenessInput) -> tuple[float, dict]:
    """
    Measures: can the TA actually perform the desired behavior?

    Components:
    - Analyst rating (anchored 0-5 scale) — primary signal
    - Decision rights quality — can they autonomously act?
    - Resource access — do they have what's needed?
    - Restriction penalty — high-severity constraints reduce score
    - Behavior definition quality — ill-defined desired behavior = unreliable score
    """

    # Normalize analyst rating to 0-1 (0-5 scale)
    rating_score = e.rating / 5.0

    # Decision rights: 0=0.0, 1=0.5, 2=0.85, 3=1.0
    decision_map = {0: 0.0, 1: 0.5, 2: 0.85, 3: 1.0}
    decision_score = decision_map.get(e.decision_rights_score, 0.0)

    # Resource access: 0=0.0, 1=0.5, 2=1.0
    resource_map = {0: 0.0, 1: 0.5, 2: 1.0}
    resource_score = resource_map.get(e.resource_access_score, 0.0)

    # Restriction penalty: each high-severity restriction reduces by 0.15 (capped at 0.45)
    restriction_penalty = min(e.restriction_count * 0.15, 0.45)

    # Behavior quality boost: well-defined behavior improves confidence in the score
    behavior_boost = (e.desired_behavior_quality / 3.0) * 0.1

    # SOBJ contribution confirmation: small bonus if analyst explicitly confirmed
    contribution_boost = 0.05 if e.sobj_contribution_estimated else 0.0

    raw = (
        rating_score      * 0.50 +   # analyst rating is primary
        decision_score    * 0.25 +
        resource_score    * 0.15 +
        behavior_boost    * 0.10
    ) - restriction_penalty + contribution_boost

    score = max(0.0, min(1.0, raw))

    breakdown = {
        "rating_component":        round(rating_score * 0.50, 3),
        "decision_rights":         round(decision_score * 0.25, 3),
        "resource_access":         round(resource_score * 0.15, 3),
        "behavior_quality_boost":  round(behavior_boost, 3),
        "restriction_penalty":     round(-restriction_penalty, 3),
        "contribution_boost":      round(contribution_boost, 3),
        "raw_before_clamp":        round(raw, 3),
        "final":                   round(score, 3)
    }
    return score, breakdown


def score_susceptibility(s: SusceptibilityInput, sobj_direction: SOBJDirection) -> tuple[float, dict]:
    """
    Measures: will the TA move toward the desired behavior?

    Components:
    - Analyst rating — primary signal
    - Net reward-risk balance — more/stronger rewards vs risks
    - Value/belief alignment — identity alignment is a strong multiplier
    - SOBJ direction modifier — stopping behaviors are harder than starting them
    """

    rating_score = (s.rating - 1) / 4.0   # scale 1-5 → 0-1

    # Net reward-risk score
    # Reward contribution: count * avg_salience, normalized to 0-1
    reward_raw = min(s.reward_count * s.reward_salience, 12.0) / 12.0
    # Risk penalty: count * avg_severity, reduces susceptibility
    risk_penalty_raw = min(s.risk_count * s.risk_severity, 12.0) / 12.0
    net_reward_score = max(0.0, reward_raw - (risk_penalty_raw * 0.6))

    # Value/belief alignment score
    alignment_map = {
        AlignmentDirection.ALIGNED:    1.0,
        AlignmentDirection.NEUTRAL:    0.6,
        AlignmentDirection.CONFLICTED: 0.3,
        AlignmentDirection.OPPOSED:    0.0
    }
    alignment_score = alignment_map[s.alignment_direction]

    # SOBJ direction modifier
    # Initiating a new behavior or stopping an entrenched one is harder
    direction_modifier_map = {
        SOBJDirection.INCREASE:  1.0,
        SOBJDirection.MAINTAIN:  1.1,   # slight boost — inertia helps
        SOBJDirection.DECREASE:  0.9,
        SOBJDirection.INITIATE:  0.85,  # new behavior = higher friction
        SOBJDirection.STOP:      0.80   # stopping entrenched behavior is hardest
    }
    direction_modifier = direction_modifier_map[sobj_direction]

    raw = (
        rating_score      * 0.45 +
        net_reward_score  * 0.25 +
        alignment_score   * 0.30
    ) * direction_modifier

    score = max(0.0, min(1.0, raw))

    breakdown = {
        "rating_component":    round(rating_score * 0.45, 3),
        "net_reward_balance":  round(net_reward_score * 0.25, 3),
        "alignment_score":     round(alignment_score * 0.30, 3),
        "direction_modifier":  direction_modifier,
        "raw_before_clamp":    round(raw, 3),
        "final":               round(score, 3)
    }
    return score, breakdown


def score_vulnerability_depth(v: VulnerabilityInput) -> tuple[float, dict]:
    """
    Measures: how many actionable levers exist to influence this TA?

    Key distinction from susceptibility: this is about QUANTITY and QUALITY
    of available influence levers, not about whether the TA is open to change.

    Sourced/cross-referenced vulnerabilities score higher than unsourced ones.
    Critical motives are weighted more than secondary ones.
    """

    # Motive score: critical motives worth more, sourced ones worth more
    # Max reasonable: 4 motives, mix of critical and sourced
    motive_quality = (
        (v.critical_motive_count * 1.5) +
        (v.sourced_motive_count * 1.0) +
        ((v.motive_count - v.sourced_motive_count) * 0.5)   # unsourced motives contribute less
    )
    motive_score = min(motive_quality / 8.0, 1.0)   # normalize: 8 = ceiling of strong motive set

    # Psychographic score: sourced = 1.0 per item, unsourced = 0.4
    psychographic_quality = (
        v.sourced_psychographic_count * 1.0 +
        (v.psychographic_count - v.sourced_psychographic_count) * 0.4
    )
    psychographic_score = min(psychographic_quality / 5.0, 1.0)

    # Symbol score: only counts symbols recognized by TA and sourced
    symbol_score = min(v.sourced_symbol_count / 3.0, 1.0)

    # Demographic score: presence bonus (demographics alone are weak levers)
    demographic_score = min(v.demographic_count / 4.0, 1.0) * 0.5

    raw = (
        motive_score       * 0.45 +
        psychographic_score* 0.30 +
        symbol_score       * 0.15 +
        demographic_score  * 0.10
    )

    score = max(0.0, min(1.0, raw))

    breakdown = {
        "motive_score":         round(motive_score * 0.45, 3),
        "psychographic_score":  round(psychographic_score * 0.30, 3),
        "symbol_score":         round(symbol_score * 0.15, 3),
        "demographic_score":    round(demographic_score * 0.10, 3),
        "final":                round(score, 3)
    }
    return score, breakdown


def score_accessibility(a: AccessibilityInput) -> tuple[float, dict]:
    """
    Measures: can we reliably reach this TA?

    Uses the best available channel (not an average) — a TA reachable
    through one excellent channel is prioritizable even if other channels
    are poor. Channels that violate restrictions are excluded.
    """

    usable = [c for c in a.channels if not c.get("violates_restrictions", False)]

    if not usable:
        return 0.0, {"note": "No usable channels (all violate restrictions)", "final": 0.0}

    # Best channel score (0-5 → 0-1)
    best_reach = max(c["reach_quality"] for c in usable) / 5.0

    # Channel breadth bonus: having 2+ strong channels adds resilience
    strong_channels = [c for c in usable if c["reach_quality"] >= 4]
    breadth_bonus = min(len(strong_channels) * 0.05, 0.15)

    raw = best_reach + breadth_bonus
    score = max(0.0, min(1.0, raw))

    breakdown = {
        "best_channel_reach":  round(best_reach, 3),
        "breadth_bonus":       round(breadth_bonus, 3),
        "usable_channel_count": len(usable),
        "final":               round(score, 3)
    }
    return score, breakdown


def audience_size_modifier(size: AudienceSizeInput) -> float:
    """
    Returns a multiplier (0.85–1.15) applied to the composite score.
    Audience size is a modifier, not a primary dimension:
    - A small but perfectly positioned TA should not be buried by a huge generic one
    - But all else equal, larger reachable audiences increase strategic value

    This function should be revisited once real audience size data is available.
    Current implementation is intentionally conservative.
    """

    if size.value is None:
        return 1.0   # no data = neutral modifier

    # Confidence penalty: low confidence in size estimate reduces modifier strength
    confidence_map = {"low": 0.3, "medium": 0.7, "high": 1.0}
    confidence_weight = confidence_map.get(size.confidence, 0.3)

    # Very small audiences (<1000): slight penalty
    # Mid-range (1K–1M): neutral to slight boost
    # Very large (>1M): small boost, but capped — size alone doesn't win
    if size.value < 1_000:
        raw_modifier = 0.90
    elif size.value < 50_000:
        raw_modifier = 0.97
    elif size.value < 500_000:
        raw_modifier = 1.03
    elif size.value < 5_000_000:
        raw_modifier = 1.08
    else:
        raw_modifier = 1.12

    # Blend toward 1.0 based on confidence: low confidence → modifier closer to neutral
    modifier = 1.0 + (raw_modifier - 1.0) * confidence_weight
    return round(modifier, 3)


# ---------------------------------------------------------------------------
# Composite scorer
# ---------------------------------------------------------------------------

@dataclass
class TAScore:
    ta_id: str
    sobj_id: str
    gate_result: GateResult
    composite_score: Optional[float]
    dimension_scores: dict
    dimension_breakdowns: dict
    size_modifier: float
    final_score: Optional[float]
    rank: Optional[int] = None
    recommendation: str = ""


def score_ta(ta: TAInput, weights: ScoringWeights = DEFAULT_WEIGHTS) -> TAScore:
    """Score a single (TA, SOBJ) pair."""

    gate = check_gates(ta)

    if not gate.passed:
        return TAScore(
            ta_id=ta.ta_id,
            sobj_id=ta.sobj_id,
            gate_result=gate,
            composite_score=None,
            dimension_scores={},
            dimension_breakdowns={},
            size_modifier=1.0,
            final_score=None,
            recommendation=f"DISQUALIFIED — {gate.reason}"
        )

    eff_score,  eff_bd   = score_effectiveness(ta.effectiveness)
    susc_score, susc_bd  = score_susceptibility(ta.susceptibility, ta.sobj_direction)
    vuln_score, vuln_bd  = score_vulnerability_depth(ta.vulnerabilities)
    acc_score,  acc_bd   = score_accessibility(ta.accessibility)

    composite = (
        eff_score  * weights.effectiveness  +
        susc_score * weights.susceptibility +
        vuln_score * weights.vulnerability  +
        acc_score  * weights.accessibility
    )

    size_mod   = audience_size_modifier(ta.audience_size)
    final      = round(composite * size_mod, 4)

    return TAScore(
        ta_id=ta.ta_id,
        sobj_id=ta.sobj_id,
        gate_result=gate,
        composite_score=round(composite, 4),
        dimension_scores={
            "effectiveness":  round(eff_score,  4),
            "susceptibility": round(susc_score, 4),
            "vulnerability":  round(vuln_score, 4),
            "accessibility":  round(acc_score,  4)
        },
        dimension_breakdowns={
            "effectiveness":  eff_bd,
            "susceptibility": susc_bd,
            "vulnerability":  vuln_bd,
            "accessibility":  acc_bd
        },
        size_modifier=size_mod,
        final_score=final
    )


# ---------------------------------------------------------------------------
# Ranker — scores and ranks a list of TAs against a single SOBJ
# ---------------------------------------------------------------------------

def rank_tas_for_sobj(ta_inputs: list[TAInput], weights: ScoringWeights = DEFAULT_WEIGHTS) -> list[TAScore]:
    """
    Score all TAs for a given SOBJ and return them ranked highest to lowest.
    Disqualified TAs are included at the bottom with final_score=None.
    """

    scored = [score_ta(ta, weights) for ta in ta_inputs]

    qualified     = [s for s in scored if s.final_score is not None]
    disqualified  = [s for s in scored if s.final_score is None]

    qualified.sort(key=lambda s: s.final_score, reverse=True)

    for rank, ta_score in enumerate(qualified, start=1):
        ta_score.rank = rank
        ta_score.recommendation = _generate_recommendation(ta_score, rank, len(qualified))

    return qualified + disqualified


def _generate_recommendation(s: TAScore, rank: int, total_qualified: int) -> str:
    """
    Generate a plain-language priority recommendation based on score profile.
    This is a heuristic narrative — the LLM layer can enrich this later.
    """

    if rank == 1:
        priority_label = "FIRST PRIORITY"
    elif rank <= max(2, total_qualified // 3):
        priority_label = "HIGH PRIORITY"
    elif rank <= max(3, (total_qualified * 2) // 3):
        priority_label = "MEDIUM PRIORITY"
    else:
        priority_label = "LOWER PRIORITY"

    ds = s.dimension_scores

    # Identify the weakest dimension as the key risk
    weakest_dim   = min(ds, key=ds.get)
    weakest_score = ds[weakest_dim]

    # Identify the strongest dimension as the key advantage
    strongest_dim   = max(ds, key=ds.get)
    strongest_score = ds[strongest_dim]

    rec = (
        f"{priority_label} — Final score: {s.final_score:.3f}. "
        f"Primary advantage: {strongest_dim} ({strongest_score:.2f}). "
    )

    if weakest_score < 0.4:
        rec += f"Key risk: {weakest_dim} is weak ({weakest_score:.2f}) — address before execution."
    else:
        rec += f"No critical weaknesses identified."

    return rec


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    ta_examples = [

        TAInput(
            ta_id="TA-01",
            sobj_id="SOBJ-01",
            sobj_direction=SOBJDirection.INCREASE,
            effectiveness=EffectivenessInput(
                rating=4,
                decision_rights_score=2,
                resource_access_score=2,
                restriction_count=1,
                desired_behavior_quality=3,
                sobj_contribution_estimated=True
            ),
            susceptibility=SusceptibilityInput(
                rating=3,
                reward_count=3,
                reward_salience=2.3,
                risk_count=2,
                risk_severity=1.5,
                alignment_direction=AlignmentDirection.CONFLICTED
            ),
            vulnerabilities=VulnerabilityInput(
                motive_count=3,
                critical_motive_count=2,
                sourced_motive_count=3,
                psychographic_count=3,
                sourced_psychographic_count=2,
                demographic_count=2,
                symbol_count=3,
                sourced_symbol_count=2
            ),
            accessibility=AccessibilityInput(channels=[
                {"reach_quality": 5, "violates_restrictions": False},
                {"reach_quality": 3, "violates_restrictions": False},
            ]),
            audience_size=AudienceSizeInput(value=120_000, unit="individuals", confidence="medium")
        ),

        TAInput(
            ta_id="TA-02",
            sobj_id="SOBJ-01",
            sobj_direction=SOBJDirection.INCREASE,
            effectiveness=EffectivenessInput(
                rating=5,
                decision_rights_score=3,
                resource_access_score=2,
                restriction_count=0,
                desired_behavior_quality=3,
                sobj_contribution_estimated=True
            ),
            susceptibility=SusceptibilityInput(
                rating=4,
                reward_count=4,
                reward_salience=2.5,
                risk_count=1,
                risk_severity=1.0,
                alignment_direction=AlignmentDirection.ALIGNED
            ),
            vulnerabilities=VulnerabilityInput(
                motive_count=4,
                critical_motive_count=3,
                sourced_motive_count=4,
                psychographic_count=4,
                sourced_psychographic_count=4,
                demographic_count=3,
                symbol_count=2,
                sourced_symbol_count=2
            ),
            accessibility=AccessibilityInput(channels=[
                {"reach_quality": 4, "violates_restrictions": False},
                {"reach_quality": 4, "violates_restrictions": False},
            ]),
            audience_size=AudienceSizeInput(value=45_000, unit="individuals", confidence="high")
        ),

        TAInput(
            ta_id="TA-03",
            sobj_id="SOBJ-01",
            sobj_direction=SOBJDirection.INCREASE,
            effectiveness=EffectivenessInput(
                rating=2,   # Will fail the gate
                decision_rights_score=1,
                resource_access_score=1,
                restriction_count=3,
                desired_behavior_quality=2,
                sobj_contribution_estimated=False
            ),
            susceptibility=SusceptibilityInput(
                rating=2,
                reward_count=1,
                reward_salience=1.0,
                risk_count=3,
                risk_severity=2.5,
                alignment_direction=AlignmentDirection.OPPOSED
            ),
            vulnerabilities=VulnerabilityInput(
                motive_count=1,
                critical_motive_count=0,
                sourced_motive_count=0,
                psychographic_count=1,
                sourced_psychographic_count=0,
                demographic_count=1,
                symbol_count=0,
                sourced_symbol_count=0
            ),
            accessibility=AccessibilityInput(channels=[
                {"reach_quality": 2, "violates_restrictions": False},
            ]),
            audience_size=AudienceSizeInput(value=500_000, unit="individuals", confidence="low")
        )
    ]

    results = rank_tas_for_sobj(ta_examples)

    print("=" * 70)
    print(f"TA PRIORITY RANKING — SOBJ: {ta_examples[0].sobj_id}")
    print("=" * 70)

    for r in results:
        print(f"\n[{r.rank or 'X'}] {r.ta_id}")
        if r.final_score is not None:
            print(f"    Final score:    {r.final_score:.4f}  (composite: {r.composite_score:.4f}, size modifier: {r.size_modifier})")
            for dim, score in r.dimension_scores.items():
                print(f"    {dim:<16} {score:.4f}")
        print(f"    → {r.recommendation}")

    print("\n" + "=" * 70)

