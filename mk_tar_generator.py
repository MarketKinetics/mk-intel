"""
mk_tar_generator.py
===================
MK Intel — TAR (Target Audience Report) Generator

Generates full structured Target Audience Reports for each
(refined TA profile x SOBJ) candidate that passed the pre-filter.

Architecture
------------
TAR generation runs in sequential LLM calls — one per schema section.
Each call receives previously generated sections as context, enabling
internally consistent cross-referencing (condition IDs to vulnerability
IDs to argument IDs to action IDs).

Section sequence:
    1. header          -- derived from inputs, no LLM call
    2. effectiveness   -- can this TA perform the SOBJ? (gate check)
    3. conditions      -- why do they behave as they do today?
    4. vulnerabilities -- what psychological levers exist?
    5. susceptibility  -- how open are they to persuasion?
    6. accessibility   -- which channels can reach them?
    7. narrative       -- the persuasion logic and recommended actions
    8. assessment      -- measurement framework
    9. traceability    -- sources, assumptions, ethics

If gate_pass = False after effectiveness, generation stops.
The TAR is saved as a disqualified document.

Source tagging
--------------
Every claim in the TAR is tagged with its source:
    company_data   -- behavioral signals from the TA card
    bta_baseline   -- ACS/GSS/Pew projection on the BTA card
    zip_inference  -- ZIP enrichment signals
    llm_inference  -- model reasoning from profile context (flag for analyst review)

Compliance
----------
Compliance mode exclusions from mk_tar_prefilter.py are preserved.
Excluded fields never appear in generation prompts.

Public API
----------
    MKTARGenerator(session, compliance_mode, sector)
    generator.generate(tar_candidates, output_dir) -> list[TARDocument]
    generator.generate_one(candidate) -> TARDocument
    tar_to_ta_input(tar_doc) -> TAInput  (adapter for mk_ta_scoring_algorithm)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from mk_intel_session import MKSession
    from mk_tar_prefilter import TARCandidate


# ── Compliance signal exclusions ──────────────────────────────────────────────

COMPLIANCE_EXCLUDED_SIGNALS: dict[str, set[str]] = {
    "standard":   set(),
    "banking_us": {
        "age_bin", "dominant_age_bin",
        "income_tier", "dominant_income_tier", "dominant_household_income_tier",
        "zip_inferred_income_tier", "zip_inferred_race_eth",
    },
    "banking_eu": {
        "age_bin", "dominant_age_bin",
        "income_tier", "dominant_income_tier", "dominant_household_income_tier",
        "zip_inferred_income_tier", "zip_inferred_race_eth",
        "marital_status", "dominant_mar_tier",
    },
    "eu_gdpr": {
        "zip_inferred_income_tier", "zip_inferred_race_eth",
    },
}

NEVER_TARGET_SIGNALS: set[str] = {
    "gender", "dominant_sex_label",
    "dominant_race_eth", "zip_inferred_race_eth",
}


# ── TARDocument ───────────────────────────────────────────────────────────────

class TARDocument:
    """
    A fully generated Target Audience Report for one (TA x SOBJ) pair.
    """

    def __init__(
        self,
        tar_id:            str,
        ta_id:             str,
        sobj_id:           str,
        sobj_statement:    str,
        sobj_direction:    str,
        sections:          dict,
        gate_passed:       bool,
        gate_fail_reason:  Optional[str],
        confidence_case:   str,
        compliance_mode:   str,
        created_at:        str,
        token_usage:       dict,
    ):
        self.tar_id           = tar_id
        self.ta_id            = ta_id
        self.sobj_id          = sobj_id
        self.sobj_statement   = sobj_statement
        self.sobj_direction   = sobj_direction
        self.sections         = sections
        self.gate_passed      = gate_passed
        self.gate_fail_reason = gate_fail_reason
        self.confidence_case  = confidence_case
        self.compliance_mode  = compliance_mode
        self.created_at       = created_at
        self.token_usage      = token_usage

    def to_dict(self) -> dict:
        return {
            "meta": {
                "schema_version": "2.0",
                "document_id":    self.tar_id,
                "created_at":     self.created_at[:10],
                "status":         "draft",
                "context_notes": (
                    f"confidence_case={self.confidence_case} | "
                    f"compliance={self.compliance_mode} | "
                    f"gate_passed={self.gate_passed}"
                ),
            },
            "tar_id":           self.tar_id,
            "ta_id":            self.ta_id,
            "sobj_id":          self.sobj_id,
            "sobj_statement":   self.sobj_statement,
            "sobj_direction":   self.sobj_direction,
            "gate_passed":      self.gate_passed,
            "gate_fail_reason": self.gate_fail_reason,
            "confidence_case":  self.confidence_case,
            "compliance_mode":  self.compliance_mode,
            "created_at":       self.created_at,
            "token_usage":      self.token_usage,
            **self.sections,
        }


# ── Main class ────────────────────────────────────────────────────────────────

class MKTARGenerator:
    """
    TAR generation orchestrator.

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
        self._excluded       = (
            COMPLIANCE_EXCLUDED_SIGNALS.get(compliance_mode, set()) |
            NEVER_TARGET_SIGNALS
        )
        print(f"[tar_generator] Initialized | compliance={compliance_mode}")


    # ── Public API ────────────────────────────────────────────────────────────

    def generate(
        self,
        tar_candidates: list["TARCandidate"],
        output_dir:     Optional[Path] = None,
    ) -> list[TARDocument]:
        """Generate TARs for all candidates."""

        print(f"\n[tar_generator] Starting TAR generation — {len(tar_candidates)} candidates")

        documents    = []
        total_in     = 0
        total_out    = 0

        for i, candidate in enumerate(tar_candidates, 1):
            print(f"\n[tar_generator] [{i}/{len(tar_candidates)}] {candidate.tar_id}")
            doc = self.generate_one(candidate)
            documents.append(doc)
            total_in  += doc.token_usage.get("total_input", 0)
            total_out += doc.token_usage.get("total_output", 0)
            if output_dir:
                self._save_tar(doc, output_dir)

        passed = sum(1 for d in documents if d.gate_passed)
        print(f"\n[tar_generator] Complete | {passed}/{len(documents)} passed gate | "
              f"{total_in:,} in / {total_out:,} out tokens")

        return documents


    def generate_one(self, candidate: "TARCandidate") -> TARDocument:
        """Generate a full TAR for a single (TA x SOBJ) candidate."""
        try:
            from utils import get_client, log_api_usage
        except ImportError:
            from mk_intel.ingestion.utils import get_client, log_api_usage

        client      = get_client(self.session)
        profile     = candidate.refined_profile.profile
        sections    = {}
        total_in    = 0
        total_out   = 0

        ctx = self._build_profile_context(profile, candidate)

        # Header — no LLM call
        sections["header"] = self._build_header(candidate, profile)

        # Section 1 — Effectiveness (gate check)
        eff, usage = self._call(
            client, self._prompt_effectiveness(ctx, candidate),
            f"tar_eff_{candidate.tar_id}", log_api_usage, max_tokens=3000
        )
        sections["effectiveness"] = eff
        total_in  += usage[0]
        total_out += usage[1]

        # Gate pass is deterministic — never trust LLM to apply threshold correctly
        rating           = int(eff.get("rating", 0))
        gate_passed      = rating > 2
        eff["gate_pass"] = gate_passed
        gate_fail_reason = (
            eff.get("gate_fail_reason") or
            f"Effectiveness rating {rating}/5 is below threshold (>2 required)."
        ) if not gate_passed else None

        if not gate_passed:
            print(f"[tar_generator]   GATE FAILED: {gate_fail_reason}")
            return self._make_doc(
                candidate, sections, False, gate_fail_reason,
                total_in, total_out
            )

        print(f"[tar_generator]   Gate passed (rating={eff.get('rating', '?')}/5)")

        # Section 2 — Conditions
        cond, usage = self._call(
            client, self._prompt_conditions(ctx, candidate, sections),
            f"tar_cond_{candidate.tar_id}", log_api_usage, max_tokens=5000
        )
        sections["conditions"] = cond
        total_in += usage[0]; total_out += usage[1]
        print(f"[tar_generator]   Conditions: {len(cond.get('external_conditions', []))} ext, "
              f"{len(cond.get('internal_conditions', []))} int")

        # Section 3 — Vulnerabilities
        vuln, usage = self._call(
            client, self._prompt_vulnerabilities(ctx, candidate, sections),
            f"tar_vuln_{candidate.tar_id}", log_api_usage, max_tokens=6000
        )
        sections["vulnerabilities"] = vuln
        total_in += usage[0]; total_out += usage[1]
        print(f"[tar_generator]   Vulnerabilities: {len(vuln.get('motives', []))} motives, "
              f"{len(vuln.get('psychographics', []))} psych")

        # Section 4 — Susceptibility
        susc, usage = self._call(
            client, self._prompt_susceptibility(ctx, candidate, sections),
            f"tar_susc_{candidate.tar_id}", log_api_usage, max_tokens=4000
        )
        sections["susceptibility"] = susc
        total_in += usage[0]; total_out += usage[1]
        print(f"[tar_generator]   Susceptibility: rating={susc.get('rating', '?')}/5")

        # Section 5 — Accessibility
        acc, usage = self._call(
            client, self._prompt_accessibility(ctx, candidate, sections),
            f"tar_acc_{candidate.tar_id}", log_api_usage, max_tokens=6000
        )
        sections["accessibility"] = acc
        total_in += usage[0]; total_out += usage[1]
        ch_count = len(acc) if isinstance(acc, list) else 0
        print(f"[tar_generator]   Accessibility: {ch_count} channels")

        # Section 6 — Narrative and actions
        narr, usage = self._call(
            client, self._prompt_narrative(ctx, candidate, sections),
            f"tar_narr_{candidate.tar_id}", log_api_usage, max_tokens=8000
        )
        sections["narrative_and_actions"] = narr
        total_in += usage[0]; total_out += usage[1]
        print(f"[tar_generator]   Narrative: "
              f"{len(narr.get('recommended_actions', []))} actions")

        # Section 7 — Assessment
        assess, usage = self._call(
            client, self._prompt_assessment(ctx, candidate, sections),
            f"tar_assess_{candidate.tar_id}", log_api_usage, max_tokens=5000
        )
        sections["assessment"] = assess
        total_in += usage[0]; total_out += usage[1]
        print(f"[tar_generator]   Assessment: {len(assess.get('metrics', []))} metrics")

        # Section 8 — Traceability
        trace, usage = self._call(
            client, self._prompt_traceability(ctx, candidate, sections),
            f"tar_trace_{candidate.tar_id}", log_api_usage, max_tokens=4000
        )
        sections["traceability"] = trace
        total_in += usage[0]; total_out += usage[1]
        print(f"[tar_generator]   Traceability: confidence={trace.get('confidence', {}).get('level', '?')}")

        print(f"[tar_generator]   Tokens: {total_in} in / {total_out} out")

        return self._make_doc(candidate, sections, True, None, total_in, total_out)


    # ── Context builder ───────────────────────────────────────────────────────

    def _build_profile_context(self, profile: dict, candidate: "TARCandidate") -> str:
        """Build compliance-filtered profile context string."""
        behavioral = {
            k: v for k, v in profile.get("behavioral_signals", {}).items()
            if not any(excl in k for excl in self._excluded) and v is not None
        }
        structural = {
            f: profile.get(f) for f in [
                "dominant_age_bin", "dominant_income_tier",
                "dominant_household_income_tier", "dominant_edu_tier",
                "dominant_emp_tier", "dominant_mar_tier", "dominant_tenure",
            ]
            if f not in self._excluded and profile.get(f) is not None
        }
        zip_ctx = ""
        if "zip_inferred_income_tier" not in self._excluded:
            z = profile.get("zip_inferred_income_tier")
            if z:
                zip_ctx = f"ZIP-inferred household income tier: {z}\n"

        company = self.session.company
        obj_stmt = (self.session.objective.statement
                    if hasattr(self.session, "objective") and self.session.objective
                    else "Not specified")

        return f"""AUDIENCE PROFILE
================
Audience name  : {profile.get('company_specific_name') or profile.get('archetype_name', 'Unknown')}
Archetype base : {profile.get('archetype_name', 'Unknown')}
Source BTA     : {profile.get('source_bta_id', 'Unknown')}
Cluster        : {profile.get('cluster_id', 'Unknown')}
Confidence case: {candidate.confidence_case} (A=full alignment, B1=income diverges, B2=race diverges, C=LLM custom)
BTA confidence : {profile.get('bta_match_confidence', 'unknown')}

IMPORTANT: Refer to this audience throughout the report as "{profile.get('company_specific_name') or profile.get('archetype_name', 'Unknown')}" — do NOT use the archetype base name as the primary audience identifier.

Structural profile:
{json.dumps(structural, indent=2)}

Psychographic summary:
{profile.get('psych_summary', 'Not available')}

Media summary:
{profile.get('media_summary', 'Not available')}

Motivational drivers:
{profile.get('motivational_drivers', 'Not available')}

Key barriers:
{profile.get('key_barriers', 'Not available')}

Trust cues:
{profile.get('trust_cues', 'Not available')}

Susceptibility notes:
{profile.get('susceptibility_notes', 'Not available')}

{zip_ctx}Behavioral signals from company data:
{json.dumps(behavioral, indent=2)}

CAMPAIGN CONTEXT
================
Company    : {company.name if company else 'Unknown'}
Industry   : {getattr(company, 'industry', 'Unknown') if company else 'Unknown'}
Objective  : {obj_stmt}
SOBJ       : {candidate.sobj_statement}
Direction  : {candidate.sobj_direction}"""


    def _build_header(self, candidate: "TARCandidate", profile: dict) -> dict:
        """Build TAR header — no LLM call."""
        obj = (self.session.objective
               if hasattr(self.session, "objective") and self.session.objective
               else None)
        return {
            "objective": {
                "id":        getattr(obj, "id", "OBJ-01") if obj else "OBJ-01",
                "statement": obj.statement if obj else "Not specified",
            },
            "supporting_objective": {
                "id":        candidate.sobj_id,
                "statement": candidate.sobj_statement,
                "direction": candidate.sobj_direction,
            },
            "target_audience": {
                "id":         candidate.ta_id,
                "definition": profile.get("company_specific_name") or profile.get("archetype_name", "Unknown"),
                "bta_definition": profile.get("archetype_name", "Unknown"),
                "actor_type": "primary",
                "audience_size_estimate": {
                    "value":      profile.get("cell_size", 0),
                    "unit":       "individuals",
                    "confidence": profile.get("bta_match_confidence", "low"),
                    "source":     "company_data — cluster cell size from ingestion",
                },
            },
        }


    # ── LLM call helper ───────────────────────────────────────────────────────

    def _call(
        self,
        client,
        prompt: str,
        log_key: str,
        log_api_usage,
        max_tokens: int = 2000,
    ) -> tuple[dict, tuple[int, int]]:
        """Single LLM call — parse JSON response, return (result, (in, out))."""
        response = client.messages.create(
            model      = "claude-haiku-4-5-20251001",
            max_tokens = max_tokens,
            messages   = [{"role": "user", "content": prompt}],
        )
        log_api_usage(response, log_key, self.session)
        result = self._parse_json(response.content[0].text)
        return result, (response.usage.input_tokens, response.usage.output_tokens)


    def _parse_json(self, raw: str) -> dict:
        """Strip markdown fences, attempt JSON repair, then parse."""
        clean = raw.strip().replace("```json", "").replace("```", "").strip()
        try:
            return json.loads(clean)
        except json.JSONDecodeError:
            pass

        # Attempt repair — truncate to last valid complete JSON structure
        # Walk backwards from end to find a valid parse point
        for end in range(len(clean), 0, -1):
            candidate = clean[:end].rstrip().rstrip(",")
            # Try closing with matching braces/brackets
            opens_b = candidate.count("{") - candidate.count("}")
            opens_r = candidate.count("[") - candidate.count("]")
            if opens_b >= 0 and opens_r >= 0:
                repaired = candidate + "]" * opens_r + "}" * opens_b
                try:
                    result = json.loads(repaired)
                    print(f"[tar_generator] ⚠ JSON repaired — truncated at char {end}/{len(clean)}")
                    return result
                except json.JSONDecodeError:
                    continue

        return {"raw_output": clean, "parse_error": True}


    def _make_doc(
        self,
        candidate:        "TARCandidate",
        sections:         dict,
        gate_passed:      bool,
        gate_fail_reason: Optional[str],
        total_in:         int,
        total_out:        int,
    ) -> TARDocument:
        return TARDocument(
            tar_id           = candidate.tar_id,
            ta_id            = candidate.ta_id,
            sobj_id          = candidate.sobj_id,
            sobj_statement   = candidate.sobj_statement,
            sobj_direction   = candidate.sobj_direction,
            sections         = sections,
            gate_passed      = gate_passed,
            gate_fail_reason = gate_fail_reason,
            confidence_case  = candidate.confidence_case,
            compliance_mode  = self.compliance_mode,
            created_at       = datetime.now(timezone.utc).isoformat(),
            token_usage      = {"total_input": total_in, "total_output": total_out},
        )


    def _save_tar(self, doc: TARDocument, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        tar_dict = doc.to_dict()

        # Persist company_specific_name as audience_name so app, export,
        # and TAR body all reference the same consistent name.
        profile = doc.sections.get("header", {}).get("target_audience", {})
        company_specific_name = profile.get("definition", "")
        if company_specific_name:
            tar_dict["audience_name"] = company_specific_name

        with open(output_dir / f"{doc.tar_id}.json", "w") as f:
            json.dump(tar_dict, f, indent=2, default=str)


    # ── Section prompts ───────────────────────────────────────────────────────

    # ── Context summary helpers ───────────────────────────────────────────────
    # Pass only IDs + one-line descriptions to downstream prompts.
    # This reduces conditions from ~2,600 tokens to ~200 tokens.

    def _summarize_conditions(self, cond: dict) -> str:
        """Compact conditions summary for downstream prompts."""
        if not cond or cond.get("parse_error"):
            return "CONDITIONS: not available"
        lines = ["CONDITIONS SUMMARY (use IDs for cross-referencing):"]
        for c in cond.get("external_conditions", []):
            lines.append(f"  {c.get('id','?')} [external]: {str(c.get('description',''))[:100]}")
        for c in cond.get("internal_conditions", []):
            lines.append(f"  {c.get('id','?')} [internal]: {str(c.get('description',''))[:100]}")
        for c in cond.get("positive_consequences", []):
            lines.append(f"  {c.get('id','?')} [positive]: {str(c.get('description',''))[:80]}")
        for c in cond.get("negative_consequences", []):
            lines.append(f"  {c.get('id','?')} [negative]: {str(c.get('description',''))[:80]}")
        return "\n".join(lines)

    def _summarize_vulnerabilities(self, vuln: dict) -> str:
        """Compact vulnerabilities summary for downstream prompts."""
        if not vuln or vuln.get("parse_error"):
            return "VULNERABILITIES: not available"
        lines = ["VULNERABILITIES SUMMARY (use IDs for cross-referencing):"]
        for m in vuln.get("motives", []):
            lines.append(f"  {m.get('id','?')} [motive/{m.get('priority','?')}]: {str(m.get('description',''))[:100]}")
        for p in vuln.get("psychographics", []):
            lines.append(f"  {p.get('id','?')} [psych]: {str(p.get('description',''))[:100]}")
        for s in vuln.get("symbols_and_cues", []):
            lines.append(f"  {s.get('id','?')} [symbol]: {str(s.get('description',''))[:80]}")
        return "\n".join(lines)

    def _summarize_susceptibility(self, susc: dict) -> str:
        """Compact susceptibility summary for downstream prompts."""
        if not susc or susc.get("parse_error"):
            return "SUSCEPTIBILITY: not available"
        rating = susc.get("rating", "?")
        approach = susc.get("recommended_approach", {}).get("primary_approach", "?")
        alignment = susc.get("value_belief_alignment", {}).get("alignment_direction", "?")
        rewards = len(susc.get("perceived_rewards", []))
        risks   = len(susc.get("perceived_risks", []))
        return (f"SUSCEPTIBILITY SUMMARY: rating={rating}/5 | approach={approach} | "
                f"alignment={alignment} | rewards={rewards} | risks={risks}")

    def _summarize_accessibility(self, acc) -> str:
        """Compact accessibility summary for downstream prompts."""
        if not acc or (isinstance(acc, dict) and acc.get("parse_error")):
            return "ACCESSIBILITY: not available"
        channels = acc if isinstance(acc, list) else []
        lines = ["ACCESSIBILITY SUMMARY:"]
        for ch in channels:
            violates = " [RESTRICTED]" if ch.get("violates_restrictions") else ""
            lines.append(f"  {ch.get('channel_name','?')} (reach={ch.get('reach_quality','?')}/5){violates}")
        return "\n".join(lines)

    SOURCE_RULE = """SOURCE TAGGING RULES (mandatory):
Tag every factual claim with its source:
  company_data   = from behavioral signals on the TA card (LTV, sessions, churn_risk, NPS etc.)
  bta_baseline   = from structural/psych/media fields derived from ACS/GSS/Pew
  zip_inference  = from ZIP enrichment signals
  llm_inference  = your reasoning — flag these explicitly for analyst review
Never make claims the profile does not support. State uncertainty rather than inventing confidence."""


    def _prompt_effectiveness(self, ctx: str, candidate: "TARCandidate") -> str:
        return f"""{ctx}

{self.SOURCE_RULE}

TASK: Analyze effectiveness — can this audience segment perform the desired behavior?

Analyze:
1. What is the single concrete action required by the SOBJ?
2. Does the TA have autonomous decision rights to take that action?
3. Do they have the required resources (time, money, access, permissions)?
4. Are there restrictions that constrain or prevent the behavior?
5. Does this TA materially contribute to the supporting objective?

Gate: rating <= 2 = disqualified (TA cannot accomplish SOBJ).

Return JSON:
{{
    "desired_behavior": {{
        "statement": "single concrete action",
        "is_specific": true,
        "is_observable": true,
        "is_measurable": true
    }},
    "authority_power_control": {{
        "decision_rights": "what the TA can autonomously decide",
        "decision_rights_score": <0-3>,
        "influence_over_others": "who the TA affects",
        "resource_access": "what resources are available",
        "resource_access_score": <0-2>
    }},
    "restrictions": [
        {{
            "type": "economic|legal|social_cultural|platform_access|physical_environmental|political_regulatory|security_safety",
            "description": "...",
            "inhibits_behavior": true,
            "severity": "low|medium|high",
            "source": "company_data|bta_baseline|llm_inference"
        }}
    ],
    "sobj_impact": {{
        "description": "why this TA matters to SOBJ accomplishment",
        "estimated_contribution": "quantified or qualified estimate",
        "source": "company_data|bta_baseline|llm_inference"
    }},
    "rating": <0-5>,
    "rating_rationale": "...",
    "desired_behavior_quality": <0-3: sum of is_specific + is_observable + is_measurable>,
    "restriction_count_high": <count of high-severity restrictions>,
    "sobj_contribution_estimated": <true|false>,
    "gate_pass": <true if rating > 2>,
    "gate_fail_reason": "<reason if false, null if true>"
}}
Return ONLY the JSON object."""


    def _prompt_conditions(self, ctx: str, candidate: "TARCandidate", sections: dict) -> str:
        return f"""{ctx}

EFFECTIVENESS (completed):
{json.dumps(sections.get('effectiveness', {}), indent=2)}

{self.SOURCE_RULE}

TASK: Analyze current behavioral conditions — why does this TA behave as it does TODAY?

Use IDs: EC1, EC2... external conditions; IC1, IC2... internal; PC1... positive consequences; NC1... negative; SC1... secondary.

Return JSON:
{{
    "current_behavior": {{
        "statement": "what the TA does now — single specific action",
        "is_specific": true,
        "is_observable": true,
        "is_measurable": true
    }},
    "external_conditions": [
        {{
            "id": "EC1",
            "type": "economic|political_regulatory|social_cultural|environmental_physical|competitive_market|technological_platform|security_safety",
            "description": "...",
            "is_cause_of_current_behavior": true,
            "source": "company_data|bta_baseline|llm_inference",
            "assumptions": "..."
        }}
    ],
    "internal_conditions": [
        {{
            "id": "IC1",
            "type": "economic|social_cultural|psychological",
            "description": "...",
            "is_cause_of_current_behavior": true,
            "source": "bta_baseline|llm_inference",
            "assumptions": "..."
        }}
    ],
    "positive_consequences": [
        {{
            "id": "PC1",
            "description": "...",
            "affects": "ta_directly",
            "reinforces_current_behavior": true,
            "source": "company_data|bta_baseline|llm_inference"
        }}
    ],
    "negative_consequences": [
        {{
            "id": "NC1",
            "description": "...",
            "affects": "ta_directly",
            "reinforces_current_behavior": false,
            "source": "company_data|bta_baseline|llm_inference"
        }}
    ],
    "secondary_consequences": [
        {{
            "id": "SC1",
            "description": "...",
            "affects": "third_party",
            "source": "llm_inference"
        }}
    ]
}}
Return ONLY the JSON object."""


    def _prompt_vulnerabilities(self, ctx: str, candidate: "TARCandidate", sections: dict) -> str:
        return f"""{ctx}

{self._summarize_conditions(sections.get('conditions', {}))}

{self.SOURCE_RULE}

TASK: Identify psychological vulnerabilities — stable levers to influence this TA toward the desired behavior.
Link vulnerabilities to condition IDs where possible.

Use IDs: M1, M2... motives; P1, P2... psychographics; D1, D2... demographics; S1, S2... symbols.

Return JSON:
{{
    "motives": [
        {{
            "id": "M1",
            "description": "...",
            "category": "primary|secondary",
            "priority": "critical|short_term|long_term",
            "behavioral_link": "how this motive influences behavior toward the SOBJ",
            "linked_condition_ids": ["EC1"],
            "source": "bta_baseline|company_data|llm_inference"
        }}
    ],
    "psychographics": [
        {{
            "id": "P1",
            "description": "attitude, value, belief, lifestyle, fear, or frustration",
            "behavioral_link": "how this can be leveraged",
            "linked_condition_ids": ["IC1"],
            "source": "bta_baseline|llm_inference"
        }}
    ],
    "demographics": [
        {{
            "id": "D1",
            "description": "demographic trait relevant to behavior",
            "behavioral_link": "how this affects behavior or targeting",
            "source": "bta_baseline|company_data"
        }}
    ],
    "symbols_and_cues": [
        {{
            "id": "S1",
            "description": "icon, narrative, identity signal, or symbol",
            "recognized_by_ta": true,
            "behavioral_link": "how this supports the argument",
            "source": "bta_baseline|llm_inference"
        }}
    ]
}}
Return ONLY the JSON object."""


    def _prompt_susceptibility(self, ctx: str, candidate: "TARCandidate", sections: dict) -> str:
        return f"""{ctx}

{self._summarize_conditions(sections.get('conditions', {}))}

{self._summarize_vulnerabilities(sections.get('vulnerabilities', {}))}

{self.SOURCE_RULE}

TASK: Estimate this TA's openness to persuasion toward the desired behavior.

Return JSON:
{{
    "perceived_risks": [
        {{
            "description": "negative consequence TA believes will happen if they do the desired behavior",
            "severity_to_ta": "low|medium|high",
            "linked_condition_ids": ["EC1"],
            "source": "bta_baseline|company_data|llm_inference"
        }}
    ],
    "perceived_rewards": [
        {{
            "description": "positive consequence TA believes will happen if they do the desired behavior",
            "salience_to_ta": "low|medium|high",
            "linked_vulnerability_ids": ["M1"],
            "source": "bta_baseline|company_data|llm_inference"
        }}
    ],
    "value_belief_alignment": {{
        "assessment": "how the desired behavior fits or conflicts with TA identity",
        "alignment_direction": "aligned|neutral|conflicted|opposed",
        "source": "bta_baseline|llm_inference"
    }},
    "rating": <1-5>,
    "rating_rationale": "...",
    "reward_count": <integer>,
    "reward_salience_avg": <float: avg where low=1, medium=2, high=3>,
    "risk_count": <integer>,
    "risk_severity_avg": <float: avg where low=1, medium=2, high=3>,
    "recommended_approach": {{
        "primary_approach": "informational|social_norms|self_efficacy|incentive|loss_aversion|identity_affirmation|friction_reduction|authority|scarcity|reciprocity|commitment_consistency",
        "secondary_approach": "optional",
        "sequencing_note": "if order matters"
    }},
    "audience_priority_recommendation": {{
        "priority": "first|simultaneous|later|deprioritize",
        "reason": "..."
    }}
}}
Return ONLY the JSON object."""


    def _prompt_accessibility(self, ctx: str, candidate: "TARCandidate", sections: dict) -> str:
        restrictions = sections.get("effectiveness", {}).get("restrictions", [])
        return f"""{ctx}

RESTRICTIONS from effectiveness:
{json.dumps(restrictions, indent=2)}

{self.SOURCE_RULE}

TASK: Identify channels that can reliably reach this TA.
Consider the TA's media behavior from the profile.
Flag channels that conflict with restrictions above.

Return a JSON array:
[
    {{
        "channel_name": "...",
        "channel_type": "digital_paid|digital_owned|digital_earned|in_app|email|sms|community|partner|in_person|print|broadcast|out_of_home",
        "reach_quality": <1-5>,
        "advantages": ["..."],
        "disadvantages": ["..."],
        "constraints": "specific restrictions for this channel",
        "violates_restrictions": false,
        "rating_rationale": "...",
        "source": "bta_baseline|company_data|llm_inference"
    }}
]
Return ONLY the JSON array."""


    def _prompt_narrative(self, ctx: str, candidate: "TARCandidate", sections: dict) -> str:
        return f"""{ctx}

{self._summarize_conditions(sections.get('conditions', {}))}

{self._summarize_vulnerabilities(sections.get('vulnerabilities', {}))}

{self._summarize_susceptibility(sections.get('susceptibility', {}))}

{self._summarize_accessibility(sections.get('accessibility', []))}

{self.SOURCE_RULE}

TASK: Build the persuasion logic and recommended actions.

Main argument structure: if [premise] then [consequence].
Supporting arguments must trace to specific condition/vulnerability IDs.
Actions must link to vulnerabilities, conditions, and arguments.

Return JSON:
{{
    "main_argument": {{
        "premise": "the 'if' — condition that makes behavior rational for this TA",
        "consequence": "the 'then' — what TA gains or avoids",
        "linked_vulnerability_ids": ["M1", "P1"]
    }},
    "supporting_arguments": [
        {{
            "id": "A1",
            "statement": "substantiating argument",
            "evidence_type": "factual|cause_effect|vulnerability_exploit|social_proof|authority",
            "linked_ids": ["EC1", "M1"],
            "source": "company_data|bta_baseline|llm_inference"
        }}
    ],
    "appeal_type": {{
        "primary": "gain|loss_avoidance|identity|authority|social_proof|scarcity|fairness|security|belonging|competence",
        "secondary": "optional",
        "rationale": "why this appeal fits",
        "linked_ids": ["M1"]
    }},
    "influence_techniques": [
        {{
            "technique_name": "...",
            "linked_argument_id": "A1",
            "linked_vulnerability_ids": ["M1"],
            "rationale": "..."
        }}
    ],
    "recommended_actions": [
        {{
            "action_id": "ACT1",
            "action_type": "message|offer|product_change|experience_change|partner_action|policy_change|customer_success_outreach|community_activation|content_asset|event",
            "description": "concrete action",
            "linked_vulnerability_ids": ["M1"],
            "linked_condition_ids": ["EC1"],
            "linked_argument_id": "A1",
            "channel": "which channel delivers this",
            "sequencing_order": 1
        }}
    ]
}}
Return ONLY the JSON object."""


    def _prompt_assessment(self, ctx: str, candidate: "TARCandidate", sections: dict) -> str:
        direction = candidate.sobj_direction
        anchor_rule = (
            "anchor metrics to DESIRED behavior"
            if direction in ("increase", "initiate")
            else "anchor metrics to CURRENT behavior"
        )
        return f"""{ctx}

NARRATIVE: {json.dumps(sections.get('narrative_and_actions', {}).get('main_argument', {}), indent=2)}
SOBJ DIRECTION: {direction} — {anchor_rule}

TASK: Define measurable criteria to evaluate whether this TA's behavior changed toward the SOBJ.

Return JSON:
{{
    "baseline_behavior": "current measurable state — quantified from company data where possible",
    "target_behavior": "desired measurable state — must reference SOBJ direction",
    "initial_assessment_question": "broad: has overall behavior changed?",
    "refined_assessment_question": "TA-specific: has THIS audience changed as SOBJ requires?",
    "metrics": [
        {{
            "metric_name": "...",
            "metric_type": "outcome|behavior|leading_indicator|process",
            "definition": "what exactly is measured",
            "measurement_method": "how to measure it",
            "frequency": "daily|weekly|monthly|quarterly|per_campaign",
            "success_threshold": "what constitutes success",
            "direction_check": "anchored_to_desired_behavior|anchored_to_current_behavior"
        }}
    ]
}}
Return ONLY the JSON object."""


    def _prompt_traceability(self, ctx: str, candidate: "TARCandidate", sections: dict) -> str:
        vuln = sections.get("vulnerabilities", {})
        narr = sections.get("narrative_and_actions", {})
        return f"""{ctx}

TAR SUMMARY:
- Effectiveness rating: {sections.get('effectiveness', {}).get('rating', '?')}/5
- Gate passed: {sections.get('effectiveness', {}).get('gate_pass', '?')}
- Motives identified: {len(vuln.get('motives', []))}
- Psychographics identified: {len(vuln.get('psychographics', []))}
- Main argument: {json.dumps(narr.get('main_argument', {}), indent=2)[:300]}
- Confidence case: {candidate.confidence_case}

TASK: Complete traceability — sources, assumptions, confidence, ethical guardrails.

Note: Case C confidence cases should reflect lower overall confidence.
llm_inference tagged claims should be explicitly flagged in assumptions.

Return JSON:
{{
    "sources": [
        {{
            "source_id": "SRC1",
            "description": "what this source provides",
            "type": "primary_data|secondary_research|expert_judgment|platform_analytics|public_dataset|internal_dataset"
        }}
    ],
    "assumptions": [
        {{
            "assumption_id": "ASS1",
            "statement": "what we are assuming",
            "risk_if_wrong": "what breaks if this assumption is incorrect"
        }}
    ],
    "confidence": {{
        "level": "low|medium|high",
        "rationale": "explain based on data richness, confidence case, and proportion of llm_inference claims"
    }},
    "ethical_guardrails": {{
        "excluded_tactics": ["tactics that must not be used with this TA"],
        "privacy_constraints": ["data handling constraints"],
        "fairness_constraints": ["non-discrimination constraints"]
    }}
}}
Return ONLY the JSON object."""


# ── Scoring adapter ───────────────────────────────────────────────────────────

def tar_to_ta_input(doc: TARDocument):
    """
    Convert a TARDocument to a TAInput object for mk_ta_scoring_algorithm.
    Returns None if the TAR failed the gate.

    Extracts the scalar integer/float fields the scoring algorithm needs
    from the rich TAR JSON sections.
    """
    try:
        from mk_ta_scoring_algorithm import (
            TAInput, EffectivenessInput, SusceptibilityInput,
            VulnerabilityInput, AccessibilityInput, AudienceSizeInput,
            SOBJDirection, AlignmentDirection,
        )
    except ImportError:
        from mk_intel.mk_ta_scoring_algorithm import (
            TAInput, EffectivenessInput, SusceptibilityInput,
            VulnerabilityInput, AccessibilityInput, AudienceSizeInput,
            SOBJDirection, AlignmentDirection,
        )

    if not doc.gate_passed:
        return None

    eff  = doc.sections.get("effectiveness", {})
    susc = doc.sections.get("susceptibility", {})
    vuln = doc.sections.get("vulnerabilities", {})
    acc  = doc.sections.get("accessibility", [])
    hdr  = doc.sections.get("header", {})
    apc  = eff.get("authority_power_control", {})

    # Effectiveness
    effectiveness = EffectivenessInput(
        rating                      = int(eff.get("rating", 3)),
        decision_rights_score       = int(apc.get("decision_rights_score", 1)),
        resource_access_score       = int(apc.get("resource_access_score", 1)),
        restriction_count           = int(eff.get("restriction_count_high", 0)),
        desired_behavior_quality    = int(eff.get("desired_behavior_quality", 2)),
        sobj_contribution_estimated = bool(eff.get("sobj_contribution_estimated", True)),
    )

    # Susceptibility
    alignment_map = {
        "aligned":    AlignmentDirection.ALIGNED,
        "neutral":    AlignmentDirection.NEUTRAL,
        "conflicted": AlignmentDirection.CONFLICTED,
        "opposed":    AlignmentDirection.OPPOSED,
    }
    vba = susc.get("value_belief_alignment", {})
    susceptibility = SusceptibilityInput(
        rating              = int(susc.get("rating", 3)),
        reward_count        = int(susc.get("reward_count",
                                           len(susc.get("perceived_rewards", [])))),
        reward_salience     = float(susc.get("reward_salience_avg", 2.0)),
        risk_count          = int(susc.get("risk_count",
                                           len(susc.get("perceived_risks", [])))),
        risk_severity       = float(susc.get("risk_severity_avg", 2.0)),
        alignment_direction = alignment_map.get(
            vba.get("alignment_direction", "neutral"),
            AlignmentDirection.NEUTRAL,
        ),
    )

    # Vulnerabilities
    motives  = vuln.get("motives", [])
    psychs   = vuln.get("psychographics", [])
    symbols  = vuln.get("symbols_and_cues", [])
    demos    = vuln.get("demographics", [])

    vulnerabilities = VulnerabilityInput(
        motive_count                = len(motives),
        critical_motive_count       = sum(1 for m in motives
                                          if m.get("priority") == "critical"),
        sourced_motive_count        = sum(1 for m in motives
                                          if m.get("source") != "llm_inference"),
        psychographic_count         = len(psychs),
        sourced_psychographic_count = sum(1 for p in psychs
                                          if p.get("source") != "llm_inference"),
        demographic_count           = len(demos),
        symbol_count                = len(symbols),
        sourced_symbol_count        = sum(1 for s in symbols
                                          if s.get("recognized_by_ta")
                                          and s.get("source") != "llm_inference"),
    )

    # Accessibility
    channels = acc if isinstance(acc, list) else []
    accessibility = AccessibilityInput(
        channels=[
            {
                "reach_quality":         ch.get("reach_quality", 3),
                "violates_restrictions": ch.get("violates_restrictions", False),
            }
            for ch in channels
        ]
    )

    # Audience size
    ta_hdr   = hdr.get("target_audience", {})
    size_est = ta_hdr.get("audience_size_estimate", {})
    audience_size = AudienceSizeInput(
        value      = size_est.get("value"),
        unit       = size_est.get("unit", "individuals"),
        confidence = size_est.get("confidence", "low"),
    )

    # SOBJ direction
    direction_map = {
        "increase": SOBJDirection.INCREASE,
        "decrease": SOBJDirection.DECREASE,
        "maintain": SOBJDirection.MAINTAIN,
        "initiate": SOBJDirection.INITIATE,
        "stop":     SOBJDirection.STOP,
    }
    sobj_direction = direction_map.get(
        doc.sobj_direction.lower(), SOBJDirection.INCREASE
    )

    return TAInput(
        ta_id           = doc.ta_id,
        sobj_id         = doc.sobj_id,
        sobj_direction  = sobj_direction,
        effectiveness   = effectiveness,
        susceptibility  = susceptibility,
        vulnerabilities = vulnerabilities,
        accessibility   = accessibility,
        audience_size   = audience_size,
    )
