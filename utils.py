"""
utils.py
========
MK Intel — Shared Utilities

API key management, client initialization, and session mode handling.
Every LLM call in the platform goes through _get_client(session).

──────────────────────────────────────────────────────────────────
Session modes
──────────────────────────────────────────────────────────────────

    developer   Key from .env / environment variable. No quota.
                Used during local development and testing.

    byok        User-provided API key. No quota — they pay.
                Default public mode for technical evaluators
                and developers building on top of MK Intel.

    demo        Platform-funded key. Hard quota enforced.
                For recruiters and non-technical evaluators.
                Requires authentication (email magic link or
                GitHub OAuth). See ROADMAP.md for full spec.

    blocked     No key, no authentication. Rejected immediately.

──────────────────────────────────────────────────────────────────
Public API
──────────────────────────────────────────────────────────────────

    get_client(session)
        Returns an initialized Anthropic client for the session.
        Enforces quota in demo mode.
        Raises if no valid key is available.

    detect_session_mode(session)
        Infers the session mode from available key sources.

    log_api_usage(response, step_name)
        Logs token usage for a completed API call.
        Helps users track consumption against demo quota.

Exceptions
──────────────────────────────────────────────────────────────────

    MKAuthError         No valid API key found.
    DemoQuotaExceededError  Demo quota exhausted.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional

import anthropic

if TYPE_CHECKING:
    from mk_intel_session import MKSession


# ── Session mode constants ────────────────────────────────────────────────────

SESSION_MODE_DEVELOPER = "developer"
SESSION_MODE_BYOK      = "byok"
SESSION_MODE_DEMO      = "demo"
SESSION_MODE_BLOCKED   = "blocked"

VALID_SESSION_MODES = {
    SESSION_MODE_DEVELOPER,
    SESSION_MODE_BYOK,
    SESSION_MODE_DEMO,
    SESSION_MODE_BLOCKED,
}

# ── Demo quota limits ─────────────────────────────────────────────────────────
# These are enforced by the demo auth backend (see ROADMAP.md).
# Defined here as constants so they are visible to all platform code.

DEMO_QUOTA_RUNS          = 2        # full analysis runs per demo session
DEMO_QUOTA_TOKENS        = 30_000   # hard token cap per demo session
DEMO_RECRUITER_QUOTA_RUNS   = 3     # runs for recruiter code sessions
DEMO_RECRUITER_QUOTA_TOKENS = 50_000


# ── Exceptions ────────────────────────────────────────────────────────────────

class MKAuthError(Exception):
    """
    Raised when no valid API key is available for the session.
    """
    pass


class DemoQuotaExceededError(Exception):
    """
    Raised when a demo session has exhausted its quota.
    Includes a user-facing message directing to BYOK.
    """
    pass


# ── Client initialization ─────────────────────────────────────────────────────

def get_client(session: "MKSession") -> anthropic.Anthropic:
    """
    Returns an initialized Anthropic client for the session.

    Handles all four session modes:
        developer : key from ANTHROPIC_API_KEY environment variable
        byok      : key from session.api_key
        demo      : key from ANTHROPIC_API_KEY (platform-funded)
                    + quota check before returning client
        blocked   : raises MKAuthError immediately

    Args:
        session : active MKSession object.

    Returns:
        anthropic.Anthropic client initialized with the correct key.

    Raises:
        MKAuthError            : no valid API key available.
        DemoQuotaExceededError : demo session quota exhausted.
        ValueError             : invalid session_mode.
    """
    mode = detect_session_mode(session)

    if mode == SESSION_MODE_BLOCKED:
        raise MKAuthError(
            "No API key provided and no demo session active.\n\n"
            "To use MK Intel:\n"
            "  1. Provide your own Anthropic API key (BYOK):\n"
            "     session.api_key = 'sk-ant-...'\n"
            "  2. Or request a free demo session at: [demo URL]\n"
            "  3. Or set the ANTHROPIC_API_KEY environment variable\n"
            "     for local development."
        )

    if mode == SESSION_MODE_DEMO:
        _check_demo_quota(session)
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise MKAuthError(
                "Demo mode requires ANTHROPIC_API_KEY to be set in the "
                "platform environment. Contact the platform administrator."
            )
        return anthropic.Anthropic(api_key=key)

    if mode == SESSION_MODE_BYOK:
        key = session.api_key
        if not key:
            raise MKAuthError(
                "BYOK mode requires session.api_key to be set.\n"
                "Set it before making any API calls:\n"
                "    session.api_key = 'sk-ant-...'"
            )
        return anthropic.Anthropic(api_key=key)

    if mode == SESSION_MODE_DEVELOPER:
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise MKAuthError(
                "Developer mode requires ANTHROPIC_API_KEY environment variable.\n"
                "Add it to your .env file:\n"
                "    ANTHROPIC_API_KEY=sk-ant-..."
            )
        return anthropic.Anthropic(api_key=key)

    raise ValueError(f"Unknown session_mode: '{mode}'")


def detect_session_mode(session: "MKSession") -> str:
    """
    Infers the session mode from available key sources.

    Priority:
        1. session.session_mode if explicitly set and valid
        2. session.api_key present → byok
        3. ANTHROPIC_API_KEY env var present + no demo token → developer
        4. session.demo_token present → demo
        5. nothing → blocked

    Args:
        session : active MKSession object.

    Returns:
        Session mode string.
    """
    # Explicit mode set on session — trust it if valid
    if hasattr(session, "session_mode") and session.session_mode:
        mode = session.session_mode
        if mode in VALID_SESSION_MODES:
            return mode

    # BYOK — user provided their own key
    if hasattr(session, "api_key") and session.api_key:
        return SESSION_MODE_BYOK

    # Demo — session has a demo token
    if hasattr(session, "demo_token") and session.demo_token:
        return SESSION_MODE_DEMO

    # Developer — env var set, no user key, no demo token
    if os.environ.get("ANTHROPIC_API_KEY"):
        return SESSION_MODE_DEVELOPER

    # Nothing available
    return SESSION_MODE_BLOCKED


# ── Usage logging ─────────────────────────────────────────────────────────────

def log_api_usage(
    response: anthropic.types.Message,
    step_name: str,
    session: Optional["MKSession"] = None,
) -> dict:
    """
    Logs token usage for a completed API call.

    Prints a summary to stdout and optionally updates the session's
    token usage counter (used for demo quota tracking).

    Args:
        response  : Anthropic API response object.
        step_name : human-readable name of the pipeline step.
        session   : active MKSession (optional — updates token counter
                    if session is in demo mode).

    Returns:
        Dict with usage stats:
        {
            "step":          str,
            "input_tokens":  int,
            "output_tokens": int,
            "total_tokens":  int,
        }
    """
    input_tokens  = response.usage.input_tokens
    output_tokens = response.usage.output_tokens
    total_tokens  = input_tokens + output_tokens

    print(
        f"[mk_intel] {step_name} — "
        f"{input_tokens} in / {output_tokens} out / "
        f"{total_tokens} total tokens"
    )

    # Update demo session token counter if applicable
    if session is not None:
        mode = detect_session_mode(session)
        if mode == SESSION_MODE_DEMO:
            if hasattr(session, "demo_tokens_used"):
                session.demo_tokens_used = (
                    getattr(session, "demo_tokens_used", 0) + total_tokens
                )
            remaining = DEMO_QUOTA_TOKENS - getattr(session, "demo_tokens_used", 0)
            print(f"[mk_intel] Demo quota remaining: ~{remaining:,} tokens")

    return {
        "step":          step_name,
        "input_tokens":  input_tokens,
        "output_tokens": output_tokens,
        "total_tokens":  total_tokens,
    }


# ── Demo quota enforcement ────────────────────────────────────────────────────

def _check_demo_quota(session: "MKSession") -> None:
    """
    Checks whether the demo session has remaining quota.

    TODO (Phase P6 — Demo Auth System):
        Replace this stub with a live quota check against the
        demo_sessions SQLite table. See ROADMAP.md for full spec.

        The check should verify:
            1. session.demo_token is valid and not expired
            2. runs_used < quota_runs
            3. tokens_used < quota_tokens

        If any check fails, raise DemoQuotaExceededError with the
        appropriate user-facing message.

    Current behavior (stub):
        Passes through without enforcement.
        Safe for development — demo mode is not yet publicly exposed.

    Args:
        session : active MKSession in demo mode.

    Raises:
        DemoQuotaExceededError : when quota is exhausted (future).
    """
    # ── TODO: implement live quota check (Phase P6) ───────────────────────────
    # from demo_auth import check_quota  # to be built
    # check_quota(session.demo_token)
    # ─────────────────────────────────────────────────────────────────────────

    # Local token counter check (client-side only, not tamper-proof)
    tokens_used = getattr(session, "demo_tokens_used", 0)
    if tokens_used >= DEMO_QUOTA_TOKENS:
        raise DemoQuotaExceededError(
            f"Demo quota exhausted ({tokens_used:,} / {DEMO_QUOTA_TOKENS:,} tokens used).\n\n"
            "To continue using MK Intel, provide your own Anthropic API key:\n"
            "    session.api_key = 'sk-ant-...'\n\n"
            "Get your API key at: https://console.anthropic.com"
        )
