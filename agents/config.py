"""Agent configuration: models, prices, limits. Frozen (committed) before the Phase 4 test run.

Model ids can be overridden by environment variables for development, but the gate run uses
exactly these values and records the model id each provider reports back.
"""

from __future__ import annotations

import os

MODELS = {
    # Report writer and the lead's planning: the strongest model in the loop.
    "writer": os.environ.get("GMPDI_WRITER_MODEL", "claude-sonnet-5-5"),
    # Sub-agents doing many small tool calls: process data analyst, case historian (CrewAI).
    "analyst": os.environ.get("GMPDI_ANALYST_MODEL", "claude-haiku-5-5"),
    "case_historian": os.environ.get("GMPDI_CASE_MODEL", "claude-haiku-5-5"),
    # Reviewer from a different vendor, so the writer never grades its own work.
    "reviewer": os.environ.get("GMPDI_REVIEWER_MODEL", "mistral-medium-latest"),
}

# USD per million tokens (input, output). Sources, checked 2026-11-05 and re-checked before the gate:
#   Claude Sonnet 5.5 $2 / $10; Claude Haiku 5.5 $0.10 / $0.50 for prompts up to 100k tokens
#   ($0.50 / $2.50 above); Mistral Medium 3.5 (mistral-medium-latest) $1.50 / $7.50.
PRICES_USD_PER_MTOK = {
    "claude-sonnet-5-5": (2.00, 10.00),
    "claude-haiku-5-5": (0.10, 0.50),
    "claude-haiku-5-5@>100k": (0.50, 2.50),
    "mistral-medium-latest": (1.50, 7.50),
    "mistral-medium-3-5": (1.50, 7.50),
}

TEMPERATURE = 0.0
MAX_ANALYST_STEPS = 12  # tool calls the process analyst may make
MAX_REVISIONS = 1  # the reviewer may send a draft back once
MAX_HYPOTHESES = 5
COST_LIMIT_EUR = 0.50  # gate criterion; a run that would exceed it is stopped, not finished


def price(model: str, input_tokens: int) -> tuple[float, float]:
    """(input, output) USD per million tokens for a model id as reported by the provider."""
    if model.startswith("claude-haiku-5-5") and input_tokens > 100_000:
        return PRICES_USD_PER_MTOK["claude-haiku-5-5@>100k"]
    for key, p in PRICES_USD_PER_MTOK.items():
        if model.startswith(key.split("@")[0]):
            return p
    raise KeyError(f"no price for model {model}; add it to agents/config.py before running")
