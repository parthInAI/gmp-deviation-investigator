"""The investigation report: every statement is a claim, and every claim carries citations.

Citation forms (anything else is invalid and the claim is stripped):

    SENSOR:<batch>:<signal>:<start_h>-<end_h>   sensor evidence, checked against the data
    EU-GMP-Ch1:1.4  /  FDA-WL-Strides-2019:2    a regulation clause retrieved in this run
    SOP-FER-003 §4.3                            an SOP section retrieved in this run
    DEV-2024-007                                a past deviation retrieved in this run
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

_GATE = json.loads((Path(__file__).resolve().parents[1] / "evals" / "gate_criteria.json").read_text())
TAXONOMY: tuple[str, ...] = tuple(_GATE["root_cause_taxonomy"])
Category = Literal[
    "aeration_supply",
    "agitation",
    "substrate_feed",
    "ph_measurement",
    "ph_dosing",
    "coolant_supply",
    "temperature_measurement",
    "vessel_pressure",
    "precursor_feed",
    "oil_feed",
    "contamination",
    "raw_material",
    "operator_error",
    "other",
]
assert set(Category.__args__) == set(TAXONOMY), "schemas.Category must match evals/gate_criteria.json"


class Claim(BaseModel):
    text: str = Field(description="One factual statement, in plain words.")
    citations: list[str] = Field(description="Ids of the evidence that supports this statement (at least one).")


class Hypothesis(BaseModel):
    rank: int = Field(description="1 = most likely.")
    category: Category = Field(description="The root-cause category this hypothesis belongs to.")
    statement: str = Field(description="The candidate root cause, e.g. 'loss of air supply to the fermenter'.")
    evidence_for: list[Claim]
    evidence_against: list[Claim] = Field(default_factory=list)
    to_confirm: str = Field(description="What check or record would confirm or rule out this cause.")


class ProcessFindings(BaseModel):
    """What the process data analyst hands to the lead."""

    first_alarm_h: float | None
    timeline: list[Claim] = Field(description="What moved, when and how far, in order; each cites SENSOR evidence.")
    suspected_subsystems: list[str] = Field(
        description="Equipment or control loops the data points at, most likely first."
    )
    summary: str = Field(description="Two or three sentences for the rest of the team.")


class SimilarCase(BaseModel):
    deviation_id: str
    why_similar: str
    root_cause: str
    category: str
    capa_that_worked: str


class Report(BaseModel):
    batch_id: int
    title: str
    event: list[Claim] = Field(description="What happened and when (the alarm and the first signals to move).")
    evidence: list[Claim] = Field(description="The sensor evidence, in time order.")
    hypotheses: list[Hypothesis] = Field(description="Ranked candidate root causes, at most 5.")
    product_impact: list[Claim]
    capa: list[Claim] = Field(description="Proposed corrective and preventive actions.")
    regulatory_basis: list[Claim] = Field(description="The GMP requirements this investigation must meet.")
    similar_cases: list[Claim] = Field(description="Relevant past deviations and what their CAPA taught.")


class ClaimVerdict(BaseModel):
    claim_ref: str = Field(description="Where the claim sits, e.g. 'evidence[2]' or 'hypotheses[0].evidence_for[1]'.")
    supported: bool
    reason: str


class Review(BaseModel):
    """The reviewer's verdict on a draft."""

    score: int = Field(ge=1, le=5, description="1 = unusable, 5 = ready for a QA reviewer.")
    verdicts: list[ClaimVerdict] = Field(description="Only claims whose text is NOT supported by their cited evidence.")
    ranking_sound: bool = Field(description="Is the hypothesis ranking consistent with the evidence?")
    send_back: bool = Field(description="True if the draft needs one more revision.")
    feedback: str = Field(description="What the writer must fix, if sending back.")
