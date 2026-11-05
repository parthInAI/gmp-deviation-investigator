"""Deterministic citation checking and stripping, used by the reviewer node and (independently) by the evals.

A citation is valid when:
  SENSOR:...      the historian confirms the signal is outside its normal band in that window
  regulation      the clause id was returned by the regulatory analyst in this run
  SOP / DEV       the section or deviation was returned by the QMS in this run

A claim keeps only its valid citations and is removed if none remain ("uncited claims are
stripped"). A hypothesis whose supporting evidence is all removed is dropped and the rest
re-ranked: a cause with no evidence is not ranked (SOP-QA-011 §4.2).
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field

from agents.schemas import Claim, Hypothesis, Report

SENSOR = re.compile(r"^SENSOR:\d+:[a-z0-9_]+:\d+(?:\.\d+)?-\d+(?:\.\d+)?$")
REGULATION = re.compile(r"^(EU-GMP-[A-Za-z0-9-]+|FDA-WL-[A-Za-z0-9-]+):[A-Za-z0-9.\-]+$")
SOP = re.compile(r"^SOP-[A-Z]+-\d{3} §\d+(?:\.\d+)?$")
DEV = re.compile(r"^DEV-\d{4}-\d{3}$")


def kind(cite: str) -> str:
    c = cite.strip()
    if SENSOR.match(c):
        return "sensor"
    if SOP.match(c):
        return "sop"
    if DEV.match(c):
        return "deviation"
    if REGULATION.match(c):
        return "regulation"
    return "invalid"


@dataclass
class Checker:
    """retrieved: every non-sensor id returned by a tool during this run. sensor_ok: historian check."""

    retrieved: set[str]
    sensor_ok: Callable[[str], bool]
    _cache: dict = field(default_factory=dict)

    def valid(self, cite: str) -> bool:
        c = cite.strip()
        if c not in self._cache:
            k = kind(c)
            self._cache[c] = self.sensor_ok(c) if k == "sensor" else (k != "invalid" and c in self.retrieved)
        return self._cache[c]


@dataclass
class StripResult:
    report: Report
    claims_before: int
    claims_after: int
    citations_before: int
    citations_invalid: int
    removed: list[dict]
    hypotheses_dropped: list[str]

    @property
    def draft_validity(self) -> float:
        """Share of the draft's claims whose citations were all valid before stripping."""
        bad = sum(1 for r in self.removed if r["kind"] == "claim") + sum(
            1 for r in self.removed if r["kind"] == "citation" and r.get("claim_kept")
        )
        return 1 - bad / self.claims_before if self.claims_before else 1.0


def _clean_claims(claims: list[Claim], where: str, chk: Checker, log: list[dict], counts: list[int]) -> list[Claim]:
    out = []
    for i, cl in enumerate(claims):
        counts[0] += 1
        counts[1] += len(cl.citations)
        good = [c for c in cl.citations if chk.valid(c)]
        bad = [c for c in cl.citations if not chk.valid(c)]
        counts[2] += len(bad)
        if not good:
            log.append({"kind": "claim", "where": f"{where}[{i}]", "text": cl.text, "citations": cl.citations})
            continue
        if bad:
            log.append({"kind": "citation", "where": f"{where}[{i}]", "removed": bad, "claim_kept": True})
        out.append(Claim(text=cl.text, citations=good))
    return out


def strip(report: Report, chk: Checker) -> StripResult:
    log: list[dict] = []
    counts = [0, 0, 0]  # claims, citations, invalid citations
    sections = {}
    for name in ("event", "evidence", "product_impact", "capa", "regulatory_basis", "similar_cases"):
        sections[name] = _clean_claims(getattr(report, name), name, chk, log, counts)
    hyps, dropped = [], []
    for j, h in enumerate(sorted(report.hypotheses, key=lambda h: h.rank)):
        ev_for = _clean_claims(h.evidence_for, f"hypotheses[{j}].evidence_for", chk, log, counts)
        ev_against = _clean_claims(h.evidence_against, f"hypotheses[{j}].evidence_against", chk, log, counts)
        if not ev_for:
            dropped.append(h.statement)
            continue
        hyps.append(h.model_copy(update={"evidence_for": ev_for, "evidence_against": ev_against}))
    hyps = [h.model_copy(update={"rank": i + 1}) for i, h in enumerate(hyps)]
    clean = report.model_copy(update={**sections, "hypotheses": hyps})
    after = sum(len(getattr(clean, n)) for n in sections) + sum(
        len(h.evidence_for) + len(h.evidence_against) for h in hyps
    )
    return StripResult(clean, counts[0], after, counts[1], counts[2], log, dropped)


def all_claims(report: Report) -> list[tuple[str, Claim]]:
    out = []
    for name in ("event", "evidence", "product_impact", "capa", "regulatory_basis", "similar_cases"):
        out += [(f"{name}[{i}]", c) for i, c in enumerate(getattr(report, name))]
    for j, h in enumerate(report.hypotheses):
        out += [(f"hypotheses[{j}].evidence_for[{i}]", c) for i, c in enumerate(h.evidence_for)]
        out += [(f"hypotheses[{j}].evidence_against[{i}]", c) for i, c in enumerate(h.evidence_against)]
    return out


def remove_claims(report: Report, refs: set[str]) -> Report:
    """Drop the claims the reviewer judged unsupported (by their location), then drop empty hypotheses."""

    def keep(prefix: str, claims: list[Claim]) -> list[Claim]:
        return [c for i, c in enumerate(claims) if f"{prefix}[{i}]" not in refs]

    upd = {
        n: keep(n, getattr(report, n))
        for n in ("event", "evidence", "product_impact", "capa", "regulatory_basis", "similar_cases")
    }
    hyps: list[Hypothesis] = []
    for j, h in enumerate(report.hypotheses):
        ef = keep(f"hypotheses[{j}].evidence_for", h.evidence_for)
        ea = keep(f"hypotheses[{j}].evidence_against", h.evidence_against)
        if ef:
            hyps.append(h.model_copy(update={"evidence_for": ef, "evidence_against": ea}))
    upd["hypotheses"] = [h.model_copy(update={"rank": i + 1}) for i, h in enumerate(hyps)]
    return report.model_copy(update=upd)
