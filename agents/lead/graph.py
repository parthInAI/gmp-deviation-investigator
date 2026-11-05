"""Investigation lead (LangGraph supervisor): from an alarm to a cited draft that waits for a human.

    open_deviation -> analyse_process -> gather_evidence -> write_report -> review
    review --(send back, at most once)--> write_report
    review --> await_approval (interrupt) -> record_decision

The graph pauses at await_approval with status "awaiting_approval". Only a decision from a named
reviewer (approve, edit with a reason, reject with a reason) resumes it, and every decision is
signed into the audit trail. Nothing is released before that: release() refuses otherwise.

Dependencies (tools, analyst, writer, reviewer, cost meter) are injected per run through
config["configurable"]["deps"], so tests and offline runs use the same graph with fakes.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from agents import config as cfg
from agents.citations import Checker, kind, remove_claims, strip
from agents.llm import CostMeter
from agents.process_analyst import ANALYST_PROMPT
from agents.report.writer import REVIEWER_PROMPT, WRITER_PROMPT, evidence_pack
from agents.schemas import ProcessFindings, Report, Review
from agents.toolbox import Toolbox

PROMPT_VERSION = hashlib.sha256((ANALYST_PROMPT + WRITER_PROMPT + REVIEWER_PROMPT).encode()).hexdigest()[:12]

REGULATION_QUERIES = [
    "investigation of significant deviations to find the root cause and corrective and preventive action",
    "what to do when the true root cause of a deviation cannot be determined",
    "effectiveness of corrective and preventive actions after a deviation",
    "records of deviations in the batch processing record",
    "human review of an AI model output before a GMP decision",
]


@dataclass
class Deps:
    toolbox: Toolbox
    analyst: Callable[..., Awaitable[ProcessFindings]]
    writer: Callable[..., Awaitable[Report]]
    reviewer: Callable[..., Awaitable[Review]] | None
    meter: CostMeter = field(default_factory=CostMeter)


class State(TypedDict, total=False):
    batch_id: int
    status: str
    alarm: dict
    findings: dict
    pack: str
    retrieved: list[str]
    draft: dict
    revisions: int
    feedback: str | None
    review: dict
    strip_log: list[dict]
    report: dict
    decision: dict
    audit_signature: str


def _deps(config) -> Deps:
    return config["configurable"]["deps"]


async def open_deviation(state: State, config) -> State:
    alarm = await _deps(config).toolbox.get_alarm(state["batch_id"])
    return {"alarm": alarm, "status": "investigating" if alarm.get("alarm") else "no_alarm", "revisions": 0}


async def analyse_process(state: State, config) -> State:
    d = _deps(config)
    findings = await d.analyst(d.toolbox, state["batch_id"], callbacks=[d.meter])
    return {"findings": findings.model_dump()}


async def gather_evidence(state: State, config) -> State:
    d = _deps(config)
    tb = d.toolbox
    findings = ProcessFindings(**state["findings"])
    sub_queries = [f"{s} deviation investigation" for s in findings.suspected_subsystems[:3]]
    regs_lists, sop_lists, (cases, case_usage) = await asyncio.gather(
        asyncio.gather(*(tb.regulations(q, k=3) for q in REGULATION_QUERIES)),
        asyncio.gather(
            *(
                tb.search_sops(q, k=3)
                for q in [*sub_queries, "root cause analysis and CAPA", "process monitor alarm response"]
            )
        ),
        tb.similar_cases(
            f"Batch {state['batch_id']}. {findings.summary} " + " ".join(c.text for c in findings.timeline[:4])
        ),
    )
    for u in (case_usage or {}).get("calls", []):
        d.meter.add(u["model"], u["input_tokens"], u["output_tokens"], source="case_historian")
    regs = list({r["cite"]: r for rs in regs_lists for r in rs}.values())
    sops = list({s["cite"]: s for ss in sop_lists for s in ss}.values())
    pack = evidence_pack(state["batch_id"], state["alarm"], findings, regs, sops, cases)
    return {"pack": pack, "retrieved": sorted(tb.retrieved)}


async def write_report(state: State, config) -> State:
    d = _deps(config)
    draft = await d.writer(state["pack"], state.get("feedback"), callbacks=[d.meter])
    return {"draft": draft.model_dump()}


async def review(state: State, config) -> State:
    d = _deps(config)
    draft = Report(**state["draft"])
    sensor = sorted({c for _, cl in _claims(draft) for c in cl.citations if kind(c) == "sensor"})
    oks = await asyncio.gather(*(d.toolbox.check_sensor_citation(c) for c in sensor))
    checked = dict(zip(sensor, oks, strict=True))
    res = strip(draft, Checker(set(state["retrieved"]), lambda c: checked.get(c.strip(), False)))
    report, verdict = res.report, None
    if d.reviewer is not None and (report.hypotheses or report.evidence):
        verdict = await d.reviewer(state["pack"], report, callbacks=[d.meter])
        unsupported = {v.claim_ref for v in verdict.verdicts if not v.supported}
        report = remove_claims(report, unsupported)
    out: State = {
        "strip_log": [
            *state.get("strip_log", []),
            {
                "revision": state.get("revisions", 0),
                "removed": res.removed,
                "hypotheses_dropped": res.hypotheses_dropped,
                "draft_validity": res.draft_validity,
            },
        ],
        "review": verdict.model_dump() if verdict else {},
        "report": report.model_dump(),
    }
    if verdict and verdict.send_back and state.get("revisions", 0) < cfg.MAX_REVISIONS:
        out.update({"revisions": state.get("revisions", 0) + 1, "feedback": verdict.feedback, "status": "revising"})
    else:
        out["status"] = "awaiting_approval"
    return out


def _claims(report: Report):
    from agents.citations import all_claims

    return all_claims(report)


def await_approval(state: State, config) -> State:
    decision = interrupt(
        {
            "batch_id": state["batch_id"],
            "status": "awaiting_approval",
            "report": state["report"],
            "ask": "Approve, edit (with reason) or reject (with reason). Give your name.",
        }
    )
    return {"decision": decision}


async def record_decision(state: State, config) -> State:
    import json

    from agents import audit

    d = _deps(config)
    dec = state["decision"] or {}
    reviewer, action = str(dec.get("reviewer", "")).strip(), dec.get("decision")
    reason = str(dec.get("reason", "")).strip()
    if not reviewer or action not in ("approve", "edit", "reject") or (action != "approve" and not reason):
        raise ValueError("a decision needs a reviewer name, approve/edit/reject, and a reason unless approving")
    report = state["report"]
    if action == "edit":
        # A reviewer's edits are held to the same rule: every claim must still cite real evidence.
        edited = Report(**dec["report"])
        sensor = sorted({c for _, cl in _claims(edited) for c in cl.citations if kind(c) == "sensor"})
        oks = await asyncio.gather(*(d.toolbox.check_sensor_citation(c) for c in sensor))
        checked = dict(zip(sensor, oks, strict=True))
        report = strip(
            edited, Checker(set(state["retrieved"]), lambda c: checked.get(c.strip(), False))
        ).report.model_dump()
    entry = audit.append(
        {
            "batch_id": state["batch_id"],
            "reviewer": reviewer,
            "decision": action,
            "reason": reason,
            "report_sha256": audit.report_hash(json.dumps(report, sort_keys=True)),
            "models": cfg.MODELS,
            "prompt_version": PROMPT_VERSION,
            "cost_eur": round(d.meter.eur, 4),
        }
    )
    status = {"approve": "approved", "edit": "approved_with_edits", "reject": "rejected"}[action]
    return {"status": status, "report": report, "audit_signature": entry["signature"]}


def _after_open(state: State) -> str:
    return "analyse_process" if state["status"] == "investigating" else END


def _after_review(state: State) -> str:
    return "write_report" if state["status"] == "revising" else "await_approval"


def build(checkpointer=None):
    g = StateGraph(State)
    for name, fn in [
        ("open_deviation", open_deviation),
        ("analyse_process", analyse_process),
        ("gather_evidence", gather_evidence),
        ("write_report", write_report),
        ("review", review),
        ("await_approval", await_approval),
        ("record_decision", record_decision),
    ]:
        g.add_node(name, fn)
    g.add_edge(START, "open_deviation")
    g.add_conditional_edges("open_deviation", _after_open, ["analyse_process", END])
    g.add_edge("analyse_process", "gather_evidence")
    g.add_edge("gather_evidence", "write_report")
    g.add_edge("write_report", "review")
    g.add_conditional_edges("review", _after_review, ["write_report", "await_approval"])
    g.add_edge("await_approval", "record_decision")
    g.add_edge("record_decision", END)
    return g.compile(checkpointer=checkpointer)


class NotApproved(PermissionError):
    pass


async def release(graph, thread_id: str) -> dict:
    """The only way a report leaves the system: refused unless a named reviewer approved it."""
    snap = await graph.aget_state({"configurable": {"thread_id": thread_id}})
    st: dict[str, Any] = snap.values
    if st.get("status") not in ("approved", "approved_with_edits") or not (st.get("decision") or {}).get("reviewer"):
        raise NotApproved(f"report for thread {thread_id} has status {st.get('status')!r}: not released")
    return {"report": st["report"], "approved_by": st["decision"]["reviewer"], "audit_signature": st["audit_signature"]}
