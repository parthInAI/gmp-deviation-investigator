"""Run an investigation, and record a reviewer's decision.

    python -m agents.run investigate --batch 1001                  # stops at "awaiting approval"
    python -m agents.run decide --thread <id> --reviewer "Name" --decision approve
    python -m agents.run decide --thread <id> --reviewer "Name" --decision reject --reason "..."
    python -m agents.run show --thread <id>

Production wiring by default: historian and QMS over MCP, case historian over A2A, Claude analyst
and writer, Mistral reviewer. Checkpoints persist in out/agents/checkpoints.sqlite, so a run can wait
for its reviewer across restarts. --analyst rules / --offline give a no-LLM path for checks.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from agents.lead.graph import Deps, build, release
from agents.llm import CostMeter
from agents.schemas import Report

OUT = Path(os.environ.get("GMPDI_AGENTS_OUT", "out/agents"))


@asynccontextmanager
async def checkpointer():
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

    OUT.mkdir(parents=True, exist_ok=True)
    async with AsyncSqliteSaver.from_conn_string(str(OUT / "checkpoints.sqlite")) as saver:
        yield saver


@asynccontextmanager
async def toolbox(offline: bool):
    if offline:
        from agents.toolbox import LocalToolbox

        yield LocalToolbox(store=os.environ.get("REGULATION_STORE", "memory"))
    else:
        from agents.toolbox import MCPToolbox

        async with MCPToolbox.connect() as tb:
            yield tb


def deps_for(tb, analyst: str, meter: CostMeter) -> Deps:
    """analyst='llm': Claude analyst + writer, Mistral reviewer. 'rules': no LLM anywhere (baseline, checks)."""
    from agents.process_analyst import llm_analyst, rules_analyst
    from agents.report import writer

    if analyst == "rules":
        return Deps(toolbox=tb, analyst=rules_analyst, writer=writer.rules_write, reviewer=None, meter=meter)
    return Deps(toolbox=tb, analyst=llm_analyst, writer=writer.write, reviewer=writer.review, meter=meter)


async def investigate(batch_id: int, analyst: str = "llm", offline: bool = False, thread: str | None = None) -> dict:
    """Run one investigation to the approval stop. Returns everything needed to score and audit it."""
    thread = thread or f"batch-{batch_id}-{uuid.uuid4().hex[:8]}"
    meter = CostMeter()
    t0 = time.time()
    async with checkpointer() as saver, toolbox(offline) as tb:
        graph = build(saver)
        cfg = {"configurable": {"thread_id": thread, "deps": deps_for(tb, analyst, meter)}}
        error = None
        try:
            state = await graph.ainvoke({"batch_id": batch_id}, cfg)
        except Exception as e:  # recorded, not hidden: a failed run counts against the gate
            error, state = f"{type(e).__name__}: {e}", {}
        try:
            await release(graph, thread)
            released_without_approval = True
        except Exception:
            released_without_approval = False
        result = {
            "thread": thread,
            "batch_id": batch_id,
            "status": state.get("status", "error"),
            "error": error,
            "seconds": round(time.time() - t0, 1),
            "analyst": analyst,
            "alarm": state.get("alarm"),
            "findings": state.get("findings"),
            "report": state.get("report"),
            "review": state.get("review"),
            "strip_log": state.get("strip_log"),
            "revisions": state.get("revisions", 0),
            "retrieved": state.get("retrieved", sorted(tb.retrieved)),
            "tool_calls": tb.calls,
            "cost": meter.summary(),
            "released_without_approval": released_without_approval,
        }
    (OUT / f"{thread}.json").write_text(json.dumps(result, indent=2, default=str))
    if result["report"]:
        (OUT / f"{thread}.md").write_text(to_markdown(Report(**result["report"]), result))
    return result


async def decide(thread: str, reviewer: str, decision: str, reason: str = "", edited: dict | None = None) -> dict:
    from langgraph.types import Command

    async with checkpointer() as saver, toolbox(offline=True) as tb:
        graph = build(saver)
        cfg = {"configurable": {"thread_id": thread, "deps": deps_for(tb, "rules", CostMeter())}}
        resume = {"reviewer": reviewer, "decision": decision, "reason": reason} | ({"report": edited} if edited else {})
        state = await graph.ainvoke(Command(resume=resume), cfg)
        out = {"thread": thread, "status": state["status"], "audit_signature": state.get("audit_signature")}
        if state["status"].startswith("approved"):
            out["released"] = await release(graph, thread)
        return out


def to_markdown(r: Report, meta: dict) -> str:
    def claims(cs):
        return "\n".join(f"- {c.text} `{'`, `'.join(c.citations)}`" for c in cs) or "- (none)"

    hyp = "\n".join(
        f"{h.rank}. **{h.statement}** ({h.category})\n{claims(h.evidence_for)}\n   - To confirm: {h.to_confirm}"
        for h in r.hypotheses
    )
    return f"""# DRAFT: {r.title}

> AI-drafted investigation for batch {r.batch_id}. Status: **{meta["status"]}**. Not a decision: a named QA reviewer
> must approve, edit or reject it. Simulated data (IndPenSim); fictional site.

## Event
{claims(r.event)}

## Evidence
{claims(r.evidence)}

## Root-cause hypotheses (most likely first)
{hyp or "- (none supported by evidence)"}

## Product impact
{claims(r.product_impact)}

## Proposed CAPA
{claims(r.capa)}

## Regulatory basis
{claims(r.regulatory_basis)}

## Similar past deviations
{claims(r.similar_cases)}

---
Cost EUR {meta["cost"]["eur"]}, {meta["seconds"]} s, {meta["revisions"]} revision(s).
"""


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("investigate")
    a.add_argument("--batch", type=int, required=True)
    a.add_argument("--analyst", choices=["llm", "rules"], default="llm")
    a.add_argument("--offline", action="store_true", help="in-process tools instead of MCP/A2A")
    d = sub.add_parser("decide")
    d.add_argument("--thread", required=True)
    d.add_argument("--reviewer", required=True)
    d.add_argument("--decision", choices=["approve", "edit", "reject"], required=True)
    d.add_argument("--reason", default="")
    s = sub.add_parser("show")
    s.add_argument("--thread", required=True)
    args = p.parse_args(argv)
    if args.cmd == "investigate":
        r = asyncio.run(investigate(args.batch, args.analyst, args.offline))
        hyps = [(h["rank"], h["category"]) for h in (r["report"] or {}).get("hypotheses", [])]
        print(f"thread {r['thread']}: {r['status']}  hypotheses {hyps}  EUR {r['cost']['eur']}  {r['seconds']} s")
        if r["error"]:
            print("ERROR", r["error"])
        print(f"report: {OUT / (r['thread'] + '.md')}")
        return 0 if not r["error"] else 1
    if args.cmd == "decide":
        print(json.dumps(asyncio.run(decide(args.thread, args.reviewer, args.decision, args.reason)), indent=1)[:2000])
        return 0
    print((OUT / f"{args.thread}.md").read_text())
    return 0


if __name__ == "__main__":
    sys.exit(main())
