"""Report writer (Claude, structured output) and reviewer (Mistral, LLM-as-judge).

The writer sees an evidence pack in which every item carries the exact id it may be cited by,
and nothing else. The reviewer is a different vendor's model: it judges whether each claim's
text is supported by the evidence it cites and whether the ranking is sound; the mechanical
checks (does the citation exist, does the sensor window show an excursion) are done by code
before it sees the draft (agents/citations.py).
"""

from __future__ import annotations

import json

from agents.schemas import TAXONOMY, ProcessFindings, Report, Review

WRITER_PROMPT = f"""You draft GMP deviation investigation reports for fermenter FV-201 (simulated penicillin
fermentation, fictional site). Your draft goes to a named QA reviewer, who decides. You do not decide.

Rules:
1. Use only the evidence pack. Every claim must cite at least one id copied exactly from it:
   SENSOR:<batch>:<signal>:<start_h>-<end_h> windows, regulation ids (e.g. EU-GMP-Ch1:1.4),
   SOP sections (e.g. SOP-FER-003 §4.3) or deviations (e.g. DEV-2024-007). You may cite a narrower
   SENSOR window than the pack shows if that signal is outside its band there. Never invent an id.
2. Hypotheses: rank candidate root causes, most likely first, at most 5. Tag each with exactly one
   category from: {", ".join(TAXONOMY)}.
   Rank by the sensor evidence: which equipment or control signal moved first and furthest near the
   alarm, and whether the culture's response (off-gas, uptake, DO) is consistent with it. A similar
   past deviation supports a hypothesis only if its symptoms match this batch's data.
   Distinguish a measurement fault (the reading is wrong) from a process fault (the process changed);
   say what would tell them apart in to_confirm.
3. Draft Annex 22 ids (EU-GMP-Annex22-draft:...) are a draft guideline: never call them a requirement.
4. Plain, short sentences. No speculation beyond the evidence; say what is unknown.
"""

REVIEWER_PROMPT = """You review a draft GMP deviation investigation written by another AI model. You did not
write it. A QA professional will read your verdict.

For each claim, compare its text with the evidence it cites (in the evidence pack). Report in
verdicts ONLY the claims whose text says more than, or something different from, their cited
evidence. Then judge whether the hypothesis ranking follows from the sensor evidence (the equipment
or control signal that moved first and furthest near the alarm should lead). Score the draft 1-5.
Set send_back only if the ranking is wrong or more than a quarter of the claims are unsupported,
and say exactly what to fix.
"""


def evidence_pack(
    batch_id: int, alarm: dict, findings: ProcessFindings, regs: list[dict], sops: list[dict], cases
) -> str:
    pack = {
        "batch_id": batch_id,
        "alarm": {
            k: alarm.get(k) for k in ("first_alarm_h", "top_contributing_signals", "confidence", "alarm_periods")
        },
        "process_findings": findings.model_dump(),
        "regulations": [{"id": r["cite"], "status": r["status"], "text": r["text"][:700]} for r in regs],
        "sops": [{"id": s["cite"], "title": s["title"], "text": s["text"]} for s in sops],
        "similar_deviations": [c.model_dump() for c in cases],
    }
    return json.dumps(pack, indent=1, ensure_ascii=False)


async def write(pack: str, feedback: str | None, callbacks) -> Report:
    from agents.llm import chat

    msgs = [("system", WRITER_PROMPT), ("user", f"Evidence pack:\n{pack}")]
    if feedback:
        msgs.append(("user", f"The reviewer sent your previous draft back. Fix this and redraft:\n{feedback}"))
    llm = chat("writer", max_tokens=6000).with_structured_output(Report)
    return await llm.ainvoke(msgs, config={"callbacks": callbacks})


async def review(pack: str, draft: Report, callbacks) -> Review:
    from agents.llm import chat

    llm = chat("reviewer", max_tokens=3000).with_structured_output(Review)
    msgs = [
        ("system", REVIEWER_PROMPT),
        ("user", f"Evidence pack:\n{pack}\n\nDraft report:\n{draft.model_dump_json(indent=1)}"),
    ]
    return await llm.ainvoke(msgs, config={"callbacks": callbacks})
