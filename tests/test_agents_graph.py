"""The investigation graph end to end, with scripted models: citations are enforced, the reviewer can send a
draft back once, the run stops for a human, and nothing is released without a named reviewer's decision."""

import asyncio
import dataclasses
import uuid

import pandas as pd
import pytest

pytest.importorskip("langgraph")
pytest.importorskip("llama_index.retrievers.bm25")
pytest.importorskip("fastembed")

from langgraph.checkpoint.memory import InMemorySaver  # noqa: E402
from langgraph.types import Command  # noqa: E402

from agents import audit  # noqa: E402
from agents.lead.graph import Deps, NotApproved, build, release  # noqa: E402
from agents.llm import CostLimitExceeded, CostMeter  # noqa: E402
from agents.process_analyst import rules_analyst  # noqa: E402
from agents.schemas import Claim, Hypothesis, Report, Review  # noqa: E402
from agents.toolbox import LocalToolbox  # noqa: E402
from mcp_servers.historian import core as H  # noqa: E402
from tests.test_mcp_servers import normal_batch  # noqa: E402


@pytest.fixture
def plant(monkeypatch, tmp_path):
    """Batch 1 normal; batch 2 with an aeration fault 40-50 h (as in test_mcp_servers)."""
    det = dataclasses.replace(H.detector(), smooth_window=1)
    monkeypatch.setattr(H, "detector", lambda: det)
    prof, feats = det.profile, list(det.profile.features)
    b2 = normal_batch(2)
    win = b2.time_h.between(40, 50)
    for sig, sigmas in {"aeration_rate_lph": -8, "offgas_o2_pct": -6, "offgas_co2_pct": 6}.items():
        b2.loc[win, sig] += sigmas * prof.std[b2.step[win], feats.index(sig)]
    data = pd.concat([normal_batch(1), b2], ignore_index=True)
    monkeypatch.setattr(H, "load_readings", lambda src: data.copy())
    monkeypatch.setenv("GMPDI_AUDIT_KEY", "test-key")
    monkeypatch.setattr(audit, "AUDIT_FILE", tmp_path / "audit.jsonl")
    H.readings.cache_clear()
    yield
    H.readings.cache_clear()


def draft(pack_ids: set[str]) -> Report:
    reg = next(i for i in pack_ids if i.startswith("EU-GMP-Ch1"))
    return Report(
        batch_id=2,
        title="Loss of aeration, batch 2",
        event=[Claim(text="Alarm at about 41 h driven by aeration.", citations=["SENSOR:2:aeration_rate_lph:40-44"])],
        evidence=[
            Claim(text="Aeration fell below its normal band at 40 h.", citations=["SENSOR:2:aeration_rate_lph:40-45"]),
            Claim(text="pH moved too.", citations=["SENSOR:2:ph:40-45"]),  # not true: must be stripped
            Claim(text="Off-gas O2 fell.", citations=["SENSOR:2:offgas_o2_pct:40-45", "EU-GMP-Ch9:9.9"]),  # 1 bad cite
        ],
        hypotheses=[
            Hypothesis(
                rank=1,
                category="aeration_supply",
                statement="Air supply loss",
                evidence_for=[Claim(text="Aeration dropped first.", citations=["SENSOR:2:aeration_rate_lph:40-42"])],
                to_confirm="Compare FIC-201 with FI-202.",
            ),
            Hypothesis(
                rank=2,
                category="contamination",
                statement="Contamination",
                evidence_for=[Claim(text="Made-up support.", citations=["DEV-2099-001"])],  # never retrieved
                to_confirm="Microbiology.",
            ),
            Hypothesis(
                rank=3,
                category="agitation",
                statement="Agitator trip",
                evidence_for=[Claim(text="Off-gas O2 moved.", citations=["SENSOR:2:offgas_o2_pct:40-45"])],
                to_confirm="Drive alarm log.",
            ),
        ],
        product_impact=[Claim(text="Impact to be assessed.", citations=[])],  # uncited: stripped
        capa=[],
        regulatory_basis=[Claim(text="Significant deviations must be investigated.", citations=[reg])],
        similar_cases=[],
    )


class Script:
    def __init__(self, send_back: bool = False, unsupported: tuple[str, ...] = ()):
        self.writes, self.reviews, self.send_back, self.unsupported = 0, 0, send_back, unsupported
        self.tb = None

    async def writer(self, pack, feedback, callbacks):
        self.writes += 1
        return draft(self.tb.retrieved)

    async def reviewer(self, pack, d, callbacks):
        self.reviews += 1
        from agents.schemas import ClaimVerdict

        return Review(
            score=3,
            verdicts=[ClaimVerdict(claim_ref=r, supported=False, reason="x") for r in self.unsupported],
            ranking_sound=True,
            send_back=self.send_back,
            feedback="tighten",
        )


def run(batch_id, script: Script, thread=None):
    tb = LocalToolbox()
    script.tb = tb
    deps = Deps(toolbox=tb, analyst=rules_analyst, writer=script.writer, reviewer=script.reviewer)
    graph = build(InMemorySaver())
    cfg = {"configurable": {"thread_id": thread or str(uuid.uuid4()), "deps": deps}}
    state = asyncio.run(graph.ainvoke({"batch_id": batch_id}, cfg))
    return graph, cfg, state


def test_normal_batch_ends_without_investigation(plant):
    _, _, state = run(1, Script())
    assert state["status"] == "no_alarm" and "report" not in state


def test_invalid_citations_are_stripped_and_unsupported_hypotheses_dropped(plant):
    _, _, state = run(2, Script())
    rep = Report(**state["report"])
    texts = [c.text for c in rep.evidence]
    assert "pH moved too." not in texts
    o2 = next(c for c in rep.evidence if c.text == "Off-gas O2 fell.")
    assert o2.citations == ["SENSOR:2:offgas_o2_pct:40-45"]
    assert [h.category for h in rep.hypotheses] == ["aeration_supply", "agitation"]
    assert [h.rank for h in rep.hypotheses] == [1, 2]
    assert rep.product_impact == []
    assert state["strip_log"][-1]["hypotheses_dropped"] == ["Contamination"]


def test_reviewer_removes_flagged_claims_and_sends_back_only_once(plant):
    s = Script(send_back=True, unsupported=("hypotheses[1].evidence_for[0]",))
    _, _, state = run(2, s)
    assert s.writes == 2 and s.reviews == 2  # one revision, then on to approval
    assert [h.category for h in Report(**state["report"]).hypotheses] == ["aeration_supply"]


def test_run_stops_for_a_human_and_release_is_refused_until_approved(plant):
    graph, cfg, state = run(2, Script())
    assert state["status"] == "awaiting_approval"
    assert "__interrupt__" in state
    with pytest.raises(NotApproved):
        asyncio.run(release(graph, cfg["configurable"]["thread_id"]))
    # no name: refused
    with pytest.raises(ValueError):
        asyncio.run(graph.ainvoke(Command(resume={"decision": "approve", "reviewer": ""}), cfg))


def test_named_approval_is_signed_and_releases(plant):
    graph, cfg, _ = run(2, Script())
    state = asyncio.run(graph.ainvoke(Command(resume={"decision": "approve", "reviewer": "P. Pandya (QA)"}), cfg))
    assert state["status"] == "approved"
    out = asyncio.run(release(graph, cfg["configurable"]["thread_id"]))
    assert out["approved_by"] == "P. Pandya (QA)"
    ok, why = audit.verify()
    assert ok, why


def test_rejection_needs_a_reason_and_is_not_released(plant):
    graph, cfg, _ = run(2, Script())
    with pytest.raises(ValueError):
        asyncio.run(graph.ainvoke(Command(resume={"decision": "reject", "reviewer": "QA"}), cfg))
    graph, cfg, _ = run(2, Script())
    state = asyncio.run(graph.ainvoke(Command(resume={"decision": "reject", "reviewer": "QA", "reason": "weak"}), cfg))
    assert state["status"] == "rejected"
    with pytest.raises(NotApproved):
        asyncio.run(release(graph, cfg["configurable"]["thread_id"]))


def test_tampering_with_the_audit_trail_is_detected(plant):
    graph, cfg, _ = run(2, Script())
    asyncio.run(graph.ainvoke(Command(resume={"decision": "approve", "reviewer": "QA"}), cfg))
    text = audit.AUDIT_FILE.read_text().replace('"approve"', '"reject"')
    audit.AUDIT_FILE.write_text(text)
    assert not audit.verify()[0]


def test_cost_meter_stops_an_expensive_run():
    m = CostMeter(usd_to_eur=1.0, limit_eur=0.5)
    m.add("claude-sonnet-5-5", 100_000, 10_000)  # $0.30
    with pytest.raises(CostLimitExceeded):
        m.add("claude-sonnet-5-5", 100_000, 10_000)
