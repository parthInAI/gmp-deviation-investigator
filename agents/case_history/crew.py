"""Case historian, built with CrewAI: finds closed deviations similar to the current one and what fixed them.

Its only tools read the QMS MCP server (read-only). It runs in its own container (CrewAI pins its
own dependency versions) and is reached by the lead over A2A (agents/case_history/server.py).
"""

from __future__ import annotations

import asyncio
import json
import os

from pydantic import BaseModel

QMS_URL = os.environ.get("QMS_MCP_URL", "http://qms:8102/mcp")


class Case(BaseModel):
    deviation_id: str
    why_similar: str
    root_cause: str
    category: str
    capa_that_worked: str


class Cases(BaseModel):
    cases: list[Case]


def qms_call(tool: str, args: dict):
    """Call one QMS MCP tool synchronously (CrewAI runs tools synchronously)."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def go():
        async with streamablehttp_client(QMS_URL) as (r, w, _), ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool(tool, args)
            if res.isError:
                raise RuntimeError(res.content[0].text if res.content else tool)
            sc = res.structuredContent
            if sc is not None:
                return sc.get("result", sc) if isinstance(sc, dict) and set(sc) == {"result"} else sc
            return json.loads(res.content[0].text)

    return asyncio.run(go())


def build_crew():
    from crewai import LLM, Agent, Crew, Task
    from crewai.tools import tool

    @tool("search_past_deviations")
    def search_past_deviations(query: str) -> str:
        """Search closed deviations by symptoms; returns id, title, category, symptoms and root cause."""
        return json.dumps(qms_call("search_deviations", {"query": query, "k": 6}))

    @tool("read_deviation")
    def read_deviation(dev_id: str) -> str:
        """Read one closed deviation in full: symptoms, investigation, root cause, CAPA, effectiveness."""
        return json.dumps(qms_call("get_deviation", {"dev_id": dev_id}))

    llm = LLM(model=f"anthropic/{os.environ.get('GMPDI_CASE_MODEL', 'claude-haiku-5-5')}", temperature=0.0)
    historian = Agent(
        role="Case historian",
        goal="Find the closed deviations whose symptoms best match the current batch, and what CAPA worked.",
        backstory=(
            "You keep the site's deviation history. You know that similar symptoms can have different causes "
            "(a falling DO can be aeration, agitation or contamination), so you match on the pattern of signals "
            "and timing, and you say how each case is similar."
        ),
        tools=[search_past_deviations, read_deviation],
        llm=llm,
        allow_delegation=False,
        max_iter=6,
        verbose=False,
    )
    task = Task(
        description=(
            "Current deviation: {summary}\n\n"
            "Search the past deviations with two or three different queries built from the symptoms. Read the most "
            "promising ones. Return at most 3 cases, most similar first. Only return deviation ids that the tools "
            "returned. If nothing is genuinely similar, return fewer."
        ),
        expected_output="Up to 3 similar closed deviations with why each is similar, its root cause and its CAPA.",
        agent=historian,
        output_pydantic=Cases,
    )
    return Crew(agents=[historian], tasks=[task], verbose=False)


def find_similar(summary: str) -> tuple[list[Case], dict]:
    """Run the crew. Returns the cases and the token usage (for the lead's cost meter)."""
    out = build_crew().kickoff(inputs={"summary": summary})
    cases = out.pydantic.cases if out.pydantic else Cases.model_validate_json(out.raw).cases
    u = out.token_usage
    model = os.environ.get("GMPDI_CASE_MODEL", "claude-haiku-5-5")
    usage = {"calls": [{"model": model, "input_tokens": u.prompt_tokens, "output_tokens": u.completion_tokens}]}
    return cases, usage


def search_only(summary: str) -> tuple[list[Case], dict]:
    """No-LLM mode (CI, offline): BM25 over the QMS, top 3."""
    hits = qms_call("search_deviations", {"query": summary, "k": 3})
    cases = []
    for h in hits:
        d = qms_call("get_deviation", {"dev_id": h["id"]})
        cases.append(
            Case(
                deviation_id=d["id"],
                why_similar="closest symptom match in the QMS (search only, no LLM)",
                root_cause=d["root_cause"],
                category=d["category"],
                capa_that_worked="; ".join(c["action"] for c in d["capa"]),
            )
        )
    return cases, {"calls": []}
