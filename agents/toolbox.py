"""One interface to every evidence source the agents may use, and a record of what each run retrieved.

    MCPToolbox    production: historian and QMS through their MCP servers (HTTP), regulations from
                  pgvector, similar cases from the CrewAI case historian over A2A
    LocalToolbox  tests and offline runs: the same core functions called in-process

Both are read-only. Every non-sensor id a tool returns is added to `retrieved`, which is what the
citation checker accepts: an agent can only cite what it was actually shown.
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager

from agents.schemas import SimilarCase


class Toolbox:
    def __init__(self):
        self.retrieved: set[str] = set()
        self.calls: list[dict] = []  # audit trail of tool use for this run

    def _log(self, tool: str, args: dict, n: int | None = None):
        self.calls.append({"tool": tool, "args": args, "results": n})

    # historian -----------------------------------------------------------
    async def get_alarm(self, batch_id: int) -> dict: ...
    async def compare_to_normal(self, batch_id: int, start_h: float, end_h: float, signals=None) -> dict: ...
    async def get_trend(
        self, batch_id: int, signals: list[str], start_h: float, end_h: float, every: int = 5
    ) -> dict: ...
    async def check_sensor_citation(self, citation: str) -> bool: ...

    # QMS -------------------------------------------------------------------
    async def search_sops(self, query: str, k: int = 3) -> list[dict]: ...
    async def search_deviations(self, query: str, k: int = 5, category: str | None = None) -> list[dict]: ...
    async def get_deviation(self, dev_id: str) -> dict: ...

    # regulations and case history -------------------------------------------
    async def regulations(self, query: str, k: int = 3) -> list[dict]: ...
    async def similar_cases(self, summary: str) -> tuple[list[SimilarCase], dict]: ...

    async def _verified(self, cases: list[SimilarCase]) -> list[SimilarCase]:
        """Keep only cases whose deviation exists in the QMS (an LLM may invent an id), using the real root cause."""
        out = []
        for c in cases:
            try:
                d = await self.get_deviation(c.deviation_id)
            except Exception:
                self._log("similar_cases.dropped_unknown_id", {"id": c.deviation_id})
                continue
            out.append(c.model_copy(update={"root_cause": d["root_cause"], "category": d["category"]}))
        return out

    def _keep(self, hits: list[dict], key: str) -> list[dict]:
        for h in hits:
            self.retrieved.add(h[key])
        return hits


class LocalToolbox(Toolbox):
    """In-process sources. Similar cases come from QMS search (no CrewAI) unless a case function is given."""

    def __init__(self, cases=None, store: str = "memory"):
        super().__init__()
        from mcp_servers.historian import core as H
        from mcp_servers.qms import core as Q

        self.H, self.Q, self.store, self._cases = H, Q, store, cases

    async def get_alarm(self, batch_id):
        self._log("get_alarm", {"batch_id": batch_id})
        return self.H.get_alarm(batch_id)

    async def compare_to_normal(self, batch_id, start_h, end_h, signals=None):
        self._log("compare_to_normal", {"batch_id": batch_id, "start_h": start_h, "end_h": end_h})
        return self.H.compare_to_normal(batch_id, start_h, end_h, signals)

    async def get_trend(self, batch_id, signals, start_h, end_h, every=5):
        self._log("get_trend", {"batch_id": batch_id, "signals": signals, "start_h": start_h, "end_h": end_h})
        return self.H.get_trend(batch_id, signals, start_h, end_h, every)

    async def check_sensor_citation(self, citation):
        return self.H.check_sensor_citation(citation).valid

    async def search_sops(self, query, k=3):
        hits = self.Q.search_sops(query, k)
        self._log("search_sops", {"query": query}, len(hits))
        return self._keep(hits, "cite")

    async def search_deviations(self, query, k=5, category=None):
        hits = self.Q.search_deviations(query, k, category)
        self._log("search_deviations", {"query": query}, len(hits))
        return self._keep(hits, "id")

    async def get_deviation(self, dev_id):
        d = self.Q.get_deviation(dev_id)
        self.retrieved.add(d["id"])
        return d

    async def regulations(self, query, k=3):
        from agents.regulatory.retriever import retrieve

        hits = [h.as_dict() for h in retrieve(query, k=k, store=self.store)]
        self._log("regulations", {"query": query}, len(hits))
        return self._keep(hits, "cite")

    async def similar_cases(self, summary):
        if self._cases:
            cases, usage = await self._cases(summary)
            cases = await self._verified(cases)
        else:
            hits = await self.search_deviations(summary, k=3)
            cases = []
            for h in hits:
                d = await self.get_deviation(h["id"])
                cases.append(
                    SimilarCase(
                        deviation_id=d["id"],
                        why_similar="closest symptom match in the QMS",
                        root_cause=d["root_cause"],
                        category=d["category"],
                        capa_that_worked="; ".join(c["action"] for c in d["capa"]),
                    )
                )
            usage = {}
        self._log("similar_cases", {"summary": summary[:200]}, len(cases))
        return cases, usage


class MCPToolbox(Toolbox):
    """Production sources over the network. Open with `async with MCPToolbox.connect() as tb:`."""

    def __init__(self, historian, qms, a2a_url: str | None, store: str = "pgvector"):
        super().__init__()
        self.hist, self.qms, self.a2a_url, self.store = historian, qms, a2a_url, store

    @classmethod
    @asynccontextmanager
    async def connect(cls):
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client

        hurl = os.environ.get("HISTORIAN_MCP_URL", "http://historian:8101/mcp")
        qurl = os.environ.get("QMS_MCP_URL", "http://qms:8102/mcp")
        async with streamablehttp_client(hurl) as (hr, hw, _), streamablehttp_client(qurl) as (qr, qw, _):
            async with ClientSession(hr, hw) as hs, ClientSession(qr, qw) as qs:
                await asyncio.gather(hs.initialize(), qs.initialize())
                yield cls(
                    hs, qs, os.environ.get("CASE_HISTORIAN_A2A_URL"), os.environ.get("REGULATION_STORE", "pgvector")
                )

    @staticmethod
    async def _call(session, tool: str, args: dict):
        res = await session.call_tool(tool, args)
        if res.isError:
            raise RuntimeError(f"{tool} failed: {res.content[0].text if res.content else ''}")
        if res.structuredContent is not None:
            sc = res.structuredContent
            return sc.get("result", sc) if isinstance(sc, dict) and set(sc) == {"result"} else sc
        return json.loads(res.content[0].text)

    async def get_alarm(self, batch_id):
        self._log("get_alarm", {"batch_id": batch_id})
        return await self._call(self.hist, "get_alarm", {"batch_id": batch_id})

    async def compare_to_normal(self, batch_id, start_h, end_h, signals=None):
        self._log("compare_to_normal", {"batch_id": batch_id, "start_h": start_h, "end_h": end_h})
        args = {"batch_id": batch_id, "start_h": start_h, "end_h": end_h}
        if signals:
            args["signals"] = signals
        return await self._call(self.hist, "compare_to_normal", args)

    async def get_trend(self, batch_id, signals, start_h, end_h, every=5):
        self._log("get_trend", {"batch_id": batch_id, "signals": signals, "start_h": start_h, "end_h": end_h})
        args = {"batch_id": batch_id, "signals": signals, "start_h": start_h, "end_h": end_h, "every": every}
        return await self._call(self.hist, "get_trend", args)

    async def check_sensor_citation(self, citation):
        return bool((await self._call(self.hist, "check_sensor_citation", {"citation": citation}))["valid"])

    async def search_sops(self, query, k=3):
        hits = await self._call(self.qms, "search_sops", {"query": query, "k": k})
        self._log("search_sops", {"query": query}, len(hits))
        return self._keep(hits, "cite")

    async def search_deviations(self, query, k=5, category=None):
        args = {"query": query, "k": k} | ({"category": category} if category else {})
        hits = await self._call(self.qms, "search_deviations", args)
        self._log("search_deviations", {"query": query}, len(hits))
        return self._keep(hits, "id")

    async def get_deviation(self, dev_id):
        d = await self._call(self.qms, "get_deviation", {"dev_id": dev_id})
        self.retrieved.add(d["id"])
        return d

    async def regulations(self, query, k=3):
        from agents.regulatory.retriever import retrieve

        hits = [h.as_dict() for h in await asyncio.to_thread(retrieve, query, k, self.store)]
        self._log("regulations", {"query": query}, len(hits))
        return self._keep(hits, "cite")

    async def similar_cases(self, summary):
        from agents.case_history.client import ask_case_historian

        cases, usage = await ask_case_historian(self.a2a_url, summary)
        cases = await self._verified(cases)
        self._log("similar_cases", {"summary": summary[:200]}, len(cases))
        return cases, usage
