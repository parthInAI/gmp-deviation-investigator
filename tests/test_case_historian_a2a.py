"""A2A contract between the lead and the CrewAI case historian (search mode: no LLM, no keys)."""

import asyncio
import socket
import threading
import time

import pytest

pytest.importorskip("a2a")
pytest.importorskip("uvicorn")

import uvicorn  # noqa: E402

from agents.case_history import crew, server  # noqa: E402
from agents.case_history.client import ask_case_historian  # noqa: E402
from mcp_servers.qms import core as Q  # noqa: E402


def _local_qms(tool, args):
    return {
        "search_deviations": lambda a: Q.search_deviations(a["query"], a.get("k", 5)),
        "get_deviation": lambda a: Q.get_deviation(a["dev_id"]),
    }[tool](args)


@pytest.fixture
def historian_url(monkeypatch):
    monkeypatch.setattr(crew, "qms_call", _local_qms)
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    srv = uvicorn.Server(
        uvicorn.Config(server.app(mode="search", url=url), host="127.0.0.1", port=port, log_level="error")
    )
    t = threading.Thread(target=srv.run, daemon=True)
    t.start()
    for _ in range(50):
        if srv.started:
            break
        time.sleep(0.1)
    yield url
    srv.should_exit = True
    t.join(5)


def test_agent_card_advertises_the_skill():
    card = server.agent_card("http://x:1")
    assert [s.id for s in card.skills] == ["find_similar_cases"]
    assert card.supported_interfaces[0].protocol_version


def test_lead_gets_similar_cases_over_a2a(historian_url):
    cases, usage = asyncio.run(
        ask_case_historian(historian_url, "Dissolved oxygen fell sharply while the aeration rate stayed at recipe")
    )
    assert 1 <= len(cases) <= 3
    assert cases[0].category == "agitation"
    assert all(c.deviation_id.startswith("DEV-") for c in cases)
    assert usage == {"calls": []}
