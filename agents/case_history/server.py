"""A2A server for the CrewAI case historian.

    python -m agents.case_history.server        # http://0.0.0.0:8103, agent card at /.well-known/agent-card.json
    CASE_HISTORIAN_MODE=crew | search            # crew = CrewAI + Claude (default); search = no LLM (CI)

Skill "find_similar_cases": send a text message describing the deviation; the completed task carries
one data artifact {"cases": [...], "usage": {"calls": [...]}}.
"""

from __future__ import annotations

import asyncio
import os

from a2a.helpers.proto_helpers import get_message_text, new_data_part, new_task
from a2a.server.agent_execution import AgentExecutor
from a2a.server.request_handlers.default_request_handler_v2 import DefaultRequestHandlerV2
from a2a.server.routes.agent_card_routes import create_agent_card_routes
from a2a.server.routes.jsonrpc_routes import create_jsonrpc_routes
from a2a.server.tasks.inmemory_task_store import InMemoryTaskStore
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types import a2a_pb2 as pb
from starlette.applications import Starlette

from agents.case_history import crew

PUBLIC_URL = os.environ.get("CASE_HISTORIAN_PUBLIC_URL", "http://case-historian:8103")


def agent_card(url: str = PUBLIC_URL) -> pb.AgentCard:
    return pb.AgentCard(
        name="gmpdi-case-historian",
        description="Finds closed deviations similar to a new one, with their root cause and the CAPA that worked. "
        "Read-only; built with CrewAI over the QMS MCP server.",
        version="1.0.0",
        supported_interfaces=[pb.AgentInterface(protocol_binding="JSONRPC", protocol_version="1.0", url=url + "/")],
        capabilities=pb.AgentCapabilities(streaming=False),
        default_input_modes=["text/plain"],
        default_output_modes=["application/json"],
        skills=[
            pb.AgentSkill(
                id="find_similar_cases",
                name="Find similar past deviations",
                description="Given a description of a new deviation, return up to 3 similar closed deviations.",
                tags=["gmp", "deviation", "capa", "case-history"],
            )
        ],
    )


class CaseHistorianExecutor(AgentExecutor):
    def __init__(self, mode: str | None = None):
        self.mode = mode or os.environ.get("CASE_HISTORIAN_MODE", "crew")

    async def execute(self, context, event_queue):
        if context.current_task is None:
            await event_queue.enqueue_event(
                new_task(
                    context.task_id, context.context_id, pb.TaskState.TASK_STATE_SUBMITTED, history=[context.message]
                )
            )
        up = TaskUpdater(event_queue, context.task_id, context.context_id)
        await up.start_work()
        summary = get_message_text(context.message)
        fn = crew.find_similar if self.mode == "crew" else crew.search_only
        cases, usage = await asyncio.to_thread(fn, summary)
        await up.add_artifact(
            [new_data_part({"cases": [c.model_dump() for c in cases], "usage": usage, "mode": self.mode})],
            name="similar_cases",
        )
        await up.complete()

    async def cancel(self, context, event_queue):
        await TaskUpdater(event_queue, context.task_id, context.context_id).cancel()


def app(mode: str | None = None, url: str = PUBLIC_URL) -> Starlette:
    card = agent_card(url)
    handler = DefaultRequestHandlerV2(
        agent_executor=CaseHistorianExecutor(mode), task_store=InMemoryTaskStore(), agent_card=card
    )
    return Starlette(routes=create_agent_card_routes(card) + create_jsonrpc_routes(handler, rpc_url="/"))


def main() -> None:
    import uvicorn

    uvicorn.run(app(), host="0.0.0.0", port=int(os.environ.get("A2A_PORT", "8103")))


if __name__ == "__main__":
    main()
