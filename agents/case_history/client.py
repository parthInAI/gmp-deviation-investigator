"""The lead's side of the A2A hand-off to the case historian."""

from __future__ import annotations

import uuid

from agents.schemas import SimilarCase


async def ask_case_historian(url: str | None, summary: str, timeout_s: float = 180) -> tuple[list[SimilarCase], dict]:
    """Send the deviation summary over A2A; return the cases and the historian's token usage."""
    import asyncio

    from a2a.client.client_factory import create_client
    from a2a.helpers.proto_helpers import get_data_parts, new_text_part
    from a2a.types import a2a_pb2 as pb

    if not url:
        raise RuntimeError("CASE_HISTORIAN_A2A_URL is not set")
    client = await create_client(url)
    msg = pb.Message(role=pb.Role.ROLE_USER, message_id=str(uuid.uuid4()), parts=[new_text_part(summary)])

    async def call():
        async for resp in client.send_message(pb.SendMessageRequest(message=msg)):
            if resp.HasField("task") and resp.task.status.state == pb.TaskState.TASK_STATE_COMPLETED:
                for art in resp.task.artifacts:
                    if art.name == "similar_cases":
                        return get_data_parts(art.parts)[0]
            if resp.HasField("task") and resp.task.status.state == pb.TaskState.TASK_STATE_FAILED:
                raise RuntimeError("case historian task failed")
        raise RuntimeError("case historian returned no similar_cases artifact")

    try:
        data = await asyncio.wait_for(call(), timeout_s)
    finally:
        await client.close()
    return [SimilarCase(**c) for c in data["cases"]], data.get("usage", {})
