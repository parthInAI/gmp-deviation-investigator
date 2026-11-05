"""QMS MCP server (read-only): SOPs, past deviations and their CAPAs from the synthetic quality system.

python -m mcp_servers.qms.server       # streamable HTTP on :8102/mcp
"""

from __future__ import annotations

import os

from mcp.server.fastmcp import FastMCP

from mcp_servers.qms import core

mcp = FastMCP(
    "gmpdi-qms",
    instructions=(
        "Read-only access to the (synthetic) quality system of fermenter FV-201: SOP sections and closed "
        "deviations with root cause and CAPA. Cite SOP sections as 'SOP-FER-001 §4.4' and deviations as "
        "'DEV-2024-007'. A similar past deviation is a lead to test against the sensor data, not an answer."
    ),
    host=os.environ.get("MCP_HOST", "0.0.0.0"),
    port=int(os.environ.get("MCP_PORT", "8102")),
)


@mcp.tool()
def search_sops(query: str, k: int = 5) -> list[dict]:
    """Find SOP sections relevant to a question; each hit has a citable 'cite' like 'SOP-FER-003 §4.3'."""
    return core.search_sops(query, k)


@mcp.tool()
def get_sop_section(cite: str) -> dict:
    """The full text of one SOP section, e.g. 'SOP-FER-001 §4.4'."""
    return core.get_sop_section(cite)


@mcp.tool()
def search_deviations(query: str, k: int = 5, category: str | None = None) -> list[dict]:
    """Find closed deviations with similar symptoms. Optional category filter (e.g. 'aeration_supply')."""
    return core.search_deviations(query, k, category)


@mcp.tool()
def get_deviation(dev_id: str) -> dict:
    """The full closed deviation record: symptoms, investigation, root cause, CAPA and effectiveness check."""
    return core.get_deviation(dev_id)


def main() -> None:
    mcp.run(transport=os.environ.get("MCP_TRANSPORT", "streamable-http"))


if __name__ == "__main__":
    main()
