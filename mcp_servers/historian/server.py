"""Historian MCP server (read-only): batch trends, the detector's alarm, comparison with normal batches.

    python -m mcp_servers.historian.server                 # streamable HTTP on :8101/mcp
    HISTORIAN_SOURCE=<silver Delta dir | IndPenSim CSV>    # default /lakehouse/silver/process_sensor

Databricks: HISTORIAN_SOURCE=databricks:workspace.gmpdi.silver_process_sensor with DATABRICKS_HOST,
DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN set (needs databricks-sql-connector).
"""

from __future__ import annotations

import os

from mcp.server.fastmcp import FastMCP

from mcp_servers.historian import core

mcp = FastMCP(
    "gmpdi-historian",
    instructions=(
        "Read-only process historian for fermenter FV-201 (simulated IndPenSim data). "
        "Start with get_alarm, then compare_to_normal over a window from a few hours before the first alarm "
        "to a few hours after it, then get_trend for the signals that matter. Cite sensor evidence as "
        "SENSOR:<batch>:<signal>:<start_h>-<end_h>; a citation is only valid if that signal is outside its "
        "normal band in that window."
    ),
    host=os.environ.get("MCP_HOST", "0.0.0.0"),
    port=int(os.environ.get("MCP_PORT", "8101")),
)


@mcp.tool()
def list_signals() -> list[dict]:
    """The 19 online signals available, with units and meaning."""
    return core.list_signals()


@mcp.tool()
def batch_overview(batch_id: int) -> dict:
    """Length of the batch and how far the normal reference reaches (later readings are not judged)."""
    return core.batch_overview(batch_id)


@mcp.tool()
def get_alarm(batch_id: int) -> dict:
    """The frozen fault detector's alarm periods for a batch, the first alarm time, the signals that drove it
    (share of the score in the 2 h before), and how often scores at that level were real faults in testing.
    An alarm says the batch left its normal pattern; it does not say why."""
    return core.get_alarm(batch_id)


@mcp.tool()
def compare_to_normal(batch_id: int, start_h: float, end_h: float, signals: list[str] | None = None) -> dict:
    """Compare each signal with the normal batches at the same process time, over [start_h, end_h].
    Returns the signals that left their normal band, in the order they left it, with how far (max |z|).
    Recipe differences can push a feed signal outside the band for hours before any fault: weigh how far a
    signal moved and how close to the alarm it moved, not only which moved first."""
    return core.compare_to_normal(batch_id, start_h, end_h, signals)


@mcp.tool()
def get_trend(batch_id: int, signals: list[str], start_h: float, end_h: float, every: int = 5) -> dict:
    """Raw values of the given signals between start_h and end_h, every `every`-th reading (5 = hourly)."""
    return core.get_trend(batch_id, signals, start_h, end_h, every)


@mcp.tool()
def check_sensor_citation(citation: str) -> dict:
    """Check a sensor citation SENSOR:<batch>:<signal>:<start_h>-<end_h>: valid only if the signal is outside
    its normal band somewhere in that window."""
    c = core.check_sensor_citation(citation)
    return {"citation": citation, "valid": c.valid, "reason": c.reason}


def main() -> None:
    mcp.run(transport=os.environ.get("MCP_TRANSPORT", "streamable-http"))


if __name__ == "__main__":
    main()
