"""Process data analyst: turns the alarm into a cited timeline of what moved, when, and which equipment it points at.

    llm_analyst     LangGraph ReAct sub-agent (Claude Haiku) that drives the historian tools itself
    rules_analyst   deterministic baseline, no LLM: alarm contributors + comparison with normal batches

Both return ProcessFindings whose timeline cites SENSOR windows; the reviewer later checks each one.
"""

from __future__ import annotations

from langchain_core.tools import StructuredTool

from agents import config
from agents.schemas import Claim, ProcessFindings
from agents.toolbox import Toolbox

# Manipulated inputs and control loops first: a fault starts in equipment; off-gas, uptake and DO follow.
SUBSYSTEM = {
    "aeration_rate_lph": "air supply (aeration)",
    "sugar_feed_rate_lph": "sugar feed system",
    "ph": "pH measurement and control",
    "base_flow_rate_lph": "pH measurement and control",
    "acid_flow_rate_lph": "pH measurement and control",
    "temperature_k": "temperature control and cooling",
    "cooling_water_flow_lph": "temperature control and cooling",
    "heating_water_flow_lph": "temperature control and cooling",
    "air_head_pressure_bar": "vessel pressure",
    "paa_flow_lph": "PAA precursor feed",
    "oil_flow_lph": "oil feed",
}
CONSEQUENCES = {
    "offgas_co2_pct",
    "offgas_o2_pct",
    "oxygen_uptake_rate",
    "carbon_evolution_rate",
    "dissolved_oxygen_mgpl",
    "generated_heat_kj",
    "vessel_volume_l",
    "vessel_weight_kg",
}


def _cite(batch_id: int, signal: str, a: float, b: float) -> str:
    return f"SENSOR:{batch_id}:{signal}:{max(a, 0):.1f}-{b:.1f}"


async def rules_analyst(tb: Toolbox, batch_id: int, callbacks=None) -> ProcessFindings:
    alarm = await tb.get_alarm(batch_id)
    if not alarm.get("alarm"):
        return ProcessFindings(first_alarm_h=None, timeline=[], suspected_subsystems=[], summary="No alarm.")
    t = alarm["first_alarm_h"]
    cmp = await tb.compare_to_normal(batch_id, t - 4, t + 2)
    near = {r["signal"]: r for r in cmp["left_band_in_order"] if r["first_left_normal_band_h"] >= t - 4}
    top = [s["signal"] for s in alarm["top_contributing_signals"]]
    timeline = [
        Claim(
            text=f"The fault detector alarmed at {t} h; the signals driving it were {', '.join(top[:3])}.",
            citations=[_cite(batch_id, s, t - 2, t) for s in top[:3] if s in near],
        )
    ]
    for s, r in sorted(near.items(), key=lambda kv: kv[1]["first_left_normal_band_h"]):
        if s not in top and r["max_abs_z"] < 5:
            continue
        t0 = r["first_left_normal_band_h"]
        timeline.append(
            Claim(
                text=f"{s} went {r['direction']} its normal band at {t0} h "
                f"(value {r.get('value_then')} vs normal {r.get('normal_mean_then')}, max |z| {r['max_abs_z']}).",
                citations=[_cite(batch_id, s, t0, min(t0 + 2, t + 2))],
            )
        )
    timeline = [c for c in timeline if c.citations]
    ranked = [s for s in top if s in SUBSYSTEM] + [s for s in near if s in SUBSYSTEM and s not in top]
    subsystems = list(dict.fromkeys(SUBSYSTEM[s] for s in ranked))
    effects = [s for s in top if s in CONSEQUENCES]
    summary = (
        f"Alarm at {t} h. Equipment-side signals involved: {', '.join(subsystems) or 'none clear'}. "
        f"Process responses: {', '.join(effects) or 'none'}."
    )
    return ProcessFindings(first_alarm_h=t, timeline=timeline, suspected_subsystems=subsystems, summary=summary)


ANALYST_PROMPT = """You are the process data analyst in a GMP deviation investigation of fermenter FV-201
(simulated penicillin fermentation). You only read data; you never change anything.

Work like this:
1. get_alarm for the batch: first alarm time and the signals that drove it.
2. compare_to_normal from about 4 h before the first alarm to 2 h after it. Signals listed in
   left_band_in_order left their normal band; note when and how far (max |z|).
3. Use get_trend on the few signals that matter to see the shape (step, drift, oscillation).
4. Separate CAUSE from EFFECT. Equipment and control signals (aeration, sugar feed, acid/base flow, pH,
   temperature, cooling/heating water, head pressure, PAA, oil) are where faults start. Off-gas CO2/O2,
   oxygen uptake, CO2 evolution, dissolved oxygen and generated heat are the culture's response.
   A feed signal can sit outside its band for hours because of a recipe difference: weigh how far a
   signal moved and how close to the alarm it moved, not only which moved first.
5. Before you finish, check each sensor citation with check_sensor_citation and drop any that fail.

Return ProcessFindings. Every timeline claim must cite at least one SENSOR:<batch>:<signal>:<start_h>-<end_h>
window in which that signal really is outside its normal band. List suspected_subsystems most likely first.
"""


def _tools(tb: Toolbox) -> list:
    async def get_alarm(batch_id: int) -> dict:
        """The fault detector's alarm periods, first alarm time and the signals that drove it."""
        return await tb.get_alarm(batch_id)

    async def compare_to_normal(batch_id: int, start_h: float, end_h: float) -> dict:
        """Which signals left their normal band in [start_h, end_h], in order, and how far."""
        return await tb.compare_to_normal(batch_id, start_h, end_h)

    async def get_trend(batch_id: int, signals: list[str], start_h: float, end_h: float) -> dict:
        """Hourly raw values of the given signals between start_h and end_h."""
        return await tb.get_trend(batch_id, signals, start_h, end_h)

    async def check_sensor_citation(citation: str) -> bool:
        """True only if SENSOR:<batch>:<signal>:<start_h>-<end_h> shows that signal outside its normal band."""
        return await tb.check_sensor_citation(citation)

    return [
        StructuredTool.from_function(coroutine=f)
        for f in (get_alarm, compare_to_normal, get_trend, check_sensor_citation)
    ]


async def llm_analyst(tb: Toolbox, batch_id: int, callbacks=None) -> ProcessFindings:
    from langgraph.prebuilt import create_react_agent

    from agents.llm import chat

    agent = create_react_agent(chat("analyst"), _tools(tb), prompt=ANALYST_PROMPT, response_format=ProcessFindings)
    out = await agent.ainvoke(
        {
            "messages": [
                ("user", f"Investigate batch {batch_id}. Find what moved, when, and which equipment it points at.")
            ]
        },
        config={"callbacks": callbacks or [], "recursion_limit": 2 * config.MAX_ANALYST_STEPS + 2},
    )
    return out["structured_response"]
