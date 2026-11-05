"""Historian and QMS MCP servers: correct answers on a constructed batch, no labels exposed, read-only tools."""

import asyncio
import dataclasses

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("mcp")

from mcp.shared.memory import create_connected_server_and_client_session  # noqa: E402

from mcp_servers.historian import core as H  # noqa: E402
from mcp_servers.qms import core as Q  # noqa: E402

STEP_H = 0.2


def normal_batch(batch_id: int, steps: int = 600) -> pd.DataFrame:
    """A batch that sits exactly on the normal profile: every z-score is ~0."""
    prof = H.detector().profile
    s = np.arange(steps)
    df = pd.DataFrame(prof.mean[np.minimum(s, prof.last_reliable)], columns=list(prof.features))
    df.insert(0, "time_h", s * STEP_H)
    df.insert(0, "seq", s + 1)
    df.insert(0, "batch_id", batch_id)
    df["step"] = s
    df["fault_ref"] = 0  # present in the source on purpose: the historian must drop it
    return df


@pytest.fixture
def historian(monkeypatch):
    """Batch 1 sits on the normal profile. Batch 2 has an aeration fault from 40 h to 50 h as a real one looks:
    aeration 8 sigma low, off-gas O2 down and CO2 up 6 sigma (and a fault label the agents must not see).
    Smoothing is switched off so a batch built on the profile has z = 0 exactly."""
    det = dataclasses.replace(H.detector(), smooth_window=1)
    monkeypatch.setattr(H, "detector", lambda: det)
    prof = det.profile
    feats = list(prof.features)
    b2 = normal_batch(2)
    win = b2.time_h.between(40, 50)
    for sig, sigmas in {"aeration_rate_lph": -8, "offgas_o2_pct": -6, "offgas_co2_pct": 6}.items():
        b2.loc[win, sig] += sigmas * prof.std[b2.step[win], feats.index(sig)]
    b2.loc[win, "fault_ref"] = 1
    data = pd.concat([normal_batch(1), b2], ignore_index=True)
    monkeypatch.setattr(H, "load_readings", lambda src: data.copy())
    H.readings.cache_clear()
    yield
    H.readings.cache_clear()


def test_labels_never_leave_the_historian(historian):
    assert not set(H.LABEL_COLUMNS) & set(H.readings().columns)
    text = repr([H.get_alarm(2), H.compare_to_normal(2, 0, 100), H.get_trend(2, ["ph"], 0, 10)])
    assert "fault_ref" not in text and "fault_flag" not in text


def test_normal_batch_has_no_alarm_and_nothing_leaves_its_band(historian):
    assert H.get_alarm(1)["alarm"] is False
    assert H.compare_to_normal(1, 0, 100)["left_band_in_order"] == []


def test_injected_aeration_drop_is_found_first_and_explained(historian):
    a = H.get_alarm(2)
    assert a["alarm"] and 40 <= a["first_alarm_h"] <= 44
    assert a["top_contributing_signals"][0]["signal"] == "aeration_rate_lph"
    c = H.compare_to_normal(2, 30, 60)
    first = c["left_band_in_order"][0]
    assert first["signal"] == "aeration_rate_lph" and first["direction"] == "below"
    assert 40 <= first["first_left_normal_band_h"] <= 41


def test_sensor_citations_must_point_at_a_real_excursion(historian):
    ok = H.check_sensor_citation("SENSOR:2:aeration_rate_lph:40-50")
    assert ok.valid, ok.reason
    for bad in [
        "SENSOR:2:aeration_rate_lph:60-70",  # signal normal there
        "SENSOR:2:ph:40-50",  # wrong signal
        "SENSOR:9:aeration_rate_lph:40-50",  # no such batch
        "SENSOR:2:aeration_rate_lph:50-40",  # backwards window
        "SENSOR:2:not_a_signal:40-50",
        "aeration was low around 45 h",  # not a citation at all
    ]:
        assert not H.check_sensor_citation(bad).valid, bad


def test_unknown_signal_is_refused(historian):
    with pytest.raises(ValueError):
        H.get_trend(2, ["fault_ref"], 0, 10)


def test_qms_search_finds_the_right_kind_of_record():
    assert Q.search_sops("pH probe drift offline sample comparison")[0]["cite"].startswith("SOP-FER-003")
    hits = Q.search_deviations("dissolved oxygen fell while aeration rate stayed at recipe agitator")
    assert hits[0]["category"] == "agitation"
    assert all(h["category"] == "contamination" for h in Q.search_deviations("rising uptake", category="contamination"))


def test_qms_citations_resolve():
    assert Q.citation_exists("SOP-FER-001 §4.4")
    assert Q.citation_exists("DEV-2024-007")
    assert not Q.citation_exists("SOP-FER-001 §9.9")
    assert not Q.citation_exists("DEV-2099-001")
    assert Q.get_sop_section("SOP-QA-013 §4.5")["text"].startswith("Investigation drafts")


@pytest.mark.parametrize("module", ["mcp_servers.historian.server", "mcp_servers.qms.server"])
def test_servers_expose_only_read_tools(module):
    import importlib

    server = importlib.import_module(module).mcp

    async def names():
        async with create_connected_server_and_client_session(server._mcp_server) as client:
            return [t.name for t in (await client.list_tools()).tools]

    tools = asyncio.run(names())
    assert tools
    write_words = ("create", "update", "delete", "write", "set_", "close", "approve", "release", "open_")
    assert not [t for t in tools if t.startswith(write_words)], tools


def test_qms_tool_call_over_mcp():
    from mcp_servers.qms.server import mcp as server

    async def call():
        async with create_connected_server_and_client_session(server._mcp_server) as client:
            return await client.call_tool("get_deviation", {"dev_id": "DEV-2024-007"})

    res = asyncio.run(call())
    assert not res.isError
    assert "aeration_supply" in res.content[0].text
