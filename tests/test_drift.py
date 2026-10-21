"""Drift scenarios and the Phase 2 gate logic (no broker, no Spark)."""

import json
from pathlib import Path

import pytest

from drift import check
from drift import scenarios as S
from drift.inject import source_slice, write_csv
from lakehouse.contracts import check_record, load_contract
from streaming import indpensim, replayer
from streaming.config import SENSOR_TOPIC
from streaming.openfda_producer import normalise

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "openfda_enforcement.json").read_text())["results"]


class ListSink:
    def __init__(self):
        self.records = []

    def send(self, key, record):
        self.records.append(record)

    def close(self):
        pass


@pytest.fixture
def slice_(sample_csv):
    return source_slice(sample_csv, rows=20)


@pytest.mark.parametrize("sc", [s for s in S.SENSOR_SCENARIOS if s.layer == "source_csv"], ids=lambda s: s.id)
def test_source_csv_scenarios_end_to_end_through_replayer(sc, slice_, tmp_path):
    """Drifted CSV -> real replayer -> records -> contract gives exactly the expected reasons."""
    header, rows = slice_
    h, r = S.drift_csv(sc.id, header, rows)
    path = tmp_path / f"{sc.id}.csv"
    write_csv(path, h, r)
    sink = ListSink()
    manifest = replayer.replay(path, sink, batch_id_offset=S.batch_offset(sc.id))
    assert len(sink.records) == 20
    assert {rec["batch_id"] for rec in sink.records} == {S.batch_offset(sc.id) + 1}
    contract = load_contract("process_sensor")
    reasons = {x for rec in sink.records for x in check_record(contract, rec)}
    assert reasons == set(sc.expected_reasons)
    if sc.source_signal:
        assert manifest["source_schema"][sc.source_signal], f"{sc.id} should already be flagged at the source"
    else:
        assert not any(manifest["source_schema"][k] for k in ("unknown_columns_dropped", "non_numeric_values_nulled"))


def test_drift_csv_does_not_modify_input(slice_):
    header, rows = slice_
    before = (list(header), [list(r) for r in rows])
    S.drift_csv("S2", header, rows)
    S.drift_csv("S4", header, rows)
    assert (header, rows) == before


def test_avro_drift_compatibility_matches_expectations():
    """S5/S6 are BACKWARD-compatible (so the registry lets them through), S7 is not."""
    import fastavro

    approved = SENSOR_TOPIC.schema_dict()
    for sid in ("S5", "S6", "S7"):
        fastavro.parse_schema(json.loads(S.drift_schema(sid, SENSOR_TOPIC.schema_str())))
    s5 = json.loads(S.drift_schema("S5", SENSOR_TOPIC.schema_str()))
    assert "temperature" in [f["name"] for f in s5["fields"]] and len(s5["fields"]) == len(approved["fields"])
    s6 = json.loads(S.drift_schema("S6", SENSOR_TOPIC.schema_str()))
    assert "ph" not in [f["name"] for f in s6["fields"]]
    rec = {"temperature_k": 298.0, "ph": 6.5}
    assert S.drift_avro_record("S5", rec) == {"temperature": 298.0, "ph": 6.5}
    assert S.drift_avro_record("S6", rec) == {"temperature_k": 298.0}


@pytest.mark.parametrize("sc", S.RECALL_SCENARIOS, ids=lambda s: s.id)
def test_recall_scenarios(sc):
    contract = load_contract("fda_recall")
    raw = S.drift_recall(sc.id, FIXTURE[0], 0)
    reasons = check_record(contract, normalise(raw, 1))
    assert reasons == sc.expected_reasons
    assert raw["recall_number"].startswith("D-9")


def test_scenario_keys_never_collide_with_real_data():
    assert min(S.batch_offset(s.id) for s in S.SENSOR_SCENARIOS) >= 9000
    assert len({S.batch_offset(s.id) for s in S.SENSOR_SCENARIOS}) == len(S.SENSOR_SCENARIOS)
    assert len({S.drift_recall(s.id, FIXTURE[0], 0)["recall_number"] for s in S.RECALL_SCENARIOS}) == 4


# ---------------------------------------------------------------- gate logic


def _manifest():
    return {
        "scenarios": [
            {"id": "S0", "title": "control", "layer": "source_csv", "expect": "silver", "expected_reasons": [],
             "keys": {"batch_id": [9001]}, "sent": 2},
            {"id": "S4", "title": "degC", "layer": "source_csv", "expect": "quarantine",
             "expected_reasons": ["out_of_range:temperature_k"], "keys": {"batch_id": [9401]}, "sent": 2},
            {"id": "S7", "title": "retype", "layer": "avro_schema", "expect": "rejected_by_registry",
             "expected_reasons": [], "keys": {"batch_id": [9701]}, "sent": 0, "registry": {"accepted": False}},
            {"id": "R2", "title": "date", "layer": "openfda_json", "expect": "quarantine",
             "expected_reasons": ["bad_format:report_date"], "keys": {"recall_number": ["D-9200-2026"]}, "sent": 1,
             "source_signal": "unknown_keys", "source_evidence": {"unknown_keys": {}}},
        ]
    }  # fmt: skip


def _tables(leak=False):
    silver = {
        "process_sensor": [{"batch_id": 9001}, {"batch_id": 9001}] + ([{"batch_id": 9401}] if leak else []),
        "fda_recall": [],
    }
    quarantine = {
        "process_sensor": [{"batch_id": 9401, "reasons": ["out_of_range:temperature_k"]}] * (1 if leak else 2),
        "fda_recall": [{"recall_number": "D-9200-2026", "reasons": ["bad_format:report_date"]}],
    }
    return silver, quarantine, {"out_of_range:temperature_k", "bad_format:report_date"}  # fmt: skip


def test_gate_passes_when_everything_is_caught():
    report = check.evaluate(_manifest(), *_tables())
    assert report["passed"], report
    assert report["drift_scenarios_caught"] == "3/3"
    assert report["drifted_records_in_silver"] == 0


def test_gate_fails_if_one_drifted_record_reaches_silver():
    report = check.evaluate(_manifest(), *_tables(leak=True))
    assert not report["passed"]
    assert report["drifted_records_in_silver"] == 1
    assert [r["id"] for r in report["scenarios"] if not r["passed"]] == ["S4"]


def test_gate_fails_without_an_alert():
    silver, quarantine, _ = _tables()
    report = check.evaluate(_manifest(), silver, quarantine, {"bad_format:report_date"})
    assert [r["id"] for r in report["scenarios"] if not r["passed"]] == ["S4"]


def test_gate_fails_if_control_is_quarantined():
    silver, quarantine, alerts = _tables()
    quarantine["process_sensor"].append({"batch_id": 9001, "reasons": ["out_of_range:ph"]})
    report = check.evaluate(_manifest(), silver, quarantine, alerts)
    assert [r["id"] for r in report["scenarios"] if not r["passed"]] == ["S0"]


def test_indpensim_slice_reads_start_of_batch_1(slice_):
    header, rows = slice_
    assert len(rows) == 20
    assert all(len(r) == len(header) for r in rows)
    assert indpensim.canonical_name(header[0]) == "time_h"
