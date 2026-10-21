"""Data contracts (contracts/*.json): consistent with the Avro schemas, pass on
good data, and give the right reason code for each kind of drift."""

import io
import json
import urllib.error
from pathlib import Path

import pytest

from lakehouse import contracts as C
from streaming import indpensim
from streaming.config import RECALL_TOPIC, SENSOR_TOPIC
from streaming.openfda_producer import normalise

FIXTURES = Path(__file__).parent / "fixtures"
SCHEMAS = Path(__file__).resolve().parent.parent / "streaming" / "schemas"


@pytest.fixture(scope="module")
def sensor():
    return C.load_contract("process_sensor")


@pytest.fixture(scope="module")
def recall():
    return C.load_contract("fda_recall")


@pytest.fixture
def good_reading(sample_csv):
    bid, frame = next(indpensim.iter_batches(sample_csv))
    return next(indpensim.to_records(bid, frame, "test"))


@pytest.mark.parametrize("name,topic", [("process_sensor", SENSOR_TOPIC), ("fda_recall", RECALL_TOPIC)])
def test_contract_matches_avro_schema(name, topic):
    contract = C.load_contract(name)
    avro_fields = {f["name"]: f for f in topic.schema_dict()["fields"]}
    for rule in contract.rules:
        assert rule.name in avro_fields, f"{contract.tag}: {rule.name} is not in {topic.schema_file}"
    for k in contract.key:
        assert k in avro_fields
    # A required contract field must not be nullable-with-default in Avro without a rule saying so:
    # the contract is stricter than the schema on purpose, never looser.
    for rule in contract.rules:
        if not rule.required:
            assert "null" in json.dumps(avro_fields[rule.name]["type"]), rule.name
    assert all((SCHEMAS / s).exists() for s in contract.approved_schemas)


def test_every_sample_reading_passes(sample_csv, sensor):
    bad = [
        (r["batch_id"], r["seq"], reasons)
        for bid, frame in indpensim.iter_batches(sample_csv)
        for r in indpensim.to_records(bid, frame, "t")
        if (reasons := C.check_record(sensor, r))
    ]
    assert bad == []


def test_fixture_recalls(recall):
    raw = json.loads((FIXTURES / "openfda_enforcement.json").read_text())["results"]
    complete, _no_number, partial = raw[0:2], raw[2], raw[3]
    for r in complete:
        assert C.check_record(recall, normalise(r, 1_760_000_000_000)) == [], r["recall_number"]
    # The partial duplicate lacks firm and product: quarantined, not silently accepted.
    assert C.check_record(recall, normalise(partial, 1_760_000_000_000)) == [
        "required_null:recalling_firm",
        "required_null:product_description",
    ]


@pytest.mark.parametrize(
    "change,expected",
    [
        ({"temperature_k": 25.4}, ["out_of_range:temperature_k"]),  # unit change K -> degC
        ({"temperature_k": None}, ["required_null:temperature_k"]),  # renamed/dropped column
        ({"ph": float("nan")}, []),  # NaN never reaches here: the replayer turns it into None
        ({"pat_ref": 3}, ["not_allowed:pat_ref"]),
        ({"fault_flag": None, "ph": 12.0}, ["required_null:fault_flag", "out_of_range:ph"]),
        ({"viscosity_offline_cp": None}, []),  # optional offline assay
    ],
)
def test_sensor_drift_reasons(sensor, good_reading, change, expected):
    rec = {**good_reading, **change}
    if change.get("ph") != change.get("ph"):  # NaN case: simulate the replayer's cleaning
        rec["ph"] = None
        expected = ["required_null:ph"]
    assert C.check_record(sensor, rec) == expected


@pytest.mark.parametrize(
    "change,expected",
    [
        ({"recall_number": "N/A"}, ["bad_format:recall_number"]),  # a real openFDA record looks like this
        ({"classification": "Class 1"}, ["not_allowed:classification"]),
        ({"report_date": "2026-01-02"}, ["bad_format:report_date"]),  # date format drift
        ({"report_date": None}, ["required_null:report_date"]),  # renamed/dropped key
        ({"termination_date": None}, []),  # optional
    ],
)
def test_recall_drift_reasons(recall, change, expected):
    raw = json.loads((FIXTURES / "openfda_enforcement.json").read_text())["results"][0]
    assert raw["recall_number"]
    rec = {**normalise(raw, 1_760_000_000_000), **change}
    assert C.check_record(recall, rec) == expected


def test_same_schema_ignores_docs_and_formatting():
    a = {"type": "record", "name": "R", "fields": [{"name": "x", "type": "double", "doc": "one"}]}
    b = json.dumps({"type": "record", "name": "R", "fields": [{"name": "x", "type": "double", "doc": "two"}]}, indent=4)
    renamed = {"type": "record", "name": "R", "fields": [{"name": "y", "type": "double"}]}
    assert C.same_schema(a, b)
    assert not C.same_schema(a, renamed)


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_approved_schema_ids_only_returns_matching_versions(monkeypatch, tmp_path):
    approved = (SCHEMAS / "sensor_reading.avsc").read_text()
    drifted = json.loads(approved)
    drifted["fields"][5]["name"] = "renamed_field"
    pages = {
        "/subjects/s-value/versions": [1, 2],
        "/subjects/s-value/versions/1": {"id": 7, "schema": approved},
        "/subjects/s-value/versions/2": {"id": 9, "schema": json.dumps(drifted)},
    }

    def fake_urlopen(url, timeout=None):
        return _Resp(json.dumps(pages[url.replace("http://reg", "")]).encode())

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    assert C.approved_schema_ids("http://reg", "s-value", [SCHEMAS / "sensor_reading.avsc"]) == {7}


def test_approved_schema_ids_unknown_subject_is_empty(monkeypatch):
    def not_found(url, timeout=None):
        raise urllib.error.HTTPError(url, 404, "not found", {}, None)

    monkeypatch.setattr("urllib.request.urlopen", not_found)
    assert C.approved_schema_ids("http://reg", "nope-value", [SCHEMAS / "sensor_reading.avsc"]) == set()
