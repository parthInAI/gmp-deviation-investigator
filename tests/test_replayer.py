import io
import json
import struct

import fastavro
import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

from streaming import replayer
from streaming.config import SENSOR_TOPIC
from streaming.sinks import NdjsonSink, make_value_serializer


class ListSink:
    def __init__(self):
        self.sent, self.closed = [], False

    def send(self, key, record):
        self.sent.append((key, record))

    def close(self):
        self.closed = True


def test_parse_batches():
    assert replayer.parse_batches(None) is None
    assert replayer.parse_batches("1-3,7, 9-10") == {1, 2, 3, 7, 9, 10}


@pytest.mark.parametrize(
    "prev, now, speed, expected",
    [(None, 0.2, 720, 0.0), (0.2, 0.4, 0, 0.0), (0.2, 0.4, 720, 1.0), (0.2, 0.4, 3600, 0.2)],
)
def test_sleep_for(prev, now, speed, expected):
    assert replayer.sleep_for(prev, now, speed) == pytest.approx(expected)


def test_replay_manifest_and_keys(sample_csv):
    sink = ListSink()
    m = replayer.replay(sample_csv, sink, replay_id="t1")
    assert sink.closed
    assert m["batches"] == 10 and m["total_rows"] == len(sink.sent)
    assert m["fault_batches"] == [10]
    assert sum(m["rows_per_batch"].values()) == m["total_rows"]
    # key is the batch id: all of a batch on one partition, in order
    for key, rec in sink.sent:
        assert key == str(rec["batch_id"]) and rec["replay_id"] == "t1"


def test_replay_subset(sample_csv):
    sink = ListSink()
    m = replayer.replay(sample_csv, sink, batches={2, 9}, replay_id="t2")
    assert sorted(int(b) for b in m["rows_per_batch"]) == [2, 9]
    assert {r["batch_id"] for _, r in sink.sent} == {2, 9}


def test_ndjson_sink_writes_one_file_per_batch(sample_csv, tmp_path):
    out = tmp_path / "nd"
    m = replayer.replay(sample_csv, NdjsonSink(out, prefix="batch"), replay_id="t3")
    files = sorted(p.name for p in out.iterdir())
    assert files[0] == "batch_001.jsonl" and len(files) == 10
    lines = (out / "batch_004.jsonl").read_text().splitlines()
    assert len(lines) == m["rows_per_batch"]["4"]
    assert json.loads(lines[0])["batch_id"] == 4


def test_inspect_reports_mapping(sample_csv):
    report = replayer.inspect(sample_csv)
    assert report["batch_id_source"] == "column"
    assert report["header_repair"].startswith("merged split PAT/Raman legend")
    assert report["unknown_columns_dropped"] == [] and report["duplicate_columns_dropped"] == []
    assert report["raman_columns_dropped"] == 4
    assert report["contract_fields_missing_from_file"] == []


def test_wire_format_is_confluent_avro(sample_csv):
    """What lands on Kafka: 0x00 + 4-byte schema id + Avro body (what bronze.py decodes)."""
    client = SchemaRegistryClient.new_client({"url": "mock://replayer-test"})
    schema_id = client.register_schema(SENSOR_TOPIC.subject, Schema(SENSOR_TOPIC.schema_str(), "AVRO"))
    ser = make_value_serializer(client, SENSOR_TOPIC)
    sink = ListSink()
    replayer.replay(sample_csv, sink, batches={1}, replay_id="t4")
    rec = sink.sent[0][1]
    raw = ser(rec, SerializationContext(SENSOR_TOPIC.name, MessageField.VALUE))
    assert raw[0] == 0
    assert struct.unpack(">I", raw[1:5])[0] == schema_id
    decoded = fastavro.schemaless_reader(io.BytesIO(raw[5:]), fastavro.parse_schema(SENSOR_TOPIC.schema_dict()))
    assert decoded["batch_id"] == 1 and decoded["seq"] == 1


def test_producer_refuses_unregistered_schema(sample_csv):
    """auto.register.schemas=False: a schema nobody registered cannot be produced."""
    client = SchemaRegistryClient.new_client({"url": "mock://empty-registry"})
    ser = make_value_serializer(client, SENSOR_TOPIC)
    sink = ListSink()
    replayer.replay(sample_csv, sink, batches={1}, replay_id="t5")
    with pytest.raises(SchemaRegistryError):
        ser(sink.sent[0][1], SerializationContext(SENSOR_TOPIC.name, MessageField.VALUE))


def test_batch_id_offset_and_non_numeric_values(tmp_path):
    from streaming.sample_data import make_frame, write_csv

    frame = make_frame(n_batches=2, rows=10, seed=4)
    frame["pH(pH:pH)"] = frame["pH(pH:pH)"].map(lambda v: f"{v:.2f} pH")  # retyped to text upstream
    path = tmp_path / "drift.csv"
    write_csv(frame, path)
    sink = ListSink()
    manifest = replayer.replay(path, sink, batch_id_offset=9000)
    assert {r["batch_id"] for _, r in sink.sent} == {9001, 9002}
    assert {k for k, _ in sink.sent} == {"9001", "9002"}
    assert all(r["ph"] is None for _, r in sink.sent)
    assert manifest["source_schema"]["non_numeric_values_nulled"] == {"ph": len(sink.sent)}
    # event times still come from the source batch (plant clock), not the shifted id
    first = next(r for _, r in sink.sent if r["batch_id"] == 9001)
    assert first["event_time"] < 1_700_000_000_000


def test_manifest_reports_renamed_column(tmp_path):
    from streaming.sample_data import make_frame, write_csv

    frame = make_frame(n_batches=1, rows=5, seed=4).rename(
        columns={"Dissolved oxygen concentration(DO2:mg/L)": "Dissolved oxygen concentration(DOT:%)"}
    )
    path = tmp_path / "renamed.csv"
    write_csv(frame, path)
    manifest = replayer.replay(path, ListSink())
    src = manifest["source_schema"]
    assert src["unknown_columns_dropped"] == ["Dissolved oxygen concentration(DOT:%)"]
    assert src["contract_fields_missing_from_file"] == ["dissolved_oxygen_mgpl"]
