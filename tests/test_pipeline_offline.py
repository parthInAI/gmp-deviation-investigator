"""Phase 1 gate, offline: replayer -> Confluent-Avro bytes -> bronze-shaped Delta -> gate CLI.

Stands in for Kafka and Spark (which the phase1-gate CI job runs for real) so
the replayer's manifest and the gate check are tested against each other on
every commit, with the same serializer and the same wire format.
"""

import io
import json

import fastavro
import pyarrow as pa
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext
from deltalake import write_deltalake

from lakehouse import gate_check
from streaming import replayer
from streaming.config import SENSOR_TOPIC
from streaming.sample_data import make_frame, write_csv
from streaming.sinks import make_value_serializer


class SerializingSink:
    """Acts like KafkaSink, but keeps the produced bytes in memory."""

    def __init__(self):
        client = SchemaRegistryClient.new_client({"url": "mock://pipeline"})
        client.register_schema(SENSOR_TOPIC.subject, Schema(SENSOR_TOPIC.schema_str(), "AVRO"))
        self.ser = make_value_serializer(client, SENSOR_TOPIC)
        self.ctx = SerializationContext(SENSOR_TOPIC.name, MessageField.VALUE)
        self.messages: list[bytes] = []

    def send(self, key, record):
        self.messages.append(self.ser(record, self.ctx))

    def close(self):
        pass


def bronze_decode(raw: bytes, parsed_schema):
    """Same rule as lakehouse/bronze.py: valid header -> decode body, else payload null."""
    if len(raw) <= 5 or raw[0] != 0:
        return None
    return fastavro.schemaless_reader(io.BytesIO(raw[5:]), parsed_schema)


def test_hundred_batches_pass_the_gate(tmp_path, capsys):
    csv = tmp_path / "indpensim.csv"
    write_csv(make_frame(n_batches=100, rows=12, seed=3), csv)

    sink = SerializingSink()
    manifest = replayer.replay(csv, sink, replay_id="offline-gate")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    parsed = fastavro.parse_schema(SENSOR_TOPIC.schema_dict())
    payloads = [bronze_decode(m, parsed) for m in sink.messages]
    payloads.append(bronze_decode(b"\x01garbage", parsed))  # one poisoned message
    table = pa.table({"payload": payloads, "decode_ok": [p is not None for p in payloads]})
    write_deltalake(str(tmp_path / "lh" / "bronze" / "process_sensor"), table)

    args = ["--lakehouse", str(tmp_path / "lh"), "--manifest", str(manifest_path),
            "--report", str(tmp_path / "gate.json")]
    # The poisoned message must fail the gate: bronze kept it, the gate noticed it.
    assert gate_check.main(args) == 1
    report = json.loads((tmp_path / "gate.json").read_text())
    assert report["undecoded_rows"] == 1
    assert report["batches_in_bronze"] == 100
    assert report["readings_in_bronze"] == manifest["total_rows"]
    assert report["fault_batches"] == list(range(91, 101))

    # Without the poisoned row, the gate passes.
    clean = table.filter(pa.array([p is not None for p in payloads]))
    write_deltalake(str(tmp_path / "lh2" / "bronze" / "process_sensor"), clean)
    args[1] = str(tmp_path / "lh2")
    assert gate_check.main(args) == 0
    assert "PHASE 1 GATE: PASS" in capsys.readouterr().out
