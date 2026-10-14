"""The .avsc files are the contract. These tests stop code and contract drifting apart."""

import io
import json

import fastavro
import pytest

from lakehouse.bronze import BRONZE_TABLES
from lakehouse.schema_utils import avro_to_ddl
from streaming import indpensim
from streaming.config import RECALL_TOPIC, SENSOR_TOPIC, TOPICS
from streaming.openfda_producer import normalise


def roundtrip(schema_dict, record):
    parsed = fastavro.parse_schema(schema_dict)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    buf.seek(0)
    return fastavro.schemaless_reader(buf, parsed)


def test_sensor_contract_fields_match_code():
    names = [f["name"] for f in SENSOR_TOPIC.schema_dict()["fields"]]
    expected = ["batch_id", "seq", "time_h", "event_time", *indpensim.INT_FIELDS, *indpensim.SENSOR_FIELDS, "replay_id"]
    assert names == expected


def test_optional_fields_have_null_defaults():
    # Needed for BACKWARD-compatible evolution: new readers can fill missing fields.
    for spec in TOPICS.values():
        for f in spec.schema_dict()["fields"]:
            if isinstance(f["type"], list):
                assert f["type"][0] == "null" and f.get("default", "missing") is None, f["name"]


def test_sensor_records_roundtrip(sample_csv):
    batch_id, frame = next(indpensim.iter_batches(sample_csv))
    for rec in indpensim.to_records(batch_id, frame, "r1"):
        out = roundtrip(SENSOR_TOPIC.schema_dict(), rec)
        assert out["batch_id"] == rec["batch_id"] and out["seq"] == rec["seq"]
        assert out["temperature_k"] == pytest.approx(rec["temperature_k"])


def test_recall_records_roundtrip():
    fixture = json.loads(open("tests/fixtures/openfda_enforcement.json").read())
    rec = normalise(fixture["results"][0], 1_760_000_000_000)
    out = roundtrip(RECALL_TOPIC.schema_dict(), rec)
    assert out["recall_number"] == "D-0101-2026"
    assert out["openfda_brand_name"] == ["EXAMPLICILLIN"]


def test_every_topic_has_a_bronze_table_and_vice_versa():
    assert set(BRONZE_TABLES) == set(TOPICS)
    for topic, (schema_file, _) in BRONZE_TABLES.items():
        assert TOPICS[topic].schema_file == schema_file


def test_avro_to_spark_ddl():
    ddl = avro_to_ddl(SENSOR_TOPIC.schema_path)
    assert ddl.startswith("`batch_id` INT, `seq` INT, `time_h` DOUBLE, `event_time` BIGINT")
    assert "`fault_ref` INT" in ddl and "`replay_id` STRING" in ddl
    assert "ARRAY<STRING>" in avro_to_ddl(RECALL_TOPIC.schema_path)


def test_avro_to_ddl_rejects_unsupported_unions():
    with pytest.raises(ValueError):
        avro_to_ddl({"fields": [{"name": "x", "type": ["null", "int", "string"]}]})
