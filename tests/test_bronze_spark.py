"""Bronze envelope logic on a local SparkSession (no Kafka, no extra jars).

from_avro needs the spark-avro package, so decoding itself is covered by the
phase1-gate CI job; here we test the wire-header parsing that decides whether
a message is even decodable.
"""

import shutil
import struct
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("no Java runtime", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from lakehouse.bronze import kafka_envelope  # noqa: E402

pytestmark = pytest.mark.spark

KAFKA_SCHEMA = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
    ]
)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("bronze-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def kafka_row(value: bytes, offset: int):
    return (b"94", value, "process.sensor.v1", 3, offset, datetime(2026, 10, 14, 12, 0))


def test_envelope_extracts_schema_id_and_metadata(spark):
    good = b"\x00" + struct.pack(">I", 1234) + b"avro-body"
    no_magic = b"\x01" + struct.pack(">I", 1234) + b"avro-body"
    too_short = b"\x00\x00\x00"
    df = spark.createDataFrame(
        [kafka_row(good, 1), kafka_row(no_magic, 2), kafka_row(too_short, 3), kafka_row(None, 4)], KAFKA_SCHEMA
    )
    rows = {r["offset"]: r for r in kafka_envelope(df).collect()}

    assert rows[1]["schema_id"] == 1234
    assert rows[1]["kafka_key"] == "94" and rows[1]["partition"] == 3
    assert bytes(rows[1]["raw_value"]) == good  # raw bytes kept for re-decoding
    assert rows[2]["schema_id"] is None
    assert rows[3]["schema_id"] is None
    assert rows[4]["schema_id"] is None
    assert all(r["ingest_date"] is not None for r in rows.values())


def test_large_schema_ids_do_not_overflow(spark):
    big = b"\x00" + struct.pack(">I", 2_000_000_000) + b"x"
    out = kafka_envelope(spark.createDataFrame([kafka_row(big, 1)], KAFKA_SCHEMA)).collect()[0]
    assert out["schema_id"] == 2_000_000_000
