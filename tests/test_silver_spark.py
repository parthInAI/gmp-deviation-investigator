"""Silver transforms on a local SparkSession (no Kafka, no Delta jars needed).

The key test is parity: the Spark reason expressions must give exactly the
same reasons as the pure-Python reference (lakehouse.contracts.check_record)
for every record, good or drifted.
"""

import json
import shutil
from datetime import datetime
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("no Java runtime", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from lakehouse import silver  # noqa: E402
from lakehouse.contracts import check_record, load_contract  # noqa: E402
from lakehouse.schema_utils import avro_to_ddl  # noqa: E402
from streaming import indpensim  # noqa: E402
from streaming.openfda_producer import normalise  # noqa: E402

pytestmark = pytest.mark.spark
SCHEMAS = Path(__file__).resolve().parent.parent / "streaming" / "schemas"
FIXTURES = Path(__file__).parent / "fixtures"
APPROVED = {11}


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("silver-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


def kafka_bronze(spark, records, avsc, schema_ids=None, decode_ok=None):
    """Build a frame shaped like bronze.py output from plain dicts."""
    n = len(records)
    schema_ids = schema_ids or [11] * n
    decode_ok = decode_ok or [True] * n
    payload = spark.createDataFrame(records, schema=avro_to_ddl(SCHEMAS / avsc))
    payload = payload.withColumn("_i", F.monotonically_increasing_id())
    meta = spark.createDataFrame(
        [(i, schema_ids[i], decode_ok[i], i, b"\x00raw") for i in range(n)],
        "_j long, schema_id int, decode_ok boolean, offset long, raw_value binary",
    )
    ordered = payload.withColumn("_j", F.row_number().over(silver.Window.orderBy("_i")) - 1)
    cols = [c for c in payload.columns if c != "_i"]
    return ordered.join(meta, "_j").select(
        F.struct(*cols).alias("payload"),
        F.col(cols[0]).cast("string").alias("kafka_key"),
        F.when(F.col("decode_ok"), F.lit(True)).otherwise(F.lit(False)).alias("decode_ok"),
        "schema_id",
        F.lit("process.sensor.v1").alias("topic"),
        F.lit(0).alias("partition"),
        "offset",
        "raw_value",
        F.lit(datetime(2026, 10, 21)).alias("ingest_ts"),
    )


@pytest.fixture(scope="module")
def readings(tmp_path_factory):
    from streaming.sample_data import make_frame, write_csv

    path = tmp_path_factory.mktemp("s") / "s.csv"
    write_csv(make_frame(n_batches=3, rows=8, seed=2), path)
    return [r for b, f in indpensim.iter_batches(path) for r in indpensim.to_records(b, f, "t")]


def drifted(readings):
    good = readings[:5]
    bad = [
        {**readings[5], "temperature_k": readings[5]["temperature_k"] - 273.15},  # degC
        {**readings[6], "ph": None},  # renamed/dropped column
        {**readings[7], "pat_ref": 7, "ph": 99.0},  # two rules at once
        {**readings[8], "viscosity_offline_cp": None},  # optional: still valid
    ]
    return good + bad


def test_spark_reasons_match_python_reference(spark, readings):
    contract = load_contract("process_sensor")
    recs = drifted(readings)
    df = silver.with_reasons(
        silver.standardise_kafka_bronze(kafka_bronze(spark, recs, "sensor_reading.avsc")), contract, APPROVED
    )
    got = {(r["payload"]["batch_id"], r["payload"]["seq"]): sorted(r["reasons"]) for r in df.collect()}
    want = {(r["batch_id"], r["seq"]): sorted(check_record(contract, r)) for r in recs}
    assert got == want
    assert sum(1 for v in got.values() if v) == 3  # the three drifted records, nothing else


def test_recall_reasons_match_python_reference(spark):
    contract = load_contract("fda_recall")
    raw = json.loads((FIXTURES / "openfda_enforcement.json").read_text())["results"]
    base = normalise(raw[0], 1_760_000_000_000)
    recs = [
        base,
        {**base, "recall_number": "N/A"},
        {**base, "recall_number": "D-0200-2026", "report_date": "2026-02-11", "classification": "Class 2"},
        {**base, "recall_number": "D-0201-2026", "recalling_firm": None},
    ]
    df = silver.with_reasons(
        silver.standardise_kafka_bronze(kafka_bronze(spark, recs, "recall_event.avsc")), contract, APPROVED
    )
    got = {r["payload"]["recall_number"]: sorted(r["reasons"]) for r in df.collect()}
    want = {r["recall_number"]: sorted(check_record(contract, r)) for r in recs}
    assert got == want


def test_unapproved_schema_and_decode_failure(spark, readings):
    contract = load_contract("process_sensor")
    recs = readings[:3]
    bronze = kafka_bronze(spark, recs, "sensor_reading.avsc", schema_ids=[11, 12, 11], decode_ok=[True, True, False])
    rows = silver.with_reasons(silver.standardise_kafka_bronze(bronze), contract, APPROVED).orderBy("_offset").collect()
    assert [r["reasons"] for r in rows] == [[], ["unapproved_schema:12"], ["decode_failed"]]


def test_no_registered_versions_means_nothing_is_approved(spark, readings):
    contract = load_contract("process_sensor")
    bronze = kafka_bronze(spark, readings[:1], "sensor_reading.avsc")
    [row] = silver.with_reasons(silver.standardise_kafka_bronze(bronze), contract, set()).collect()
    assert row["reasons"] == ["unapproved_schema:11"]


def test_split_dedupes_on_key_and_keeps_every_bad_record(spark, readings):
    contract = load_contract("process_sensor")
    replayed = readings[:4] + readings[:2]  # a re-replay sends the first two again
    bad = [{**readings[4], "temperature_k": 20.0}, {**readings[4], "temperature_k": 21.0}]
    df = silver.with_reasons(
        silver.standardise_kafka_bronze(kafka_bronze(spark, replayed + bad, "sensor_reading.avsc")), contract, APPROVED
    )
    valid, quarantine = silver.split(df, contract)
    assert valid.count() == 4
    assert valid.select("batch_id", "seq").distinct().count() == 4
    assert {"_offset", "_schema_id", "_silver_ts", "_contract", "temperature_k"} <= set(valid.columns)
    assert quarantine.count() == 2  # quarantine never dedupes: every bad arrival is evidence
    q = quarantine.first()
    assert q["reasons"] == ["out_of_range:temperature_k"]
    assert json.loads(q["payload_json"])["temperature_k"] in (20.0, 21.0)

    alerts = silver.summarise(quarantine, contract, "run-1").collect()
    assert [(a["reason"], a["records"], a["contract"]) for a in alerts] == [
        ("out_of_range:temperature_k", 2, "process_sensor.v1")
    ]


def test_all_null_struct_from_permissive_decode_is_a_decode_failure(spark, readings):
    """Spark's PERMISSIVE from_avro returns a struct of nulls (not null) for bytes it
    cannot decode. Silver must not treat that as a record with 40 missing fields."""
    contract = load_contract("process_sensor")
    empty = {k: None for k in readings[0]}
    bronze = kafka_bronze(spark, [readings[0], empty], "sensor_reading.avsc", schema_ids=[11, 14])
    rows = silver.with_reasons(silver.standardise_kafka_bronze(bronze), contract, APPROVED).orderBy("_offset").collect()
    assert rows[0]["reasons"] == []
    assert rows[1]["reasons"] == ["decode_failed", "unapproved_schema:14"]
    _, quarantine = silver.split(
        silver.with_reasons(silver.standardise_kafka_bronze(bronze), contract, APPROVED), contract
    )
    assert quarantine.select("_kafka_key", "_raw_value").count() == 1


def test_bronze_knows_which_fields_cannot_be_null():
    from lakehouse.bronze import non_nullable_fields

    assert non_nullable_fields((SCHEMAS / "sensor_reading.avsc").read_text()) == [
        "batch_id",
        "seq",
        "time_h",
        "event_time",
        "replay_id",
    ]
    assert non_nullable_fields((SCHEMAS / "recall_event.avsc").read_text())[0] == "recall_number"


def test_databricks_autoloader_bronze_path(spark, readings):
    """Auto Loader bronze (JSON files): no schema ids; off-contract fields arrive in _rescued_data."""
    contract = load_contract("process_sensor")
    recs = readings[:3]
    payload = spark.createDataFrame(recs, schema=avro_to_ddl(SCHEMAS / "sensor_reading.avsc"))
    rescued = F.when(F.col("seq") == recs[2]["seq"], F.lit('{"temperature_c": 25.1}'))
    bronze = payload.select(
        F.struct(*payload.columns).alias("payload"),
        rescued.alias("_rescued_data"),
        F.lit("/Volumes/workspace/gmpdi/landing/process_sensor/batch_001.jsonl").alias("source_file"),
        F.lit(datetime(2026, 10, 21)).alias("ingest_ts"),
        rescued.isNull().alias("decode_ok"),  # what bronze_autoloader.py writes
    )
    df = silver.with_reasons(silver.standardise_file_bronze(bronze), contract, approved_ids=None)
    got = {r["payload"]["seq"]: r["reasons"] for r in df.collect()}
    assert got == {recs[0]["seq"]: [], recs[1]["seq"]: [], recs[2]["seq"]: ["rescued_data"]}
    valid, quarantine = silver.split(df, contract)
    assert valid.count() == 2
    assert quarantine.first()["_rescued_data"] == '{"temperature_c": 25.1}'
