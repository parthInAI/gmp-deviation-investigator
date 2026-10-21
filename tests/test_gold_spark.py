"""Gold transforms on a local SparkSession, fed with synthetic silver rows."""

import shutil

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("no Java runtime", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from lakehouse import gold  # noqa: E402
from lakehouse.schema_utils import avro_to_ddl  # noqa: E402
from streaming import indpensim  # noqa: E402
from streaming.sample_data import make_frame, write_csv  # noqa: E402
from tests.test_silver_spark import SCHEMAS  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("gold-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture(scope="module")
def silver_sensor(spark, tmp_path_factory):
    path = tmp_path_factory.mktemp("g") / "s.csv"
    write_csv(make_frame(n_batches=100, rows=12, seed=3), path)  # 91-100 are fault batches
    recs = [r for b, f in indpensim.iter_batches(path) for r in indpensim.to_records(b, f, "t")]
    return spark.createDataFrame(recs, schema=avro_to_ddl(SCHEMAS / "sensor_reading.avsc"))


def test_batch_summary(silver_sensor):
    rows = {r["batch_id"]: r for r in gold.batch_summary(silver_sensor).collect()}
    assert sorted(rows) == list(range(1, 101))
    assert all(r["complete"] for r in rows.values())
    assert rows[95]["campaign"] == "fault" and rows[95]["fault_readings"] > 0 and rows[95]["fault_onset_h"] > 0
    assert rows[1]["campaign"] == "recipe_driven" and rows[1]["fault_onset_h"] is None
    assert rows[40]["campaign"] == "operator_controlled"
    assert rows[70]["campaign"] == "raman_pat_control"
    assert rows[1]["temperature_k_min"] <= rows[1]["temperature_k_mean"] <= rows[1]["temperature_k_max"]
    assert rows[1]["final_penicillin_gpl"] > 20  # sample titre ramps to ~25 g/L


def test_batch_summary_accepts_avro_timestamps(silver_sensor):
    """The Kafka path decodes event_time as TIMESTAMP, the NDJSON path as epoch millis."""
    from pyspark.sql import functions as F

    as_ts = silver_sensor.withColumn("event_time", F.timestamp_millis("event_time"))
    a = gold.batch_summary(silver_sensor).select("batch_id", "start_time", "end_time").collect()
    b = gold.batch_summary(as_ts).select("batch_id", "start_time", "end_time").collect()
    assert a == b
    assert str(a[0]["start_time"]).startswith("2023-01-01")  # PLANT_EPOCH


def test_golden_profile_uses_normal_batches_only(silver_sensor):
    prof = gold.golden_profile(silver_sensor).collect()
    assert prof[0]["hour"] == 0
    assert max(r["n_batches"] for r in prof) == 90  # the 10 fault batches are excluded
    assert prof[0]["low_support"] is False
    for r in prof:
        assert r["temperature_k_p10"] <= r["temperature_k_p50"] <= r["temperature_k_p90"]


def test_recall_summary(spark):
    rows = [
        ("D-0001-2026", "Class II", "20260105", "CGMP Deviations: OOS", "Firm A"),
        ("D-0002-2026", "Class II", "20260120", "Subpotent", "Firm B"),
        ("D-0003-2026", "Class I", "20260211", "cgmp: sterility", "Firm A"),
    ]
    df = spark.createDataFrame(
        rows,
        "recall_number string, classification string, report_date string, "
        "reason_for_recall string, recalling_firm string",
    )
    got = [
        (r["report_month"], r["classification"], r["recalls"], r["cgmp_recalls"])
        for r in gold.recall_summary(df).collect()
    ]
    assert got == [("2026-01", "Class II", 2, 1), ("2026-02", "Class I", 1, 1)]
