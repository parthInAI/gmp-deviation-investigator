"""Silver -> gold: the tables the detection model, agents and dashboard read.

    spark-submit ... lakehouse/gold.py

  gold/batch_summary    one row per batch: campaign, length, fault info, final titre,
                        and the range of the key process variables
  gold/golden_profile   per process hour: p10 / p50 / p90 of each key variable across
                        NORMAL batches only (no fault flag). The "golden batch" envelope
                        that Phase 3 compares new batches against.
  gold/recall_summary   openFDA recalls per report month and class, and how many cite CGMP

Gold is recomputed in full from silver on every run (overwrite). Silver is
small (~114k rows) and deduplicated, so a full rebuild takes seconds and is
trivially idempotent; incremental gold would add state to get wrong for no gain.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# The variables an investigator looks at first; also the model's core features.
KEY_VARIABLES = [
    "temperature_k",
    "ph",
    "dissolved_oxygen_mgpl",
    "substrate_conc_gpl",
    "penicillin_conc_gpl",
    "oxygen_uptake_rate",
    "carbon_evolution_rate",
    "offgas_co2_pct",
    "vessel_volume_l",
    "agitator_rpm",
    "aeration_rate_lph",
]


def campaign(batch_id: F.Column, control_ref: F.Column, pat_ref: F.Column, fault_flag: F.Column) -> F.Column:
    """How the batch was run, from the reference flags (not from batch-number ranges)."""
    return (
        F.when(fault_flag == 1, "fault")
        .when(pat_ref == 2, "raman_pat_control")
        .when(control_ref == 1, "operator_controlled")
        .otherwise("recipe_driven")
    )


def as_timestamp(df: DataFrame, col: str) -> F.Column:
    """event_time is a TIMESTAMP when decoded from Avro (Kafka path) but epoch
    millis when read from NDJSON (Databricks path); accept both."""
    if dict(df.dtypes)[col] in ("bigint", "int"):
        return F.timestamp_millis(F.col(col))
    return F.col(col).cast("timestamp")


def batch_summary(silver: DataFrame) -> DataFrame:
    silver = silver.withColumn("event_time", as_timestamp(silver, "event_time"))
    last = Window.partitionBy("batch_id").orderBy(F.col("seq").desc())
    final = (
        silver.withColumn("_r", F.row_number().over(last))
        .filter("_r = 1")
        .select("batch_id", F.col("penicillin_conc_gpl").alias("final_penicillin_gpl"))
    )
    stats = []
    for v in KEY_VARIABLES:
        stats += [F.min(v).alias(f"{v}_min"), F.avg(v).alias(f"{v}_mean"), F.max(v).alias(f"{v}_max")]
    agg = silver.groupBy("batch_id").agg(
        F.count("*").alias("readings"),
        F.max("seq").alias("max_seq"),
        F.max("time_h").alias("duration_h"),
        F.min("event_time").alias("start_time"),
        F.max("event_time").alias("end_time"),
        F.max("fault_flag").alias("fault_flag"),
        F.max("fault_ref").alias("fault_ref_max"),
        F.sum(F.when(F.col("fault_ref") > 0, 1).otherwise(0)).alias("fault_readings"),
        F.min(F.when(F.col("fault_ref") > 0, F.col("time_h"))).alias("fault_onset_h"),
        F.max("control_ref").alias("control_ref"),
        F.max("pat_ref").alias("pat_ref"),
        *stats,
    )
    return (
        agg.join(final, "batch_id")
        .withColumn(
            "campaign", campaign(F.col("batch_id"), F.col("control_ref"), F.col("pat_ref"), F.col("fault_flag"))
        )
        .withColumn("complete", F.col("readings") == F.col("max_seq"))  # no gaps in seq
        .withColumn("_gold_ts", F.current_timestamp())
        .orderBy("batch_id")
    )


def golden_profile(
    silver: DataFrame, quantiles=(0.1, 0.5, 0.9), accuracy: int = 10_000, min_batches: int = 20
) -> DataFrame:
    normal = silver.filter(F.col("fault_flag") == 0).withColumn("hour", F.floor("time_h").cast("int"))
    aggs = [F.countDistinct("batch_id").alias("n_batches"), F.count("*").alias("n_readings")]
    for v in KEY_VARIABLES:
        q = F.percentile_approx(v, list(quantiles), accuracy)
        aggs += [q[i].alias(f"{v}_p{int(p * 100)}") for i, p in enumerate(quantiles)]
    return (
        normal.groupBy("hour")
        .agg(*aggs)
        # Batches end between ~200 and 290 h, so late hours rest on a handful of batches.
        # Phase 3 must not judge a batch against an envelope built from fewer than this.
        .withColumn("low_support", F.col("n_batches") < min_batches)
        .withColumn("_gold_ts", F.current_timestamp())
        .orderBy("hour")
    )


def recall_summary(silver: DataFrame) -> DataFrame:
    return (
        silver.withColumn("report_month", F.date_format(F.to_date("report_date", "yyyyMMdd"), "yyyy-MM"))
        .groupBy("report_month", "classification")
        .agg(
            F.count("*").alias("recalls"),
            F.sum(F.when(F.lower("reason_for_recall").contains("cgmp"), 1).otherwise(0)).alias("cgmp_recalls"),
            F.countDistinct("recalling_firm").alias("firms"),
        )
        .withColumn("_gold_ts", F.current_timestamp())
        .orderBy("report_month", "classification")
    )


def build_spark(app: str = "gmpdi-gold") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lakehouse", default=os.environ.get("LAKEHOUSE_DIR", "/lakehouse"))
    args = p.parse_args(argv)
    lh = args.lakehouse

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    from delta.tables import DeltaTable

    def write(df: DataFrame, name: str) -> None:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{lh}/gold/{name}")
        print(f"gold/{name}: {spark.read.format('delta').load(f'{lh}/gold/{name}').count()} rows")

    sensor_path, recall_path = f"{lh}/silver/process_sensor", f"{lh}/silver/fda_recall"
    if DeltaTable.isDeltaTable(spark, sensor_path):
        sensor = spark.read.format("delta").load(sensor_path).cache()
        write(batch_summary(sensor), "batch_summary")
        write(golden_profile(sensor), "golden_profile")
    else:
        print(f"no silver table at {sensor_path}; run silver.py first")
    if DeltaTable.isDeltaTable(spark, recall_path):
        write(recall_summary(spark.read.format("delta").load(recall_path)), "recall_summary")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
