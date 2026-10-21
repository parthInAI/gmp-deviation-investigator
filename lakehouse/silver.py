"""Bronze -> silver: every record checked against its data contract.

    spark-submit ... lakehouse/silver.py --contract all

For each micro-batch read from a bronze Delta table:

  * valid records   -> silver/<table>: flattened, typed, deduplicated on the
                       contract key (insert-only MERGE, so replays never duplicate)
  * invalid records -> quarantine/<table>: the payload as JSON, the raw bytes,
                       and EVERY reason it failed (nothing is dropped)
  * per run         -> alerts/contract_violations: count of records per reason

Record-level checks on top of the field rules (lakehouse/contracts.py):
  decode_failed           bronze could not decode the Avro body
  unapproved_schema:<id>  written with a registry schema version the contract has
                          not approved. Avro bodies are not self-describing, so a
                          message written with another schema decodes into
                          plausible-looking garbage; the id is the only safe guard.
  rescued_data            (Databricks Auto Loader input) fields outside the contract

The transformation functions take and return DataFrames, so the Databricks
notebook reuses them unchanged.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lakehouse.contracts import Contract, approved_schema_ids, load_contract  # noqa: E402

_SUFFIX = os.environ.get("GMPDI_TOPIC_SUFFIX", "")
# contract name -> (bronze/silver/quarantine table name, base topic)
CONTRACT_TABLES = {
    "process_sensor": ("process_sensor", "process.sensor.v1"),
    "fda_recall": ("fda_recall", "quality.fda_recall.v1"),
}
SCHEMA_DIR = Path(os.environ.get("GMPDI_SCHEMA_DIR", Path(__file__).resolve().parent.parent / "streaming" / "schemas"))


# ---------------------------------------------------------------- transforms


def standardise_kafka_bronze(df: DataFrame) -> DataFrame:
    """bronze.py output -> common shape."""
    return df.select(
        "payload",
        F.col("decode_ok").cast("boolean").alias("decode_ok"),
        F.col("schema_id").alias("_schema_id"),
        F.col("kafka_key").alias("_kafka_key"),
        F.col("topic").alias("_topic"),
        F.col("partition").alias("_partition"),
        F.col("offset").alias("_offset"),
        F.lit(None).cast("string").alias("_source_file"),
        F.lit(None).cast("string").alias("_rescued_data"),
        F.col("raw_value").alias("_raw_value"),
        F.col("ingest_ts").alias("_bronze_ingest_ts"),
        F.col("offset").alias("_order"),
    )


def standardise_file_bronze(df: DataFrame) -> DataFrame:
    """Databricks Auto Loader bronze (bronze_autoloader.py) -> common shape."""
    return df.select(
        "payload",
        # JSON always parses; off-contract fields are reported as rescued_data (plus the
        # field checks they break), not as decode failures.
        F.lit(True).alias("decode_ok"),
        F.lit(None).cast("int").alias("_schema_id"),
        F.lit(None).cast("string").alias("_kafka_key"),
        F.lit(None).cast("string").alias("_topic"),
        F.lit(None).cast("int").alias("_partition"),
        F.lit(None).cast("long").alias("_offset"),
        F.col("source_file").alias("_source_file"),
        F.col("_rescued_data").cast("string").alias("_rescued_data"),
        F.lit(None).cast("binary").alias("_raw_value"),
        F.col("ingest_ts").alias("_bronze_ingest_ts"),
        F.unix_timestamp("ingest_ts").cast("long").alias("_order"),
    )


def field_reason_exprs(contract: Contract) -> list[Column]:
    """One nullable reason column per rule; null means the rule passed."""
    out = []
    for r in contract.rules:
        c = F.col(f"payload.{r.name}")
        if r.required:
            out.append(F.when(c.isNull(), F.lit(f"required_null:{r.name}")))
        if r.min is not None or r.max is not None:
            bad = F.lit(False)
            if r.min is not None:
                bad = bad | (c < F.lit(r.min))
            if r.max is not None:
                bad = bad | (c > F.lit(r.max))
            out.append(F.when(c.isNotNull() & bad, F.lit(f"out_of_range:{r.name}")))
        if r.allowed is not None:
            out.append(F.when(c.isNotNull() & ~c.isin(list(r.allowed)), F.lit(f"not_allowed:{r.name}")))
        if r.pattern is not None:
            out.append(F.when(c.isNotNull() & ~c.cast("string").rlike(r.pattern), F.lit(f"bad_format:{r.name}")))
    return out


def with_reasons(df: DataFrame, contract: Contract, approved_ids: set[int] | None) -> DataFrame:
    """Add `reasons` (array<string>, empty = valid) to a standardised bronze frame.

    approved_ids=None skips the schema-version check (file-based input has no ids).
    """
    schema_reason = F.lit(None).cast("string")
    if approved_ids is not None:
        ids = sorted(approved_ids) or [-1]
        schema_reason = F.when(
            F.col("_schema_id").isNull() | ~F.col("_schema_id").isin(ids),
            F.concat(F.lit("unapproved_schema:"), F.coalesce(F.col("_schema_id").cast("string"), F.lit("none"))),
        )
    rescued = F.when(F.col("_rescued_data").isNotNull(), F.lit("rescued_data"))
    checks = F.filter(F.array(schema_reason, rescued, *field_reason_exprs(contract)), lambda x: x.isNotNull())
    # Defence in depth: a "decoded" record with none of its key fields is not a record
    # (older bronze rows were written before bronze.decode learned this).
    key_missing = F.lit(True)
    for k in contract.key:
        key_missing = key_missing & F.col(f"payload.{k}").isNull()
    decoded = F.coalesce(F.col("decode_ok"), F.lit(False)) & ~key_missing
    # An undecodable record has no fields worth checking: report the cause (and the schema id).
    failed = F.filter(F.array(F.lit("decode_failed"), schema_reason), lambda x: x.isNotNull())
    return df.withColumn("reasons", F.when(decoded, checks).otherwise(failed))


def split(df: DataFrame, contract: Contract) -> tuple[DataFrame, DataFrame]:
    """(valid rows ready for silver, quarantine rows). Input must have `reasons`."""
    lineage = ["_schema_id", "_kafka_key", "_topic", "_partition", "_offset", "_source_file", "_bronze_ingest_ts"]
    valid = df.filter(F.size("reasons") == 0)
    w = Window.partitionBy(*[F.col(f"payload.{k}") for k in contract.key]).orderBy(F.col("_order").desc_nulls_last())
    valid = (
        valid.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select(
            "payload.*", *lineage, F.current_timestamp().alias("_silver_ts"), F.lit(contract.tag).alias("_contract")
        )
    )
    quarantine = df.filter(F.size("reasons") > 0).select(
        *[F.col(f"payload.{k}").alias(k) for k in contract.key],
        F.to_json("payload").alias("payload_json"),
        "reasons",
        "_raw_value",
        "_rescued_data",
        *lineage,
        F.current_timestamp().alias("_quarantined_at"),
        F.lit(contract.tag).alias("_contract"),
    )
    return valid, quarantine


def summarise(quarantine: DataFrame, contract: Contract, run_id: str) -> DataFrame:
    """Alert rows: how many records failed for each reason in this run."""
    # Undecodable records have no payload key: identify them by their Kafka key instead.
    no_key = F.lit(True)
    for k in contract.key:
        no_key = no_key & F.col(k).isNull()
    first_key = F.when(no_key, F.to_json(F.struct(F.col("_kafka_key").alias("kafka_key")))).otherwise(
        F.to_json(F.struct(*[F.col(k) for k in contract.key]))
    )
    return (
        quarantine.select(F.explode("reasons").alias("reason"), first_key.alias("_key"))
        .groupBy("reason")
        .agg(F.count("*").alias("records"), F.min("_key").alias("example_key"))
        .withColumn("contract", F.lit(contract.tag))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("raised_at", F.current_timestamp())
    )


# ---------------------------------------------------------------- job


def build_spark(app: str = "gmpdi-silver") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def merge_into_silver(spark: SparkSession, valid: DataFrame, path: str, key: tuple[str, ...]) -> None:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        valid.write.format("delta").mode("append").save(path)
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in key)
    DeltaTable.forPath(spark, path).alias("t").merge(valid.alias("s"), cond).whenNotMatchedInsertAll().execute()


def run_contract(spark: SparkSession, name: str, lakehouse: str, registry_url: str | None) -> dict:
    table, base_topic = CONTRACT_TABLES[name]
    contract = load_contract(name)
    approved = None
    if registry_url:
        subject = f"{base_topic}{_SUFFIX}-value"
        approved = approved_schema_ids(registry_url, subject, [SCHEMA_DIR / s for s in contract.approved_schemas])
        print(f"{contract.tag}: approved schema ids for {subject}: {sorted(approved) or 'none registered'}")

    totals = {"contract": contract.tag, "valid": 0, "quarantined": 0}

    def process(batch: DataFrame, epoch_id: int) -> None:
        df = with_reasons(standardise_kafka_bronze(batch), contract, approved).persist()
        valid, quarantine = split(df, contract)
        n_valid, n_bad = valid.count(), quarantine.count()
        if n_valid:
            merge_into_silver(spark, valid, f"{lakehouse}/silver/{table}", contract.key)
        if n_bad:
            quarantine.write.format("delta").mode("append").save(f"{lakehouse}/quarantine/{table}")
            run_id = f"{table}-{epoch_id}-{spark.sparkContext.applicationId}"
            alerts = summarise(quarantine, contract, run_id)
            alerts.write.format("delta").mode("append").option("mergeSchema", "true").save(
                f"{lakehouse}/alerts/contract_violations"
            )
            for r in alerts.orderBy(F.desc("records")).collect():
                print(f"  ALERT {contract.tag}: {r['records']} records -> {r['reason']} (e.g. {r['example_key']})")
        totals["valid"] += n_valid
        totals["quarantined"] += n_bad
        df.unpersist()

    bronze_path = f"{lakehouse}/bronze/{table}"
    q = (
        spark.readStream.format("delta")
        .load(bronze_path)
        .writeStream.foreachBatch(process)
        .option("checkpointLocation", f"{lakehouse}/_checkpoints/silver/{table}")
        .trigger(availableNow=True)
        .queryName(f"silver_{table}")
        .start()
    )
    q.awaitTermination()
    return totals


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--contract", default="all", choices=[*CONTRACT_TABLES, "all"])
    p.add_argument("--lakehouse", default=os.environ.get("LAKEHOUSE_DIR", "/lakehouse"))
    p.add_argument("--registry-url", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081"))
    p.add_argument("--no-schema-check", action="store_true", help="skip the approved-schema-version check")
    args = p.parse_args(argv)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    names = list(CONTRACT_TABLES) if args.contract == "all" else [args.contract]
    for n in names:
        t = run_contract(spark, n, args.lakehouse, None if args.no_schema_check else args.registry_url)
        print(f"{t['contract']}: {t['valid']} valid -> silver, {t['quarantined']} -> quarantine")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
