"""Kafka -> bronze Delta, with Spark Structured Streaming.

    spark-submit --packages <kafka,avro,delta> lakehouse/bronze.py --topic all

Bronze rule: keep what arrived, add where it came from, never drop a record.
Each row stores the raw Kafka bytes alongside the decoded Avro payload, so:
  * a message that fails to decode is still kept (decode_ok = false), not lost;
  * if the schema changes later, bronze can be re-decoded from raw_value
    without re-reading Kafka.
Cleaning, contracts and quarantine happen in silver (Phase 2), not here.

Default trigger is availableNow: read everything currently on the topic, write
it, stop. Same code also runs continuously with --trigger continuous.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

# topic -> (Avro schema file, bronze table name)
BRONZE_TABLES: dict[str, tuple[str, str]] = {
    "process.sensor.v1": ("sensor_reading.avsc", "process_sensor"),
    "quality.fda_recall.v1": ("recall_event.avsc", "fda_recall"),
}

# Confluent wire format: 1 magic byte (0x00) + 4-byte big-endian schema id + Avro body.
WIRE_HEADER_BYTES = 5


def schema_id_col(value: Column) -> Column:
    """Schema registry id from the wire header; null if the header is not valid."""
    has_header = (F.length(value) > WIRE_HEADER_BYTES) & (F.hex(F.substring(value, 1, 1)) == "00")
    sid = F.conv(F.hex(F.substring(value, 2, 4)), 16, 10).cast("int")
    return F.when(has_header, sid)


def kafka_envelope(df: DataFrame) -> DataFrame:
    """Kafka source columns -> bronze metadata columns (no decoding yet)."""
    return df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        schema_id_col(F.col("value")).alias("schema_id"),
        F.col("value").alias("raw_value"),
        F.current_timestamp().alias("ingest_ts"),
    ).withColumn("ingest_date", F.to_date("ingest_ts"))


def decode(df: DataFrame, avro_schema_json: str) -> DataFrame:
    """Add payload (decoded struct) and decode_ok. PERMISSIVE: bad bytes -> null, not a crash."""
    from pyspark.sql.avro.functions import from_avro  # needs the spark-avro package

    body = F.expr(f"substring(raw_value, {WIRE_HEADER_BYTES + 1})")
    payload = from_avro(body, avro_schema_json, {"mode": "PERMISSIVE"})
    return df.withColumn("payload", F.when(F.col("schema_id").isNotNull(), payload)).withColumn(
        "decode_ok", F.col("payload").isNotNull()
    )


def build_spark(app: str = "gmpdi-bronze") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def start_stream(
    spark: SparkSession,
    topic: str,
    bootstrap: str,
    lakehouse: str,
    schema_dir: Path,
    trigger: str,
    max_offsets: int,
):
    schema_file, table = BRONZE_TABLES[topic]
    avro_schema = (schema_dir / schema_file).read_text()
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )
    bronze = decode(kafka_envelope(source), avro_schema)
    writer = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{lakehouse}/_checkpoints/bronze/{table}")
        .partitionBy("ingest_date")
        .queryName(f"bronze_{table}")
    )
    writer = writer.trigger(availableNow=True) if trigger == "available-now" else writer.trigger(
        processingTime="10 seconds"
    )
    return writer.start(f"{lakehouse}/bronze/{table}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--topic", default="all", help=f"one of {list(BRONZE_TABLES)} or 'all'")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
    p.add_argument("--lakehouse", default=os.environ.get("LAKEHOUSE_DIR", "/lakehouse"))
    p.add_argument("--schema-dir", type=Path, default=Path(__file__).resolve().parent.parent / "streaming" / "schemas")
    p.add_argument("--trigger", choices=["available-now", "continuous"], default="available-now")
    p.add_argument("--max-offsets-per-trigger", type=int, default=100_000)
    args = p.parse_args(argv)

    topics = list(BRONZE_TABLES) if args.topic == "all" else [args.topic]
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    queries = [
        start_stream(
            spark, t, args.bootstrap, args.lakehouse, args.schema_dir, args.trigger, args.max_offsets_per_trigger
        )
        for t in topics
    ]
    for q in queries:
        q.awaitTermination()
        total = sum(int(p.get("numInputRows", 0)) for p in q.recentProgress)
        print(f"{q.name}: finished, {total} rows in {len(q.recentProgress)} micro-batches")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
