# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion on Databricks (Auto Loader)
# MAGIC
# MAGIC Databricks Free Edition is serverless-only and limits outbound internet to trusted domains,
# MAGIC so it cannot consume from the Redpanda broker on a laptop. Instead, the replayer writes the
# MAGIC **same records** as NDJSON (`--sink ndjson`), you upload them to a Unity Catalog Volume, and
# MAGIC Auto Loader streams them into bronze Delta tables.
# MAGIC
# MAGIC Same contract as the local path: the Spark schema is generated from the `.avsc` files in
# MAGIC `streaming/schemas`. Anything that does not fit lands in `_rescued_data` instead of being
# MAGIC dropped, which is the hook for Phase 2's drift checks.
# MAGIC
# MAGIC Run this notebook from a Git folder (Repos) so `lakehouse/` and `streaming/` are importable.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "gmpdi")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
base = f"/Volumes/{catalog}/{schema}/{volume}"
print("Upload NDJSON to:", base + "/process_sensor/  and  " + base + "/fda_recall/")

# COMMAND ----------

import os
import sys
from pathlib import Path

repo_root = Path(os.getcwd()).parents[1]  # notebook lives in <repo>/lakehouse/databricks
if str(repo_root) not in sys.path:
    sys.path.append(str(repo_root))

from pyspark.sql import functions as F

from lakehouse.schema_utils import avro_to_ddl

SOURCES = {
    # landing folder -> (contract, bronze table)
    "process_sensor": ("sensor_reading.avsc", "bronze_process_sensor"),
    "fda_recall": ("recall_event.avsc", "bronze_fda_recall"),
}

# COMMAND ----------


def ingest(folder: str, schema_file: str, table: str):
    ddl = avro_to_ddl(repo_root / "streaming" / "schemas" / schema_file)
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # unexpected fields -> _rescued_data
        .option("rescuedDataColumn", "_rescued_data")
        .schema(ddl)
        .load(f"{base}/{folder}")
    )
    payload_cols = [c for c in stream.columns if c != "_rescued_data"]
    bronze = stream.select(
        F.struct(*payload_cols).alias("payload"),
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingest_ts"),
    ).withColumn("decode_ok", F.col("_rescued_data").isNull())
    return (
        bronze.writeStream.option("checkpointLocation", f"{base}/_checkpoints/{table}")
        .trigger(availableNow=True)  # serverless supports availableNow triggers
        .toTable(f"{catalog}.{schema}.{table}")
    )


queries = [ingest(folder, sf, tbl) for folder, (sf, tbl) in SOURCES.items()]
for q in queries:
    q.awaitTermination()

# COMMAND ----------

# MAGIC %md ## Phase 1 gate, Databricks side

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT
      count(DISTINCT payload.batch_id)                               AS batches,
      count(*)                                                       AS readings,
      count(*) - count(DISTINCT payload.batch_id, payload.seq)       AS duplicate_rows,
      sum(CASE WHEN decode_ok THEN 0 ELSE 1 END)                     AS rows_with_rescued_data,
      array_sort(collect_set(CASE WHEN payload.fault_ref > 0 THEN payload.batch_id END)) AS fault_batches
    FROM {catalog}.{schema}.bronze_process_sensor
""")
)
