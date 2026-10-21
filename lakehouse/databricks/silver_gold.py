# Databricks notebook source
# MAGIC %md
# MAGIC # Silver and gold on Databricks
# MAGIC
# MAGIC Same transforms as the local Spark jobs: `lakehouse/silver.py` (data contracts, quarantine,
# MAGIC alerts) and `lakehouse/gold.py` are imported, not copied. Only the I/O differs: Unity Catalog
# MAGIC tables instead of Delta paths, and no `persist()`/`sparkContext`, which serverless compute
# MAGIC does not offer.
# MAGIC
# MAGIC Input is the Auto Loader bronze from `bronze_autoloader.py`. There are no schema-registry ids
# MAGIC on this path, so the schema check is Auto Loader's `_rescued_data` (anything that does not fit
# MAGIC the `.avsc`) plus the same field rules from `contracts/*.json`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "gmpdi")
dbutils.widgets.text("volume", "landing")
catalog, schema, volume = (dbutils.widgets.get(k) for k in ("catalog", "schema", "volume"))
base = f"/Volumes/{catalog}/{schema}/{volume}"

import os
import sys
from pathlib import Path

repo_root = Path(os.getcwd()).parents[1]  # notebook lives in <repo>/lakehouse/databricks
if str(repo_root) not in sys.path:
    sys.path.append(str(repo_root))
os.environ["GMPDI_CONTRACT_DIR"] = str(repo_root / "contracts")

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from lakehouse import gold, silver
from lakehouse.contracts import load_contract


def t(name: str) -> str:
    return f"{catalog}.{schema}.{name}"


# COMMAND ----------

# MAGIC %md ## Silver: contracts, quarantine, alerts

# COMMAND ----------


def run_silver(contract_name: str, table: str):
    contract = load_contract(contract_name)

    def process(batch, epoch_id):
        df = silver.with_reasons(silver.standardise_file_bronze(batch), contract, approved_ids=None)
        valid, quarantine = silver.split(df, contract)
        if not valid.isEmpty():
            if spark.catalog.tableExists(t(f"silver_{table}")):
                cond = " AND ".join(f"t.{k} = s.{k}" for k in contract.key)
                (
                    DeltaTable.forName(spark, t(f"silver_{table}"))
                    .alias("t")
                    .merge(valid.alias("s"), cond)
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                valid.write.saveAsTable(t(f"silver_{table}"))
        if not quarantine.isEmpty():
            quarantine.write.mode("append").saveAsTable(t(f"quarantine_{table}"))
            alerts = silver.summarise(quarantine, contract, f"{table}-{epoch_id}")
            alerts.write.mode("append").option("mergeSchema", "true").saveAsTable(t("alerts_contract_violations"))

    return (
        spark.readStream.table(t(f"bronze_{table}"))
        .writeStream.foreachBatch(process)
        .option("checkpointLocation", f"{base}/_checkpoints/silver_{table}")
        .trigger(availableNow=True)
        .start()
    )


for name, (table, _topic) in silver.CONTRACT_TABLES.items():
    run_silver(name, table).awaitTermination()

# COMMAND ----------

# MAGIC %md ## Gold

# COMMAND ----------

sensor = spark.table(t("silver_process_sensor"))
gold.batch_summary(sensor).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_batch_summary"))
gold.golden_profile(sensor).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    t("gold_golden_profile")
)
if spark.catalog.tableExists(t("silver_fda_recall")):
    gold.recall_summary(spark.table(t("silver_fda_recall"))).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(t("gold_recall_summary"))

# COMMAND ----------

# MAGIC %md ## Reconciliation: every bronze record is in silver or quarantine

# COMMAND ----------

q_exists = spark.catalog.tableExists(t("quarantine_process_sensor"))
display(
    spark.sql(f"""
    SELECT
      (SELECT count(*) FROM {t('bronze_process_sensor')})                                   AS bronze_rows,
      (SELECT count(*) FROM {t('silver_process_sensor')})                                   AS silver_rows,
      {f"(SELECT count(*) FROM {t('quarantine_process_sensor')})" if q_exists else "0"}     AS quarantined,
      (SELECT count(*) FROM {t('gold_batch_summary')})                                      AS gold_batches,
      (SELECT count(*) FROM {t('gold_batch_summary')} WHERE campaign = 'fault')             AS fault_batches
""")
)
