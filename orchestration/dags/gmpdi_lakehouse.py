"""Hourly lakehouse refresh: Kafka -> bronze -> silver -> gold, then reconcile.

The producers (replayer, openFDA) are streams, not batch jobs, so they are not
tasks here. Each run catches the lakehouse up with whatever is on the topics
(availableNow triggers + checkpoints), so a run that is late or retried does
the right amount of work, and a run with nothing new is cheap.

The reconcile task fails the run if any bronze record is unaccounted for in
silver or quarantine. Quarantined records do NOT fail the run: they are the
contract working, and they are visible in the alerts table.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG
from gmpdi_common import DEFAULT_ARGS, app_task, spark_task

with DAG(
    dag_id="gmpdi_lakehouse",
    description="Kafka -> bronze -> silver (contracts) -> gold, then reconcile",
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 10, 1),
    catchup=False,  # each run processes everything new; backfilling old hours adds nothing
    max_active_runs=1,  # Delta MERGE and streaming checkpoints: one writer at a time
    default_args=DEFAULT_ARGS,
    tags=["gmpdi", "phase2"],
) as dag:
    bronze = spark_task("bronze", "bronze.py", ["--topic", "all"])
    silver = spark_task("silver", "silver.py", ["--contract", "all"])
    gold = spark_task("gold", "gold.py")
    reconcile = app_task("reconcile", ["lakehouse.reconcile", "--lakehouse", "/lakehouse"])

    bronze >> silver >> [gold, reconcile]
