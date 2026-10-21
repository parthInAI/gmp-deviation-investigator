"""Nightly drift gate: re-prove that every kind of schema change is still caught.

Runs the Phase 2 gate on synthetic data in an isolated environment (topics
*.driftgate, lakehouse /lakehouse/driftgate), so it can never touch real data.
A code change that weakens a contract, the schema-id check or the decode check
turns this DAG red the next morning, not when real drift arrives.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from gmpdi_common import DEFAULT_ARGS, app_task, spark_task

DRIFT = {"GMPDI_TOPIC_SUFFIX": ".driftgate"}
LH = "/lakehouse/driftgate"

with DAG(
    dag_id="gmpdi_drift_gate",
    description="Inject 10 kinds of schema drift + 2 controls; fail if any reaches silver",
    schedule="15 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},  # a gate result should not be retried into a pass
    tags=["gmpdi", "phase2", "gate"],
) as dag:
    sample = app_task(
        "make_sample_data",
        ["streaming.sample_data", "--out", "out/drift/sample.csv", "--batches", "2", "--rows", "250"],
    )
    inject = app_task("inject_drift", ["drift.inject", "--csv", "out/drift/sample.csv", "--lakehouse", LH], env=DRIFT)
    bronze = spark_task("bronze", "bronze.py", ["--topic", "all", "--lakehouse", LH], env=DRIFT)
    silver = spark_task("silver", "silver.py", ["--contract", "all", "--lakehouse", LH], env=DRIFT)
    check = app_task("check_gate", ["drift.check"], env=DRIFT)
    reconcile = app_task(
        "reconcile", ["lakehouse.reconcile", "--lakehouse", LH, "--report", "out/quality/reconcile_driftgate.json"]
    )

    sample >> inject >> bronze >> silver >> [check, reconcile]
