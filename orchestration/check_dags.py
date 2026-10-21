"""CI check: every DAG imports cleanly and has the expected tasks.

    docker run --rm -v ./orchestration:/opt/gmpdi:ro -e PYTHONPATH=/opt/gmpdi/dags \
      --entrypoint python apache/airflow:<version> /opt/gmpdi/check_dags.py

The scheduler puts the DAGs folder on sys.path itself; here PYTHONPATH does it.
"""

import sys

from airflow.dag_processing.dagbag import DagBag

EXPECTED = {
    "gmpdi_lakehouse": {"bronze", "silver", "gold", "reconcile"},
    "gmpdi_drift_gate": {"make_sample_data", "inject_drift", "bronze", "silver", "check_gate", "reconcile"},
}

bag = DagBag("/opt/gmpdi/dags")
if bag.import_errors:
    for path, err in bag.import_errors.items():
        print(f"IMPORT ERROR {path}\n{err}")
    sys.exit(1)
for dag_id, tasks in EXPECTED.items():
    dag = bag.dags.get(dag_id)
    assert dag is not None, f"missing DAG {dag_id}"
    got = {t.task_id for t in dag.tasks}
    assert got == tasks, f"{dag_id}: tasks {sorted(got)} != {sorted(tasks)}"
    assert dag.max_active_runs == 1, f"{dag_id}: must not run concurrently (single Delta writer)"
    print(f"{dag_id}: OK ({len(got)} tasks)")
