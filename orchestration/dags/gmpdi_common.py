"""Shared settings for the GMPDI DAGs.

Every task is a DockerOperator that starts one of the project's own images
(gmpdi-app for Python tools, gmpdi-spark for Spark jobs) on the compose network.
Airflow only orchestrates; it never runs pipeline code in its own process. So
the code that Airflow runs is byte-for-byte the code you run with
`docker compose run`, and Airflow needs none of the pipeline's dependencies.

Containers share data through named Docker volumes only (no host bind mounts),
because a bind-mount path from inside the Airflow container means nothing to
the Docker daemon on the host.
"""

from __future__ import annotations

import os
from datetime import timedelta

from docker.types import Mount

PROJECT = os.environ.get("GMPDI_COMPOSE_PROJECT", "gmpdi")
NETWORK = os.environ.get("GMPDI_DOCKER_NETWORK", f"{PROJECT}_default")
APP_IMAGE = os.environ.get("GMPDI_APP_IMAGE", "gmpdi-app:dev")
SPARK_IMAGE = os.environ.get("GMPDI_SPARK_IMAGE", "gmpdi-spark:dev")

LAKEHOUSE = Mount(target="/lakehouse", source=f"{PROJECT}_lakehouse", type="volume")
OUT = Mount(target="/app/out", source=f"{PROJECT}_pipeline-out", type="volume")

BASE_ENV = {
    "KAFKA_BOOTSTRAP": "redpanda:9092",
    "SCHEMA_REGISTRY_URL": "http://redpanda:8081",
    "LAKEHOUSE_DIR": "/lakehouse",
}

DEFAULT_ARGS = {
    "owner": "gmpdi",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def docker_task(task_id: str, image: str, command: list[str], env: dict | None = None, mounts=None, **kw):
    from airflow.providers.docker.operators.docker import DockerOperator

    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment={**BASE_ENV, **(env or {})},
        network_mode=NETWORK,
        mounts=mounts or [LAKEHOUSE],
        user="root",  # named volumes are root-owned
        docker_url=os.environ.get("DOCKER_HOST", "unix://var/run/docker.sock"),
        auto_remove="success",  # keep failed containers for `docker logs`
        mount_tmp_dir=False,
        tty=False,
        **kw,
    )


def spark_task(task_id: str, job: str, args: list[str] | None = None, env: dict | None = None, **kw):
    return docker_task(task_id, SPARK_IMAGE, args or [], env={"JOB": job, **(env or {})}, **kw)


def app_task(task_id: str, module_args: list[str], env: dict | None = None, **kw):
    return docker_task(task_id, APP_IMAGE, ["python", "-m", *module_args], env=env, mounts=[LAKEHOUSE, OUT], **kw)
