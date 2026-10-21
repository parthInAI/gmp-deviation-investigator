#!/bin/sh
# Entrypoint of the gmpdi-spark image: run lakehouse/$JOB with spark-submit.
# Arguments after the image/service name go to the job, e.g.
#   docker compose run --rm spark --trigger continuous
#   JOB=silver.py ... --contract fda_recall
# --py-files: the Delta Python API (delta.tables) ships inside the Delta jar.
# --packages used to put it on the Python path; with baked-in jars we do it here.
set -e
exec /opt/spark/bin/spark-submit \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  --conf spark.log.level=WARN \
  --conf spark.ui.showConsoleProgress=false \
  --py-files /opt/spark/jars/delta-spark_2.13-4.0.1.jar \
  "/opt/app/lakehouse/${JOB:?set JOB to bronze.py, silver.py or gold.py}" "$@"
