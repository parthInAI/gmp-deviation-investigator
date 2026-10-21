# Phase 2 design notes: lakehouse and orchestration

What each component does, why it is built this way, and the questions an interviewer is likely to ask about it. Same rule as Phase 1: don't say anything here that you can't defend without notes.

```
                       ┌── source manifest flags renamed / dropped / text columns (replayer, openFDA producer)
CSV / openFDA ─► producers ─► registry (BACKWARD) rejects incompatible schemas
                       │
                    Kafka ─► bronze (raw bytes + decode, decode_ok = all non-nullable fields present)
                                 │
                                 ▼
                   silver.py: contract check per record (Spark expressions, no UDFs)
                    ├─ valid ──► silver/<table>   (dedup on key, insert-only MERGE)
                    └─ invalid ► quarantine/<table> (payload, raw bytes, Kafka key, ALL reasons)
                                 + alerts/contract_violations (records per reason, per run)
                                 │
                   gold.py ──► batch_summary · golden_profile · recall_summary
                   reconcile ─► bronze = silver + duplicates + quarantine, unaccounted must be 0
```

---

## 1. Data contracts (`contracts/*.json`, `lakehouse/contracts.py`)

**What they do.** For each field: required or optional, numeric range, allowed values, or a format pattern. A record that breaks a rule gets a reason code such as `out_of_range:temperature_k`.

**Design choices**
- **Two layers of contract.** The `.avsc` says what *shape* a record has (types, names). The JSON contract says what *values* are plausible. Avro cannot tell you that 25.4 is a wrong temperature for a fermenter in kelvin. The contract can, and that's what catches S4, the drift that is invisible everywhere else.
- **Ranges come from the data, then get verified.** Every one of the 113,935 real readings passes, and the margin still rejects a unit change (°C values of ~23–29 against a 285 K minimum).
- **Online sensors required, offline assays optional.** The five lab assays are null in 111,873 of 113,935 rows by design (sampled every ~12 h). Making them required would quarantine 98% of good data.
- **JSON rather than YAML**, because the Spark image has no YAML parser and the contract must load identically in the Spark job, the app container and Databricks.
- **A pure-Python reference** (`check_record`) next to the Spark expressions. A parity test feeds the same good and drifted records to both and requires identical reasons, so the two can't drift apart.

**Likely questions**
- *Why not Great Expectations / dbt tests / DLT expectations?* Those test a table after it's written. Here each *record* is routed before it reaches silver, and the reasons travel with it. A small contract file plus Spark expressions does that with no extra platform, and it runs the same locally and on Databricks.
- *Why are reasons a list, not one code?* An investigator needs every rule a record broke. A record with a null pH *and* an impossible pat_ref is a different problem from one with only a null pH.

## 2. Silver (`lakehouse/silver.py`)

**What it does.** Streams each bronze table (availableNow), adds the reasons, and in `foreachBatch` sends valid records to silver and invalid ones to quarantine, plus an alert row per reason.

**Design choices**
- **Reasons as native Spark expressions, no Python UDFs.** They stay in the JVM: no serialisation per row, and the optimiser sees them.
- **Insert-only MERGE on the contract key** (`batch_id, seq` / `recall_number`), after keeping the latest offset per key within the micro-batch. A replay or a retried micro-batch can never duplicate silver. Re-running silver adds 0 rows.
- **Quarantine never deduplicates.** Every bad arrival is evidence: how often a broken producer sent, and when.
- **Quarantine keeps everything:** the payload as JSON, the raw Kafka bytes, the Kafka key, topic/partition/offset, and the reasons. A record can be re-processed after a fix, and traced even if it never decoded.
- **The approved-schema-id check.** Silver resolves which registry ids belong to the approved `.avsc` and quarantines anything else as `unapproved_schema:<id>`. Avro bodies are not self-describing, so a message written with a different schema decodes into *plausible-looking* values. The id in the message header is the only reliable signal (see RESULTS: S5 decoded perfectly).
- **The same functions run on Databricks** (`lakehouse/databricks/silver_gold.py`). Only the I/O differs: UC tables, no `persist()`, no `sparkContext`, because serverless compute has neither.

**Likely questions**
- *Exactly-once?* Bronze → silver is effectively once: the checkpoint gives at-least-once micro-batch replay, and the keyed insert-only MERGE makes a replayed batch a no-op for silver. Quarantine and alerts are append-only, so a crash mid-batch can duplicate a quarantine row. Reconciliation counts positions, not rows, so that cannot hide a loss.
- *Why does a quarantined record not fail the pipeline?* Quarantine *is* the control working. Failing the run would stop good data behind one bad producer. The run fails only when a record is unaccounted for.

## 3. Bronze, revisited (`lakehouse/bronze.py`)

**What changed.** `decode_ok` now requires every field that the Avro schema declares non-nullable to be present, not just a non-null payload.

**Why.** Spark 4's PERMISSIVE `from_avro` returns a struct of nulls, not null, when it cannot decode. Phase 1 missed it because Phase 1 never sent a bad message. The drift gate's S6 (a dropped field, which the registry accepted) produced 200 all-null rows marked as decoded.

**Likely question.** *How did you find it?* By injecting the failure on purpose. The existing gate couldn't see it: undecoded = 0 was true for the real data and meaningless for bad data.

## 4. Drift harness and the gate (`drift/`)

**What it does.** `drift.inject` sends 10 kinds of drift and 2 controls into an isolated copy of the pipeline. `drift.check` looks up every injected key in silver, quarantine and alerts.

**Design choices**
- **Three entry points, because drift enters in three places:** the source file (S1–S4), a producer's Avro schema (S5–S7) and the openFDA API (R1–R3). Each scenario states where it should be caught.
- **Pass means all of it:** 0 records in silver, *every* record in quarantine with an expected reason, *and* an alert raised. Controls must land fully in silver with nothing quarantined, so a contract that rejects everything fails the gate too.
- **Isolation by topic suffix.** `GMPDI_TOPIC_SUFFIX=.driftgate` gives separate topics, registry subjects and lakehouse folder. `--reset` refuses to run without a suffix, so the harness can't delete real topics.
- **Keys that cannot collide:** sensor batches 9001+, recall numbers `D-9xxx-2026`.
- **Defence in depth is visible.** S1–S3 are already flagged in the replay manifest (unknown column, missing field, non-numeric values nulled). S4 is caught only by the contract range. S7 never gets past the registry.
- **The gate logic is unit-tested**, including that one leaked record, a missing alert or a quarantined control each fail it.

**Likely questions**
- *Isn't 100% caught easy if you write the tests?* The scenarios aren't hypothetical: S5 and S6 are changes the registry *accepts*, and S6 broke my own bronze code. The value is that each one is now a regression test that runs every night and in CI.
- *What drift is not covered?* Semantic drift within range: a sensor slowly going out of calibration while staying plausible. That's a model problem (Phase 3, golden-batch envelope), not a schema problem.

## 5. Gold (`lakehouse/gold.py`)

**What it does.** Three tables, each rebuilt from silver on every run:
- `batch_summary`: per batch, campaign, length, fault onset, final titre and the min/mean/max of the key variables.
- `golden_profile`: p10/p50/p90 per process hour across normal batches only.
- `recall_summary`: recalls per month and class, and how many cite CGMP.

**Design choices**
- **Full rebuild, not incremental.** ~114k rows rebuild in seconds, and a full rebuild is idempotent by construction. Incremental gold adds state to get wrong for no gain at this size. The trade-off flips at a scale this project doesn't have.
- **The campaign comes from the flags** (`pat_ref`, `control_ref`, `fault_flag`), not batch-number ranges, so the labels stay right if the data changes.
- **`low_support` on the golden profile.** Batches end between ~200 and 290 h, so the late hours rest on a handful of batches (hour 290: 1). Phase 3 must not judge a batch against an envelope built from fewer than 20.
- **Both timestamp encodings accepted.** Avro decodes `event_time` as a TIMESTAMP, NDJSON as epoch millis. Found when gold first ran on real Kafka data. A test covers both.

**Likely question.** *What did gold tell you?* Raman/PAT-controlled batches end at the highest mean titre (29.4 g/L), against 24.3 recipe-driven and 21.5 operator-controlled. Raman/PAT is the most advanced control strategy in IndPenSim, so that ordering is plausible, but it's a descriptive observation from 30 batches per group, not a causal claim. It falls out of a group-by over clean data.

## 6. Reconciliation (`lakehouse/reconcile.py`)

**What it does.** For every bronze record (by topic/partition/offset): in quarantine, in silver, or a duplicate of a silver key. `unaccounted` must be 0, or the task exits 1.

**Why it matters.** ALCOA+ asks for data that is *complete*. "No error was logged" isn't evidence of completeness; a count that closes is. It caught a real failure during the build: when the silver job crashed, reconcile reported all 113,935 records unaccounted instead of letting gold build on an empty silver.

## 7. Orchestration (`orchestration/`, Airflow 3.3.2)

**What it does.** Two DAGs: `gmpdi_lakehouse` (hourly: bronze → silver → gold + reconcile) and `gmpdi_drift_gate` (nightly: the full Phase 2 gate on synthetic data).

**Design choices**
- **Every task is a DockerOperator running the project's own images.** Airflow orchestrates only. The code it runs is the same image you run with `docker compose run`, and Airflow needs none of the pipeline's dependencies (no PySpark in the scheduler).
- **Named volumes, no bind mounts, for task containers.** A path inside the Airflow container means nothing to the Docker daemon on the host. This is the classic Docker-in-Docker trap.
- **Producers are not tasks.** They are streams. Each DAG run catches the lakehouse up with whatever is on the topics (availableNow + checkpoints), so a late or retried run does the right amount of work. `catchup=False` and `max_active_runs=1`, because streaming checkpoints and MERGE want one writer.
- **The drift gate has `retries=0`.** A gate should not be retried into a pass.
- **The Spark image is immutable.** The Kafka, Avro and Delta jars are baked in, each pinned by SHA-256, instead of `--packages` downloading them on every run. A job no longer depends on Maven Central, and two runs can't get different jars. (Side effect found and fixed: `--packages` used to put the Delta Python API on the path, so the entrypoint now passes the Delta jar as `--py-files`.)
- **CI checks the DAGs import** and have the expected tasks (`orchestration/check_dags.py`) without running a scheduler.

**Likely questions**
- *Why not KubernetesPodOperator?* Same idea; there's no Kubernetes here. DockerOperator is the local equivalent, and moving to KPO changes the operator, not the images.
- *Why standalone mode and SQLite?* It's a single-machine dev setup, and the compose file says so. Production would be Postgres plus a separate scheduler and API server (or a managed Airflow).

## 8. Terraform for Databricks (`infra/databricks/`)

**What it does.** Creates the schema, the landing volume, a Git folder pinned to `main`, and the `gmpdi-lakehouse` job (bronze Auto Loader → silver/gold notebook on serverless, hourly schedule created paused).

**Design choices**
- **Credentials only from the environment** (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`); nothing secret in the repo.
- **Adopt, don't duplicate.** Phase 1 created the schema and volume by hand, so the README imports them before the first apply.
- **A separate deploy Git folder.** The job runs the pinned branch, not whatever is checked out in your dev folder.
- **The lock file has provider hashes for Windows, Linux and macOS**, and CI runs `fmt -check` and `validate` with no credentials.

**Likely question.** *Is local state OK?* For one person on Free Edition, yes, and it's git-ignored. A team needs a remote backend with locking.

## Verified

- 130 tests, including Spark tests of silver/gold and a parity test between the Spark reasons and the Python reference.
- The gate on the real data: [RESULTS.md](RESULTS.md).
- The exact CI job rehearsed locally on a fresh stack: both gates pass on synthetic data.
- Both DAGs green in Airflow 3.3.2. `terraform validate` passes against provider 1.138.0.

- `terraform apply` on Databricks Free Edition (2 added, 2 changed, 0 destroyed), and the job's silver/gold run matched the local numbers exactly.
