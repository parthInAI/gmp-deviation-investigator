# GMP Deviation Investigator

[![ci](https://github.com/parthInAI/gmp-deviation-investigator/actions/workflows/ci.yml/badge.svg)](https://github.com/parthInAI/gmp-deviation-investigator/actions/workflows/ci.yml)
![Phase 1](https://img.shields.io/badge/phase_1-gate_passed-2ea043)
![Phase 2](https://img.shields.io/badge/phase_2-gate_passed-2ea043)
![License: MIT](https://img.shields.io/badge/license-MIT-blue)

**Phase 1 results:** 100/100 batches and 113,935/113,935 readings in bronze, locally and on Databricks. [Evidence →](docs/phase1/RESULTS.md)

**Phase 2 results:** 10 of 10 injected kinds of schema drift caught, 0 of 1,206 drifted records reached silver, and every bronze record is accounted for in silver or quarantine. [Evidence →](docs/phase2/RESULTS.md)

An agentic deviation-investigation assistant for biopharma manufacturing, built on a streaming lakehouse and designed against the **draft** EU GMP Annex 22 rules for AI.

It watches a simulated 100,000 L penicillin fermentation (IndPenSim) in near real time, flags abnormal batches with a static, versioned anomaly model, and hands them to a team of AI agents that draft a cited investigation report. A named QA reviewer approves or rejects every draft, and every step is audited. The LLMs only draft: they never detect, decide or act on the process.

> Simulated process data (IndPenSim) and synthetic quality records only. Not GMP-compliant, not validated, not for use in a real plant. Annex 22 is a consultation draft (consultation closed 7 Oct 2025; EMA targets final text in Q4 2026). Check its status before quoting it.

## Status

| Phase | Weeks | Gate | State |
|---|---|---|---|
| 1. Data foundation | 1–2 | All 100 batches replay into bronze | **Passed** on the real data, locally and on Databricks ([results](docs/phase1/RESULTS.md)) |
| 2. Lakehouse and orchestration | 3–4 | Injected schema changes are caught | **Passed**: 10/10 drift scenarios caught on the real data, in CI and nightly in Airflow ([results](docs/phase2/RESULTS.md)) |
| 3. Detection model | 5–6 | Faults caught on held-out batches | Not started |
| 4. Agent team | 7–8 | Cited draft for all 10 fault batches | Not started |
| 5. Review, deploy, evidence | 9–10 | Public repo and demo live | Not started |

## Architecture (Phases 1–2)

```
IndPenSim CSV ──► replayer ──► Redpanda (Kafka API) ──► Spark Structured Streaming ──► bronze Delta
 (100 batches)    Avro +        process.sensor.v1           availableNow / continuous      raw bytes + decoded payload
                  schema reg.                                                              │
openFDA API ────► recall producer ──► quality.fda_recall.v1 ─────────────────────────────►│
                                                                                            ▼
                          contracts/*.json ──► silver.py ──► silver   (valid, deduplicated, insert-only MERGE)
                                                         ├─► quarantine (payload, raw bytes, every reason)
                                                         └─► alerts   (records per reason, per run)
                                                gold.py ──► batch_summary · golden_profile · recall_summary
                                           reconcile.py ──► bronze = silver + duplicates + quarantine

Airflow 3: gmpdi_lakehouse (hourly)  ·  gmpdi_drift_gate (nightly: 10 kinds of drift + 2 controls)
Terraform: Databricks schema, volume, Git folder, job (infra/databricks)
```

Databricks Free Edition can't reach a broker on a laptop, so on Databricks the same records arrive as NDJSON in a Unity Catalog Volume and Auto Loader writes the bronze tables. Both paths use the same `.avsc` contracts.

## Run Phase 1 locally

Needs Docker Desktop (give it at least 6 GB RAM) and nothing else. Commands work the same in PowerShell, cmd or bash.

```bash
cp .env.example .env                       # PowerShell: copy .env.example .env
docker compose up -d --wait                # Redpanda + schema registry + Console (http://localhost:8080)
docker compose build app
```

**1. Try it on synthetic data first (about 2 minutes):**

```bash
docker compose run --rm app python -m streaming.sample_data --out out/sample/sample_indpensim.csv
docker compose run --rm app python -m streaming.registry register
docker compose run --rm app python -m streaming.replayer --csv out/sample/sample_indpensim.csv
docker compose run --rm spark                       # first run downloads the Kafka/Avro/Delta jars once
docker compose run --rm app python -m lakehouse.gate_check
```

**2. Then the real data.** Point `RAW_DATA_DIR` in `.env` at an empty folder outside OneDrive, then download it: `docker compose run --rm fetch-data`. This fetches `100_Batches_IndPenSim.zip` (504 MB) from [Mendeley Data](https://data.mendeley.com/datasets/pdnjz7zz5x/1) (CC BY 4.0), resumes if interrupted, checks the MD5 and unzips the 2.57 GB CSV into that folder. Check the column mapping before you replay anything:

```bash
docker compose run --rm app python -m streaming.replayer --csv data/raw/100_Batches_IndPenSim_V3.csv --inspect
```

Expect `header_repair: "merged split PAT/Raman legend at columns 33-35"`. The V3 file's header row has two more fields than its data, and the reader repairs that (see [results](docs/phase1/RESULTS.md)). `unknown_columns_dropped` and `contract_fields_missing_from_file` should both be empty. Then run the gate:

```bash
docker compose run --rm app python -m streaming.replayer --csv data/raw/100_Batches_IndPenSim_V3.csv
docker compose run --rm app python -m streaming.openfda_producer
docker compose run --rm spark
docker compose run --rm app python -m lakehouse.gate_check
```

The gate passes when all 100 batches are in bronze, every batch has exactly the readings the replayer sent, nothing failed to decode, and the fault batches seen in bronze match the ones sent. The report is saved to `out/gate/phase1_gate.json`.

Useful variations:

```bash
... streaming.replayer --csv ... --batches 91-100 --speed 3600   # fault batches, 1 h of plant time per second
docker compose run --rm spark --trigger continuous                # keep streaming into bronze
```

## Run Phase 2

After the Phase 1 steps (bronze is loaded), build the Spark image once (the Kafka, Avro and Delta jars are baked in, pinned by SHA-256):

```bash
docker compose build app spark
docker compose run --rm spark-silver                     # contracts -> silver / quarantine / alerts
docker compose run --rm spark-gold                       # gold tables
docker compose run --rm app python -m lakehouse.reconcile
```

**The Phase 2 gate.** It runs on isolated `*.driftgate` topics and `/lakehouse/driftgate`, so your real data is never touched (PowerShell: `$env:GMPDI_TOPIC_SUFFIX=".driftgate"`):

```bash
export GMPDI_TOPIC_SUFFIX=.driftgate
docker compose run --rm app python -m drift.inject --csv data/raw/100_Batches_IndPenSim_V3.csv
docker compose run --rm spark --topic all --lakehouse /lakehouse/driftgate
docker compose run --rm spark-silver --contract all --lakehouse /lakehouse/driftgate
docker compose run --rm app python -m drift.check      # GATE PASSED; report in out/gate/phase2_gate.json
unset GMPDI_TOPIC_SUFFIX
```

**Airflow:** `docker compose --profile airflow up -d --wait airflow`, then open http://localhost:8090 (no login on this local setup). Unpause `gmpdi_lakehouse` or `gmpdi_drift_gate`, or press Trigger.

**Databricks:** `lakehouse/databricks/silver_gold.py` runs the same silver/gold code on the Auto Loader bronze; `infra/databricks/` creates the schema, volume, Git folder and job with Terraform ([how](infra/databricks/README.md)).

## Working in VS Code

Open `gmp-deviation-investigator.code-workspace`. Every step above is a task: Ctrl+Shift+P → **Tasks: Run Task**.

- **Phase 1 gate on sample data (all steps)** runs the whole pipeline in order.
- **Phase 2: silver + gold + reconcile on REAL data** and **Phase 2 GATE: drift injection on REAL data (all steps)** run Phase 2; **Airflow: start** and **Open Airflow UI** for the DAGs.
- **LIVE DEMO: fault batch 94 streaming** replays fault batch 94 at 2 h of plant time per second while a second terminal shows each reading arriving on Kafka. Fault readings are marked `<< FAULT`.
- **Open Redpanda Console** shows topics, schemas and messages at http://localhost:8080.
- Tests appear in the Testing sidebar after **Setup: local Python venv for tests**.

## Bronze on Databricks (Free Edition)

1. Write the same records as files: `docker compose run --rm app python -m streaming.replayer --csv data/raw/<file>.csv --sink ndjson`
2. Clone this repo into a Databricks Git folder and open `lakehouse/databricks/bronze_autoloader.py`. Run its first cell to create the `workspace.gmpdi.landing` volume.
3. Upload `out/ndjson/process_sensor/` to `/Volumes/workspace/gmpdi/landing/process_sensor/`, using the Catalog UI or `databricks fs cp -r`.
4. Run the notebook. The last cell is the gate query.

## Development

```bash
pip install -e ".[lakehouse,spark,dev]"    # Python 3.11+; the Spark tests also need Java 17 or 21
ruff check . && pytest
```

CI runs the unit tests, Terraform `validate` and an Airflow DAG import check, then both gates end to end in Docker on synthetic data (`.github/workflows/ci.yml`).

## Repo layout

```
streaming/      replayer, openFDA producer, schema registry CLI, Avro schemas (schemas/*.avsc)
contracts/      data contracts: required fields, ranges, allowed values, formats
lakehouse/      bronze.py, silver.py, gold.py (Spark), gate_check.py + reconcile.py (delta-rs), databricks/ notebooks
drift/          schema-drift scenarios, injector and the Phase 2 gate check
orchestration/  Airflow DAGs and the Spark image entrypoint
infra/          Terraform for the Databricks objects
tests/          130 tests: unit, Spark (silver/gold parity with the Python reference), offline pipeline
docs/           design notes (NOTES.md) and gate results (RESULTS.md) per phase
```

Later phases add `models/`, `mcp_servers/`, `agents/`, `api/`, `ui/` and `evals/`, following the project plan.

## Data sources

- **IndPenSim**: Goldrick et al., "Modern day monitoring and control challenges outlined on an industrial-scale benchmark fermentation process", data on [Mendeley Data](https://data.mendeley.com/datasets/pdnjz7zz5x/1) (CC BY 4.0). Batches 1–30 are recipe-driven, 31–60 operator-controlled, 61–90 Raman/PAT-controlled, and 91–100 contain faults.
- **openFDA drug enforcement API**: public recall records, <https://open.fda.gov/apis/drug/enforcement/>.
