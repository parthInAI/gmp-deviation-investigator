# GMP Deviation Investigator

An agentic deviation-investigation assistant for biopharma manufacturing, built on a streaming lakehouse and designed against the **draft** EU GMP Annex 22 rules for AI.

It watches a simulated 100,000 L penicillin fermentation (IndPenSim) in near real time, flags abnormal batches with a static, versioned anomaly model, and hands them to a team of AI agents that draft a cited investigation report. A named QA reviewer approves or rejects every draft, and every step is audited. The LLMs only draft: they never detect, decide or act on the process.

> Simulated process data (IndPenSim) and synthetic quality records only. Not GMP-compliant, not validated, not for use in a real plant. Annex 22 is a consultation draft (consultation closed 7 Oct 2025; EMA targets final text in Q4 2026). Check its status before quoting it.

## Status

| Phase | Weeks | Gate | State |
|---|---|---|---|
| 1. Data foundation | 1–2 | All 100 batches replay into bronze | **Passed** on the real data ([results](docs/phase1/RESULTS.md)) |
| 2. Lakehouse and orchestration | 3–4 | Injected schema changes are caught | Not started |
| 3. Detection model | 5–6 | Faults caught on held-out batches | Not started |
| 4. Agent team | 7–8 | Cited draft for all 10 fault batches | Not started |
| 5. Review, deploy, evidence | 9–10 | Public repo and demo live | Not started |

## Phase 1 architecture

```
IndPenSim CSV ──► replayer ──► Redpanda (Kafka API) ──► Spark Structured Streaming ──► bronze Delta
 (100 batches)    Avro +        topic process.sensor.v1      availableNow / continuous      raw bytes + decoded payload
                  schema reg.   key = batch_id                                              decode_ok flag
openFDA API ────► recall producer ──► topic quality.fda_recall.v1 ──────────────────────► bronze Delta
                                                                                              │
                                                     gate_check (delta-rs) ◄── replay manifest ┘
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

## Working in VS Code

Open `gmp-deviation-investigator.code-workspace`. Every step above is a task: Ctrl+Shift+P → **Tasks: Run Task**.

- **Phase 1 gate on sample data (all steps)** runs the whole pipeline in order.
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

CI runs the unit tests, then the whole Phase 1 gate end to end in Docker on synthetic data (`.github/workflows/ci.yml`).

## Repo layout (Phase 1)

```
streaming/      replayer, openFDA producer, schema registry CLI, Avro contracts (schemas/*.avsc)
lakehouse/      bronze.py (Spark), gate_check.py (delta-rs), databricks/ notebook
tests/          66 tests; test_pipeline_offline.py runs the gate on 100 synthetic batches
docs/phase1/    design notes (NOTES.md) and gate results (RESULTS.md)
```

Later phases add `orchestration/`, `models/`, `mcp_servers/`, `agents/`, `api/`, `ui/`, `evals/` and `infra/`, following the project plan.

## Data sources

- **IndPenSim**: Goldrick et al., "Modern day monitoring and control challenges outlined on an industrial-scale benchmark fermentation process", data on [Mendeley Data](https://data.mendeley.com/datasets/pdnjz7zz5x/1) (CC BY 4.0). Batches 1–30 are recipe-driven, 31–60 operator-controlled, 61–90 Raman/PAT-controlled, and 91–100 contain faults.
- **openFDA drug enforcement API**: public recall records, <https://open.fda.gov/apis/drug/enforcement/>.
