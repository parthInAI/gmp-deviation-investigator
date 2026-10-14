# Phase 1 design notes: data foundation

What each component does, why it is built this way, and the questions an interviewer is likely to ask about it. Read this before you push. Every claim here should be something you can defend without notes.

---

## 1. IndPenSim reader (`streaming/indpensim.py`)

**What it does.** It streams the 2.5 GB CSV in 20,000-row chunks, maps the long headers to stable snake_case names, drops the ~2,200 Raman columns, and splits the rows into batches.

**Design choices**
- **Header mapping by short code.** `"Aeration rate(Fg:L/h)"` maps to `Fg` and then to `aeration_rate_lph`. The code inside the parentheses is the stable part of each header; the surrounding text varies between dataset versions. Units go into the canonical name so nobody has to guess them downstream.
- **Raman columns are never parsed.** `usecols` skips them at read time, which is what makes the 2.5 GB file workable. The spectra belong on their own topic later; they are not a sensor tag.
- **Header repair.** The real V3 file's header row has two more fields than its data rows, because one legend was split into three cells. `repair_headers` fixes that known defect and refuses any other mismatch. See RESULTS.md for how it was found.
- **Batch splitting.** The batch-ID column is used only if every ID change coincides with the time column resetting and IDs increase. Without an ID column, the time resets alone define the batches. State carries across chunk boundaries (tested at chunk sizes 7, 25 and 1,000).
- **A deterministic plant clock.** Batch *k* starts at 2023-01-01 + (k−1)×240 h. A re-replay therefore produces identical event times, which silver needs for deduplication.

**Likely questions**
- *Why not load it into pandas in one go?* 2.5 GB of CSV is roughly 10 GB or more in memory once parsed. Chunked reading keeps memory flat and is the same pattern you would use for a historian export.
- *What happens if a new column appears?* It shows up in `unknown_columns_dropped`; it is never silently added to the contract. Phase 2 turns that into a drift alert.
- *Did the real data have problems?* Yes. The header was misaligned by two columns, so a column literally called "Batch ID" held Raman intensities. A plausible-looking report from `--inspect` didn't catch it; cross-checking two independent signals (the ID column against the time resets) did. That check now runs on every replay.

## 2. Avro contracts and the schema registry (`streaming/schemas/*.avsc`, `streaming/registry.py`)

**What it does.** The two `.avsc` files are the contracts for the two topics. `registry register` sets BACKWARD compatibility and registers both schemas.

**Design choices**
- **Producers cannot auto-register** (`auto.register.schemas=False`). Only a deliberate registration step can create a schema version, and a producer with an unregistered schema fails fast (tested). In GMP terms this is change control applied to a data contract.
- **BACKWARD compatibility**, because the lakehouse replays history from the start of a topic: a reader on the new schema must still read old data. This is also why every optional field is `["null", X]` with `default: null` (tested).
- **A test pins code and contract together.** If someone adds a field in code but not in the `.avsc` file (or the other way round), CI fails.
- **Redpanda's built-in schema registry** speaks the Confluent API, so there is one fewer container and the client code is standard Confluent code.

**Likely questions**
- *Why Avro rather than JSON?* A typed, enforced schema, compact binary, and evolution rules that the registry checks. JSON has none of those.
- *BACKWARD versus FORWARD versus FULL?* BACKWARD lets consumers upgrade first, which suits a lakehouse that re-reads old data. FULL is stricter: you would choose it when producers and consumers deploy independently.

## 3. Replayer (`streaming/replayer.py`, `streaming/sinks.py`)

**What it does.** It sends each reading to `process.sensor.v1`, keyed by `batch_id`, and writes a manifest (rows per batch, fault batches, events per second).

**Design choices**
- **Key = `batch_id`**, so all of a batch lands on one partition and stays in order. Six partitions allow parallel consumers across batches.
- **An idempotent producer** (`enable.idempotence`, `acks=all`), so broker retries cannot duplicate or reorder messages within a partition.
- **`--speed`** compresses the simulated clock. `0` means as fast as possible (used for the gate); `720` sends one reading per batch per second. This is how the plan's 1×/10×/100× throughput metric gets measured.
- **The manifest is the source of truth for the gate.** The gate compares bronze against what was sent, filtered by `replay_id`, so an old test replay in the same table cannot fake a pass.
- **The NDJSON sink** exists only for Databricks Free Edition, which cannot reach a laptop broker.

**Likely questions**
- *Is it exactly-once?* Producer to broker: no duplicates from retries, thanks to idempotence. Broker to Delta: Spark checkpoints plus Delta's transactional sink give exactly-once writes per query. End to end, replaying the same file twice *does* produce duplicates, by design: silver deduplicates on `(batch_id, seq)`. Being able to explain where duplicates can come from is the strong answer.

## 4. openFDA recall producer (`streaming/openfda_producer.py`)

**What it does.** It pages through `drug/enforcement` by `report_date`, keeps a watermark in a state file, skips records with no `recall_number` and in-page duplicates, and keys messages by recall number.

**Why it exists.** It is a real, messy external feed: empty strings, missing nested `openfda` blocks, a 25,000-record skip cap. Phase 2 uses it as the topic for injected schema drift.

**Likely questions**
- *The API caps `skip` at 25,000. What then?* Narrow the date window. The incremental watermark already keeps normal runs small.

## 5. Bronze (`lakehouse/bronze.py`, `lakehouse/databricks/bronze_autoloader.py`)

**What it does.** Spark Structured Streaming reads Kafka and writes Delta, keeping the raw bytes, Kafka metadata, the schema ID parsed from the wire header, the decoded payload, and a `decode_ok` flag.

**Design choices**
- **Never drop at bronze.** A message that fails to decode is kept with `decode_ok = false`. Quarantine and cleaning are silver's job (Phase 2). The offline pipeline test injects a poisoned message and checks that the gate catches it.
- **Raw bytes are kept.** `from_avro` in open-source Spark decodes with a fixed reader schema. If the writer schema evolves, bronze can be re-decoded from `raw_value` without re-reading Kafka. This is a deliberate trade-off against storage.
- **`availableNow` is the default trigger.** It processes everything currently on the topic, then stops. It suits batch-style runs, it is the trigger Databricks serverless supports, and `--trigger continuous` reuses the same code path.
- **Delta tables live in a Docker volume**, not your OneDrive folder, so OneDrive never syncs thousands of small Parquet and log files.

**Likely questions**
- *Why parse the schema ID yourself?* Open-source Spark's `from_avro` does not understand the Confluent wire format: the 5-byte header (magic byte plus schema ID) must be stripped. Recording the schema ID also tells you which contract version produced each row.
- *Bronze, silver, gold in one sentence each?* Bronze is what arrived, with lineage. Silver is validated, typed, deduplicated and quarantined. Gold is business-level aggregates such as batch summaries and golden-batch profiles.

## 6. The gate (`lakehouse/gate_check.py`)

It reads bronze with **delta-rs** (no Spark needed) and fails if any row did not decode, the batch count is not 100, any batch is incomplete, or the fault batches differ from the ones sent. Duplicates are reported but do not fail the gate.

**What the gate does not prove yet:** throughput at 1×, 10× and 100× (run the replayer with `--speed` and record `events_per_s` from the manifest), and end-to-end latency to gold (Phase 2).

---

## Verified

The gate was run end to end on the real file: 100 batches, 113,935 readings, no losses, no duplicates, and an idempotent re-run. The image tags pull and the Spark packages resolve. Numbers and details are in RESULTS.md.

## When the gate passes

- Push, make sure CI is green, and take a screenshot of the gate output and the Redpanda Console topic view for the LinkedIn post.
- Skills you can then evidence: **Kafka (Redpanda), Avro with a schema registry, Spark Structured Streaming, Delta Lake (bronze), Databricks Auto Loader, pytest and CI.** Silver, gold, Airflow and drift checks come after Phase 2.
