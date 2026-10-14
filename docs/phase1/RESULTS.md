# Phase 1 gate results

**Gate: all 100 batches replay into bronze. PASSED on 14 Oct 2026**, on the real IndPenSim data (`100_Batches_IndPenSim_V3.csv` from [Mendeley Data](https://data.mendeley.com/datasets/pdnjz7zz5x/1), CC BY 4.0).

| Check | Result |
|---|---|
| Batches in bronze | 100 / 100 |
| Readings in bronze vs sent | 113,935 / 113,935 |
| Rows that failed to decode | 0 |
| Duplicate rows | 0 |
| Fault batches in bronze | 91–100 (matches the dataset's statistics file) |
| Re-running the Spark job | 0 new rows (the checkpoint prevents double-writes) |
| Recall topic (live openFDA, last 365 days) | 877 recalls in bronze; 236 cite CGMP |

Stack: Redpanda v25.3.11 (Kafka API and schema registry), Spark 4.0.4 Structured Streaming, Delta Lake 4.0.1, Python 3.12. It ran in Docker on a 2-vCPU, 7 GB machine.

**Reproduced on a Windows laptop** (Docker Desktop, WSL2), run from the VS Code task "Phase 1 gate on REAL IndPenSim data": 100/100 batches, 113,935/113,935 readings, 0 duplicates, 0 decode failures, faults 91–100. The replay ran at 776 events/s, or 113,935 in 147 s, slower than on Linux because the 2.5 GB CSV is read through the Windows bind mount.

## Measured numbers (2 vCPU, so treat these as a floor)

- **Replay, end to end at max speed:** 3,014 events/s, which is 113,935 readings in 37.8 s. This includes parsing the 2.5 GB CSV, about 18 s of the total.
- **Kafka → bronze Delta:** 74 s for both topics on the first run, including the one-off download of the Kafka, Avro and Delta packages.

Still to measure for the README: throughput at 1×, 10× and 100× (`--speed`), and end-to-end latency to gold (Phase 2).

## A real data-quality defect found on the way

The real V3 file's header row has **2,239 fields, but every data row has 2,237**. One column's legend ("1- No Raman spec / 1-Raman spec recorded / 2-PAT control(PAT_ref)") was split by commas into three header cells. Read naively:

- every column from there on is mislabelled by two places;
- `Batch ID` holds Raman intensities (47,361 distinct values instead of 100);
- the last two Raman columns come out empty.

How it was caught: the first `--inspect` reported `batch_id_source: column` and looked fine. Comparing the "Batch ID" column against the time column resetting at each batch exposed it.

How it is handled now (`streaming/indpensim.py`):

1. `repair_headers` merges the known split legend, and **refuses any other header/data mismatch** rather than guess.
2. A batch-ID column is accepted only if every ID change coincides with a time reset and IDs increase. Otherwise the replay stops with the line number.
3. The sample generator reproduces the defect, so the tests cover the repair.
4. The contract gained `pat_ref` (1 = no Raman, 2 = Raman/PAT control) and `fault_flag`, and lost the misread `raman_recorded`.

Verified against the dataset's own documentation: batches 1–30 are recipe-driven, 31–60 operator-controlled, 61–90 Raman/PAT-controlled, and 91–100 contain faults. The per-batch fault flag matches `100_Batches_IndPenSim_Statistics.csv` for all 100 batches.

This is the interview story for "what broke when the schema changed": the first schema break in the project came from the source data itself, not from an injected test.
