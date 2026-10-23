# Model card: gmpdi-fault-detector v1

Written in the spirit of the **draft** EU GMP Annex 22 (Artificial Intelligence, consultation draft, July 2025). It is a learning project on simulated data, not a validated GMP system.

| | |
|---|---|
| Model | PCA multivariate statistical process control: Hotelling T² + SPE (Q), 11 components (90% of normal variation) |
| Version | v1, MLflow registry `gmpdi-fault-detector` version 1 |
| Frozen artifacts | `models/artifacts/pca/` (`profile.json`, `pca.json`, `config.json`), each SHA-256-pinned in `manifest.json`. Loading refuses a mismatched file |
| Status | **Static.** It does not learn in use. A retrained model is a new version, re-tested against the same pre-registered criteria |
| Trained | 23 Oct 2026, on 60 normal batches (fingerprint `7f3becddb25a1733…`) |
| Comparison model | LSTM autoencoder (`models/artifacts/lstm_ae/`), scored on the same test, not used for decisions |

## Intended use

It flags a fermentation batch whose online process signals have left normal behaviour, so that a qualified person can open a deviation investigation. It **detects only**. It does not classify the fault, name a root cause, or decide anything about the batch. In this project its alarm, together with the signals that drove it, starts the Phase 4 agent investigation, which a named QA reviewer approves or rejects.

It is out of scope for real plant data, for any process other than the IndPenSim penicillin fermentation, and for process times beyond its coverage (230 h).

## Inputs

There are 19 online signals, aligned to process time (0.2 h steps):

- aeration rate
- sugar, acid, base, PAA and oil flows
- cooling and heating water flows
- air head pressure
- dissolved O₂
- vessel volume and weight
- pH and temperature
- generated heat
- off-gas CO₂ and O₂
- OUR and CER

Deliberately excluded:

| Excluded | Why |
|---|---|
| Labels and recipe flags (`fault_ref`, `fault_flag`, `control_ref`, `pat_ref`, `batch_ref`) | They would leak the answer |
| Simulator states (substrate, penicillin concentration) | A real plant does not measure these online |
| Lab assays | 98% empty |
| Constant signals | No information |
| Broth-dump and water-injection commands | Their timing varies between normal batches. Their effect is measured through volume and weight |

## How it decides

1. **Smoothing:** a trailing 1-hour median, causal, so it uses only past readings.
2. **Standardising against normal:** the deviation from the normal trajectory at the same process step: z = (value − mean) / spread across training batches.
3. **Scoring:** T² and SPE are computed on the z-vector.
4. **Alarm rule:** an alarm fires when either statistic stays above its control limit for **10 consecutive readings (2 h)**. The limits are the 99.5th percentile of each statistic on the training batches.

## Data and independence

The split was fixed and committed before any modelling (`models/gate_criteria.json`, commit 589a0ae). It is stratified by control strategy, with seed 2026.

| Set | Batches | Used for |
|---|---|---|
| Train | 60 normal | Normal trajectory, PCA |
| Calibration | 15 normal, plus 180 synthetic faults injected into them | Choosing the variance, the limit quantile and the persistence |
| Test | 15 normal, plus fault batches 91–100 | Scored once (commit a5867d9), after the models were frozen (927ee77) |

The synthetic faults were steps, drifts and stuck sensors on random signals, and were deliberately not modelled on IndPenSim's own fault types.

## Performance against pre-registered acceptance criteria

| Criterion (set before testing) | Result | |
|---|---|---|
| ≥ 8 of 10 fault batches detected after fault onset | **10 / 10** | pass |
| ≤ 2 false-alarm batches (15 normal + pre-onset alarms in fault batches) | **1** (normal batch 32) | pass |

Measured on the same test, reported without a criterion:

| Measure | Value |
|---|---|
| Median detection delay | 2.2 h (range 2.2 to 9.4 h) |
| Reading-level ROC AUC (score against fault-active) | 0.931 |
| Fault episodes caught, post-hoc (fault batches contain 2–3 episodes) | 18 / 21 |
| LSTM autoencoder, same test | 10/10 batches, 2 false-alarm batches, 2.7 h median delay, AUC 0.902, 18/21 episodes |

## Explainability

Every alarm comes with per-signal contributions to T² and SPE. Without being told the fault types, the top signals at the first alarm fell into four consistent groups:

- **Aeration and off-gas:** batches 94, 99 and 100.
- **pH and acid/base flow:** batches 92 and 98.
- **Temperature and cooling water:** batches 93 and 96.
- **Sugar feed and oxygen uptake:** batches 91, 95 and 97.

These contributions are what the investigating agents receive.

## Confidence

The score is the statistic divided by its limit, and a higher score means a higher chance of a real fault:

| Score band (statistic / limit) | Readings | Share in an active fault |
|---|---:|---:|
| < 0.5 | 24,882 | 1% |
| 0.5 – 1 | 1,267 | 9% |
| 1 – 2 | 1,176 | 50% |
| 2 – 5 | 235 | 69% |
| ≥ 5 | 125 | 96% |

Each alarm should be reported with its score, not as a bare yes/no.

## Known limitations

- **Late, small faults are missed.** All 3 missed episodes are the third episode of batches 91, 95 and 97, at about 200 h. These belong to the sugar-feed fault group, at a point where the feed is small.
- **Stuck sensors are a blind spot.** A signal frozen at a plausible value looks normal. Only about 20% of synthetic stuck faults were caught on calibration.
- **Coverage ends at 230 h.** After that, too few training batches remain for a reliable reference, so later readings are reported as outside coverage rather than scored.
- **Real upsets in "normal" batches alarm.** The two such cases seen were batch 67 (calibration: pH 5.40 against a normal 6.50) and batch 32 (test: acid dosing with a 1 K temperature swing). The monitor flags a real excursion even when the simulator labels the batch normal. A human decides whether it matters.
- **The test is small.** 10 fault batches with 21 episodes, 4 fault families and one simulator. The numbers are not a claim about a real plant.
- **The alarm comes after about 2 hours, by design.** That is the persistence rule plus smoothing, a trade-off against false alarms that was chosen on calibration data.

## Human oversight and change control

- The model only raises a flag. A named QA reviewer owns every decision that follows (Phase 4 and 5).
- Any change, whether to signals, the split, limits, persistence or retraining, produces a new version. That version is re-scored against the same pre-registered criteria on data it has not seen, and is reported separately. Test results are never used to tune a model.
- Reproduce with `python -m models.evaluate --source <silver table>`. The code is committed, the artifacts are hash-pinned, and the run is logged in MLflow.
