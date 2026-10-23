# Phase 3 design notes: the detection model

What each piece does, why it is built this way, and the questions an interviewer is likely to ask. As in Phases 1 and 2, every claim here should be defensible without notes.

## 1. Pre-registration (`models/gate_criteria.json`)

**What it is.** The batch split and the pass criteria, committed before any model existed. They are: at least 8 of 10 fault batches detected, and no more than 2 false-alarm batches.

**Why.** With 10 fault batches it is very easy to fool yourself. Tune a threshold while looking at the test, and the result says nothing. Committing the criteria, then the frozen models, then the evaluation code, and only then running the test once, gives a timeline anyone can check on GitHub. It is the same discipline as validating a computerised system against pre-approved acceptance criteria.

**Likely questions**
- *What if it had failed?* Then it fails, and the report says so. A fix becomes v2, re-tested against the same criteria. Nudging v1 until it passes is exactly what pre-registration prevents.
- *How did you tune without faults?* By injecting synthetic faults (steps, drifts, stuck sensors on random signals) into the 15 calibration batches. They were deliberately not modelled on IndPenSim's real fault types, which would have been tuning to the test.

## 2. Features (`models/data.py`, `models/features.py`)

**Design choices**
- **Only the signals a plant actually has online.** Substrate and penicillin concentration are simulator states. Using them would flatter the model, because estimating them is what Raman/PAT is for.
- **Deviation from the normal trajectory at the same process time.** Hour 10 and hour 200 of a fermentation look nothing alike, so raw values are compared with the mean and spread of training batches at that step. This is the classic batch-evolution approach (Nomikos and MacGregor).
- **A spread floor, smoothing and a ±10 z clip.** These were added after looking at calibration data. Some signals are identical in every batch at some steps, which would mean dividing by almost zero. On/off events spike, so one reading should not dominate.
- **A causal filter.** The trailing median uses only past readings, which is all a live monitor has. A centred filter would quietly use the future.
- **Coverage ends at 230 h.** Training support drops from 44 batches at 230 h to 8 at 240 h. Rather than alarming on a reference built from a handful of batches, later readings are reported as outside coverage.
- **Event commands dropped.** Broth-dump and water-injection commands spike at different times in every normal batch, and PCA spent a whole component on them. A real fault on those signals then scored below the limit: the noise blinded the model to itself. Their effect is measured anyway through vessel volume and weight.

## 3. PCA: Hotelling T² and SPE (`models/pca_monitor.py`)

**What it does.** It learns how the 19 signals normally move together. T² measures how far a reading sits along known patterns. SPE measures how much of it no known pattern explains. Each has a limit, and the alarm rule is either one over its limit for 2 hours.

**Why this is the gating model.** On calibration it tied with the LSTM (74% against 77% of synthetic step and drift faults, both at 1 of 15 false-alarm batches). The selection rule, fixed in code before testing, prefers PCA unless the LSTM wins by more than 5 points. The reason is explainability. Per-signal contributions say which signals drove an alarm, and they grouped the 10 fault batches into four sensible families without being told what the faults were. Phase 4's agents need exactly that, and the draft Annex 22 asks for it.

**Likely questions**
- *Why not just use a deep model?* I trained one. On this data it did not beat the 1990s baseline (AUC 0.90 against 0.93), and it cannot say why it alarmed. Using the simpler, explainable model when performance is equal is a decision, not a shortcut.
- *T² against SPE in one sentence?* T² is a known pattern pushed too far. SPE is a pattern the model has never seen.

## 4. LSTM autoencoder (`models/lstm_ae.py`)

It takes 24-reading windows (4.8 h) of the same deviations, passes them through a bottleneck of 8, and reconstructs them. The reconstruction error is the score. It trains in about 15 s on a 2-core CPU, with a fixed seed. It is scored on the same test and reported, but it does not gate.

## 5. Alarm rule and metrics (`models/alarms.py`)

- **An alarm needs 10 consecutive readings over the limit, and runs never cross batches.** That costs 2 hours of delay and removes single-reading noise. The persistence was chosen on calibration from 3, 5 or 10.
- **Batch metrics follow the pre-registered definitions.** A fault counts as detected if an alarm comes at or after the first fault reading. An alarm before onset in a fault batch counts as a false alarm, not a detection.

## 6. Frozen, static, hash-pinned (`models/detector.py`, `models/mlflow_model.py`)

- **Everything needed to score is plain files** (the profile, the PCA or LSTM weights, the limits) with a SHA-256 manifest. `Detector.load` refuses a mismatched file. A test proves that quietly changing the persistence gets refused.
- **MLflow** logs each run's parameters, calibration metrics and artifacts. The gating model is registered as a pyfunc model, `gmpdi-fault-detector` v1, loadable with `mlflow.pyfunc.load_model`.
- **Static, because the draft Annex 22 expects it.** A model used in a GMP context should not change itself in use. Retraining means a new version and new validation.

## 7. What the evaluation found that the gate does not show

- **Fault batches have 2–3 episodes.** Per episode, 18 of 21 were caught (`models/episodes.py`, labelled post-hoc). All three misses are the late substrate-feed episode at about 200 h. It is reported, not hidden, and it is a concrete target for v2.
- **The one false alarm is a real excursion** in a batch labelled normal: acid dosing switched on while temperature swung 1 K. Whether that matters is a human call, which is the point of the design.

## Verified

- 148 tests, including split integrity, label leakage, causal smoothing, artifact tampering, and that the frozen artifacts load and match the selection.
- The gate gives the same result from silver Delta and from the CSV.
- Both detectors load hash-checked inside the app Docker image, in CI.
