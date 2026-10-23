"""Train, tune and freeze the fault detectors. Touches train + calibration batches only.

    python -m models.train --source /lakehouse/silver/process_sensor     (or the IndPenSim CSV)

1. Time profile of normal behaviour, from TRAIN batches.
2. PCA (T² + SPE) and an LSTM autoencoder, fitted on TRAIN batches.
3. Control limits = quantiles of each statistic on TRAIN batches; the quantile and
   the persistence are chosen on CALIBRATION batches: normal ones (false alarms)
   and the same batches with synthetic faults injected (detection).
4. Selection rule, fixed in code before any test scoring: among settings with at
   most 1 calibration false-alarm batch, take the highest synthetic step/drift
   detection. The gating model is PCA unless the LSTM beats it by more than
   5 points on that measure (PCA explains its alarms; Annex 22 values that).
5. Both detectors are frozen to models/artifacts/<name>/ with a SHA-256 manifest,
   and every run is logged to MLflow (params, calibration metrics, artifacts),
   with the gating model registered as `gmpdi-fault-detector`.

The test batches (models/gate_criteria.json) are never loaded here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd

from models import lstm_ae
from models.alarms import alarms, batch_events
from models.data import MONITORED, load_readings, load_split, subset
from models.detector import ARTIFACT_DIR, Detector
from models.features import TimeProfile, smooth_causal
from models.pca_monitor import PCAMonitor
from models.synthetic import inject, scenarios

SMOOTH = 5
WINDOW = 24
PCA_GRID = {"variance": (0.8, 0.9), "quantile": (0.995, 0.999), "persistence": (3, 5, 10)}
LSTM_GRID = {"quantile": (0.99, 0.995, 0.996, 0.997, 0.998, 0.999), "persistence": (3, 5, 10)}
LSTM_PARAMS = {"hidden": 32, "latent": 8}
MAX_CALIB_FALSE_ALARMS = 1
LSTM_MARGIN = 0.05


def fingerprint(df: pd.DataFrame) -> str:
    """Hash of exactly which readings trained the model (batch, seq, values)."""
    h = hashlib.sha256(pd.util.hash_pandas_object(df[["batch_id", "seq", *MONITORED]], index=False).values)
    return h.hexdigest()


def synthetic_set(calib: pd.DataFrame, spread: pd.Series):
    scen = scenarios(sorted(calib["batch_id"].unique()), MONITORED, n=90, seed=7, size=2.0)
    scen += scenarios(sorted(calib["batch_id"].unique()), MONITORED, n=90, seed=8, size=4.0)
    out = []
    for s in scen:
        one = calib[calib["batch_id"] == s["batch_id"]]
        out.append((s, inject(one, s["batch_id"], s["signal"], s["kind"], s["onset_h"], s["size"], spread[s["signal"]])))
    return out


def evaluate_calibration(det: Detector, calib: pd.DataFrame, syn) -> dict:
    fa = int(batch_events(calib, det.alarm(calib)).any_alarm.sum())
    res = []
    for s, f in syn:
        ev = batch_events(f, det.alarm(f)).iloc[0]
        res.append((s["kind"], s["size"], bool(ev.detected), ev.delay_h if ev.detected else None))
    r = pd.DataFrame(res, columns=["kind", "size", "det", "delay"])
    return {
        "calib_false_alarm_batches": fa,
        "synthetic_step_drift_detected": float(r[r.kind != "stuck"].det.mean()),
        "synthetic_stuck_detected": float(r[r.kind == "stuck"].det.mean()),
        "synthetic_2sd_detected": float(r[r["size"] == 2.0].det.mean()),
        "synthetic_4sd_detected": float(r[r["size"] == 4.0].det.mean()),
        "synthetic_median_delay_h": float(r.delay.dropna().median()) if r.delay.notna().any() else None,
    }


def pick(results: list[dict]) -> dict:
    ok = [r for r in results if r["calib_false_alarm_batches"] <= MAX_CALIB_FALSE_ALARMS]
    ok.sort(key=lambda r: (-r["synthetic_step_drift_detected"], -r["synthetic_2sd_detected"],
                           r["calib_false_alarm_batches"], r["persistence"]))
    if not ok:
        raise SystemExit("no setting meets the calibration false-alarm limit")
    return ok[0]


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", help="silver Delta dir, .parquet cache or IndPenSim CSV")
    p.add_argument("--out", type=Path, default=ARTIFACT_DIR)
    p.add_argument("--epochs", type=int, default=8)
    p.add_argument("--tracking-uri", default=os.environ.get("MLFLOW_TRACKING_URI", "sqlite:///out/mlflow/mlflow.db"))
    p.add_argument("--no-mlflow", action="store_true")
    p.add_argument("--register-only", action="store_true", help="log the frozen artifacts to MLflow, no training")
    args = p.parse_args(argv)
    if args.register_only:
        register_only(args.tracking_uri, args.out)
        return 0

    t0 = time.time()
    split = load_split()
    df = load_readings(args.source)
    train, calib = subset(df, split["train"]), subset(df, split["calibration"])
    assert not set(train.batch_id) & set(split["test_normal"] + split["test_fault"])
    print(f"train {train.batch_id.nunique()} batches / {len(train)} readings; "
          f"calibration {calib.batch_id.nunique()} batches / {len(calib)} readings (test batches not loaded)")

    trs = smooth_causal(train, MONITORED, SMOOTH)
    profile = TimeProfile.fit(trs, MONITORED)
    ztr = profile.transform(trs)
    spread = train[list(MONITORED)].std()
    syn = synthetic_set(calib, spread)
    meta = {
        "trained_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "train_batches": sorted(int(b) for b in train.batch_id.unique()),
        "calibration_batches": sorted(int(b) for b in calib.batch_id.unique()),
        "train_fingerprint": fingerprint(train),
        "features": list(MONITORED),
        "coverage_h": round(profile.last_reliable * 0.2, 1),
    }

    # ---- PCA
    pca_results = []
    stats_tr_cache = {}
    for var in PCA_GRID["variance"]:
        pca = PCAMonitor.fit(ztr, var)
        stats_tr_cache[var] = (pca, pca.scores(ztr))
        for q in PCA_GRID["quantile"]:
            limits = {k: float(np.quantile(v, q)) for k, v in stats_tr_cache[var][1].items()}
            for pers in PCA_GRID["persistence"]:
                det = Detector("pca", profile, limits, pers, SMOOTH, pca=pca)
                r = {"variance": var, "components": pca.k, "quantile": q, "persistence": pers,
                     **evaluate_calibration(det, calib, syn)}
                pca_results.append(r)
    best_pca = pick(pca_results)
    pca, st = stats_tr_cache[best_pca["variance"]]
    pca_det = Detector("pca", profile, {k: float(np.quantile(v, best_pca["quantile"])) for k, v in st.items()},
                       best_pca["persistence"], SMOOTH, pca=pca)
    print("PCA chosen:", {k: best_pca[k] for k in ("variance", "components", "quantile", "persistence",
                                                   "calib_false_alarm_batches", "synthetic_step_drift_detected")})

    # ---- LSTM autoencoder
    w, _ = lstm_ae.windows(train, ztr, WINDOW)
    model = lstm_ae.train(w[::2], len(MONITORED), epochs=args.epochs, **LSTM_PARAMS)
    rec_tr = lstm_ae.score(model, train, ztr, WINDOW)
    rec_tr = rec_tr[rec_tr > 0]
    lstm_results = []
    for q in LSTM_GRID["quantile"]:
        for pers in LSTM_GRID["persistence"]:
            det = Detector("lstm_ae", profile, {"recon": float(np.quantile(rec_tr, q))}, pers, SMOOTH,
                           lstm=model, window=WINDOW, meta={"lstm": LSTM_PARAMS})
            lstm_results.append({"quantile": q, "persistence": pers, **evaluate_calibration(det, calib, syn)})
    best_lstm = pick(lstm_results)
    lstm_det = Detector("lstm_ae", profile, {"recon": float(np.quantile(rec_tr, best_lstm["quantile"]))},
                        best_lstm["persistence"], SMOOTH, lstm=model, window=WINDOW, meta={"lstm": LSTM_PARAMS})
    print("LSTM chosen:", {k: best_lstm[k] for k in ("quantile", "persistence", "calib_false_alarm_batches",
                                                    "synthetic_step_drift_detected")})

    gating = "lstm_ae" if (best_lstm["synthetic_step_drift_detected"]
                           > best_pca["synthetic_step_drift_detected"] + LSTM_MARGIN) else "pca"

    # ---- freeze
    manifests = {}
    for name, det, best, grid in (("pca", pca_det, best_pca, pca_results),
                                  ("lstm_ae", lstm_det, best_lstm, lstm_results)):
        det.meta = {**meta, "kind": name, "calibration": best, "lstm": LSTM_PARAMS if name == "lstm_ae" else None}
        manifests[name] = det.save(args.out / name)
        pd.DataFrame(grid).to_csv(args.out / name / "tuning_grid.csv", index=False)
    selection = {
        "gating_model": gating,
        "rule": f"PCA unless the LSTM beats it by more than {LSTM_MARGIN:.0%} on calibration synthetic "
                "step/drift detection at <= 1 calibration false-alarm batch",
        "pca_calibration": best_pca,
        "lstm_calibration": best_lstm,
        "selected_before_test": True,
        "manifests": {k: v["files"] for k, v in manifests.items()},
    }
    (args.out / "selection.json").write_text(json.dumps(selection, indent=2))
    print(f"gating model: {gating}. Frozen to {args.out} in {time.time() - t0:.0f}s")

    if not args.no_mlflow:
        log_mlflow(args.tracking_uri, meta, {"pca": (pca_det, best_pca), "lstm_ae": (lstm_det, best_lstm)},
                   args.out, gating)
    return 0


def log_mlflow(uri, meta, runs, out_dir, gating):
    import mlflow

    from models.mlflow_model import DetectorModel

    if uri.startswith("sqlite:///"):
        Path(uri.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment("gmpdi-fault-detection")
    models_dir = Path(__file__).resolve().parent
    for name, (det, best) in runs.items():
        with mlflow.start_run(run_name=name) as run:
            mlflow.set_tags({"phase": "3", "model_kind": name, "gating": str(name == gating),
                             "train_fingerprint": meta["train_fingerprint"]})
            mlflow.log_params({k: v for k, v in best.items() if not k.startswith(("calib_", "synthetic_"))})
            mlflow.log_params({"persistence": det.persistence, "smooth_window": det.smooth_window,
                               "n_features": len(det.features), "coverage_h": meta["coverage_h"],
                               "train_batches": len(meta["train_batches"])})
            mlflow.log_metrics({k: v for k, v in best.items()
                                if k.startswith(("calib_", "synthetic_")) and v is not None})
            mlflow.log_artifact(str(out_dir / name / "tuning_grid.csv"))
            info = mlflow.pyfunc.log_model(
                name="detector",
                python_model=DetectorModel(),
                artifacts={"detector": str(out_dir / name)},
                code_paths=[str(models_dir)],
                registered_model_name="gmpdi-fault-detector" if name == gating else None,
            )
            if name == gating:
                print(f"MLflow: registered gmpdi-fault-detector from run {run.info.run_id} ({info.model_uri})")


def register_only(uri, out_dir=ARTIFACT_DIR):
    """Log the already-frozen detectors to MLflow without retraining."""
    sel = json.loads((out_dir / "selection.json").read_text())
    runs = {}
    for name in ("pca", "lstm_ae"):
        det = Detector.load(out_dir / name)
        runs[name] = (det, sel["pca_calibration" if name == "pca" else "lstm_calibration"])
    meta = json.loads((out_dir / "pca" / "manifest.json").read_text())
    log_mlflow(uri, meta, runs, out_dir, sel["gating_model"])


if __name__ == "__main__":
    raise SystemExit(main())
