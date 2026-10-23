"""The Phase 3 gate: score the held-out test batches with the frozen detectors.

    python -m models.evaluate --source /lakehouse/silver/process_sensor   (or the IndPenSim CSV)

Loads the frozen artifacts (refusing any file whose SHA-256 does not match its
manifest), loads ONLY the test batches named in models/gate_criteria.json
(15 held-out normal batches + fault batches 91-100), and scores them. The gating
model (models/artifacts/selection.json) is judged against the pre-registered
criteria; the other model is reported alongside for comparison.

Writes out/gate/phase3_gate.json and a chart per fault batch (out/gate/phase3/).
Nothing here feeds back into training: changing a setting after seeing these
numbers would make them meaningless, so any later change is a new, separately
reported version.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

from models.alarms import batch_events, summarise
from models.data import GATE_FILE, load_readings, load_split, subset
from models.detector import ARTIFACT_DIR, Detector


def norm_score(det: Detector, stats: dict) -> np.ndarray:
    """One number per reading: the statistic / its limit (worst of T² and SPE for PCA). > 1 = over the limit."""
    return np.max(np.vstack([stats[k] / v for k, v in det.limits.items()]), axis=0)


def roc_auc(y: np.ndarray, s: np.ndarray) -> float:
    """Rank-based AUC (Mann-Whitney), no sklearn needed."""
    pos, neg = s[y == 1], s[y == 0]
    if not len(pos) or not len(neg):
        return float("nan")
    ranks = pd.Series(np.concatenate([pos, neg])).rank().to_numpy()
    return float((ranks[: len(pos)].sum() - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg)))


def reliability(y: np.ndarray, s: np.ndarray) -> list[dict]:
    """Share of fault-active readings per band of normalised score: is a higher score more likely a fault?"""
    bands = [0, 0.5, 1, 2, 5, np.inf]
    out = []
    for lo, hi in zip(bands[:-1], bands[1:], strict=True):
        m = (s >= lo) & (s < hi)
        out.append(
            {"band": f"{lo}-{hi}", "readings": int(m.sum()), "fault_share": float(y[m].mean()) if m.any() else None}
        )
    return out


def top_signals(det: Detector, frame: pd.DataFrame, events: pd.DataFrame, n: int = 3) -> dict:
    """PCA: the signals contributing most over the first two hours after each fault-batch alarm."""
    contrib = det.contributions(frame)
    out = {}
    for _, ev in events.iterrows():
        if not ev.detected:
            continue
        onset_alarm = ev.fault_onset_h + ev.delay_h
        m = (frame.batch_id == ev.batch_id) & frame.time_h.between(onset_alarm - 2, onset_alarm)
        out[int(ev.batch_id)] = contrib[m.to_numpy()].mean().sort_values(ascending=False).head(n).round(2).to_dict()
    return out


def plot(frame, score, alarm, events, det_name, out_dir: Path):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    out_dir.mkdir(parents=True, exist_ok=True)
    ids = sorted(frame[frame.fault_ref > 0].batch_id.unique())
    fig, axes = plt.subplots(len(ids), 1, figsize=(10, 1.6 * len(ids)), sharex=True)
    for ax, bid in zip(np.atleast_1d(axes), ids, strict=False):
        m = (frame.batch_id == bid).to_numpy()
        t, s, f, a = frame.time_h[m], score[m], frame.fault_ref[m] > 0, alarm[m]
        ax.fill_between(t, 0, 1, where=f, transform=ax.get_xaxis_transform(), color="#e66767", alpha=0.18, lw=0)
        ax.plot(t, np.clip(s, 0, 6), color="#3987e5", lw=1)
        ax.axhline(1, color="#333", lw=0.8, ls="--")
        if a.any():
            ax.axvline(float(t[a].iloc[0]), color="#199e70", lw=1.6)
        ax.set_ylim(0, 6)
        ax.set_ylabel(f"#{bid}", rotation=0, ha="right", va="center")
        ax.spines[["top", "right"]].set_visible(False)
    np.atleast_1d(axes)[-1].set_xlabel("process time (h)")
    fig.suptitle(
        f"{det_name}: statistic / control limit (clipped at 6); red = fault active, green = first alarm", fontsize=9
    )
    fig.tight_layout()
    path = out_dir / f"fault_batches_{det_name}.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return str(path)


def evaluate(det: Detector, name: str, test: pd.DataFrame, split: dict, out_dir: Path) -> dict:
    stats = det.statistics(test)
    alarm = det.alarm(test, stats)
    events = batch_events(test, alarm)
    summary = summarise(events, split["test_fault"], split["test_normal"])
    score = norm_score(det, stats)
    cov = det.profile.in_coverage(test)
    y = (test.fault_ref > 0).to_numpy().astype(int)
    result = {
        "model": name,
        **summary,
        "reading_auc": roc_auc(y[cov], score[cov]),
        "reliability": reliability(y[cov], score[cov]),
        "faults_starting_after_coverage": int((events.fault_onset_h.fillna(0) > det.profile.last_reliable * 0.2).sum()),
        "per_batch": json.loads(events.to_json(orient="records")),
        "plot": plot(test, score, alarm, events, name, out_dir),
    }
    if det.kind == "pca":
        result["top_signals_at_alarm"] = top_signals(det, test, events[events.detected.fillna(False)])
    return result


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", required=True)
    p.add_argument("--artifacts", type=Path, default=ARTIFACT_DIR)
    p.add_argument("--report", type=Path, default=Path("out/gate/phase3_gate.json"))
    args = p.parse_args(argv)

    gate = json.loads(GATE_FILE.read_text())
    selection = json.loads((args.artifacts / "selection.json").read_text())
    split = load_split()
    df = load_readings(args.source)
    test = subset(df, split["test_normal"] + split["test_fault"])
    assert set(test.batch_id) == set(split["test_normal"] + split["test_fault"])

    results = {}
    for name in ("pca", "lstm_ae"):
        det = Detector.load(args.artifacts / name)  # refuses tampered files
        results[name] = evaluate(det, name, test, split, args.report.parent / "phase3")

    g = results[selection["gating_model"]]
    c = gate["criteria"]
    passed = (
        g["fault_batches_detected"] >= c["fault_batches_detected_min"]
        and g["false_alarm_batches"] <= c["false_alarm_batches_max"]
    )
    try:
        commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    except OSError:
        commit = None
    report = {
        "gate": gate["gate"],
        "criteria": c,
        "gating_model": selection["gating_model"],
        "passed": bool(passed),
        "evaluated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "code_commit": commit,
        "artifact_hashes": {n: Detector.load(args.artifacts / n).meta["files"] for n in results},
        "results": results,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, default=float))

    print(f"\n{gate['gate']} (gating model: {selection['gating_model']})")
    print(f"{'model':8} {'faults caught':>14} {'false-alarm batches':>20} {'median delay h':>15} {'reading AUC':>12}")
    for n, r in results.items():
        print(
            f"{n:8} {r['fault_batches_detected']:>9}/{r['fault_batches']:<4} "
            f"{r['false_alarm_batches']:>13}/{r['normal_batches']:<6} "
            f"{(r['median_delay_h'] if r['median_delay_h'] is not None else float('nan')):>15.1f} "
            f"{r['reading_auc']:>12.3f}"
        )
    print(
        f"criteria: >= {c['fault_batches_detected_min']}/{c['of_fault_batches']} faults caught, "
        f"<= {c['false_alarm_batches_max']}/{c['of_test_normal_batches']} false-alarm batches"
    )
    print("PHASE 3 GATE: " + ("PASS" if passed else "FAIL"))
    print(f"report -> {args.report}")
    return 0 if passed else 1


if __name__ == "__main__":
    from lakehouse.cli import exit_now

    exit_now(main())
