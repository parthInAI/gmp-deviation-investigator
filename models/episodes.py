"""Post-hoc analysis: detection per fault EPISODE, not just per batch.

    python -m models.episodes --source <silver dir | csv>

Added after the gate was run, and labelled as such: it changes nothing about
the gate. The pre-registered gate asks whether each fault batch raises an alarm
after its first fault starts. Plotting the test batches showed most fault
batches contain two or three separate fault episodes, so this asks the stricter
question: is EACH episode caught? An episode counts as caught if an alarm fires
between its start and 2 h after its end (persistence + smoothing need ~2 h).
Results are appended to out/gate/phase3_gate.json under "post_hoc_episodes".
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from models.data import load_readings, load_split, subset
from models.detector import ARTIFACT_DIR, Detector

GRACE_H = 2.0


def episodes(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for bid, g in frame.groupby("batch_id"):
        on = (g["fault_ref"] > 0).to_numpy()
        t = g["time_h"].to_numpy()
        starts = np.flatnonzero(on & ~np.r_[False, on[:-1]])
        ends = np.flatnonzero(on & ~np.r_[on[1:], False])
        for k, (s, e) in enumerate(zip(starts, ends, strict=True), start=1):
            rows.append({"batch_id": int(bid), "episode": k, "start_h": float(t[s]), "end_h": float(t[e])})
    return pd.DataFrame(rows)


def score_episodes(det: Detector, frame: pd.DataFrame, eps: pd.DataFrame, coverage_h: float) -> pd.DataFrame:
    alarm = det.alarm(frame)
    out = []
    for _, ep in eps.iterrows():
        m = (frame.batch_id == ep.batch_id).to_numpy()
        t = frame.time_h.to_numpy()[m]
        a = alarm[m]
        hit = a & (t >= ep.start_h) & (t <= ep.end_h + GRACE_H)
        out.append(
            {
                **ep.to_dict(),
                "in_coverage": bool(ep.start_h <= coverage_h),
                "caught": bool(hit.any()),
                "delay_h": float(t[hit][0] - ep.start_h) if hit.any() else None,
            }
        )
    return pd.DataFrame(out)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", required=True)
    p.add_argument("--artifacts", type=Path, default=ARTIFACT_DIR)
    p.add_argument("--report", type=Path, default=Path("out/gate/phase3_gate.json"))
    args = p.parse_args(argv)

    split = load_split()
    test = subset(load_readings(args.source), split["test_fault"])
    eps = episodes(test)
    report = json.loads(args.report.read_text())
    post = {
        "note": "post-hoc analysis added after the gate run; does not change the pre-registered gate",
        "grace_h": GRACE_H,
        "models": {},
    }
    for name in ("pca", "lstm_ae"):
        det = Detector.load(args.artifacts / name)
        cov_h = det.profile.last_reliable * 0.2
        r = score_episodes(det, test, eps, cov_h)
        inside = r[r.in_coverage]
        post["models"][name] = {
            "episodes": len(r),
            "episodes_in_coverage": len(inside),
            "caught_in_coverage": int(inside.caught.sum()),
            "by_order": inside.groupby("episode").caught.agg(["sum", "count"]).astype(int).to_dict("index"),
            "per_episode": json.loads(r.to_json(orient="records")),
        }
        print(
            f"{name:8} episodes caught {int(inside.caught.sum())}/{len(inside)} within coverage "
            f"({len(r) - len(inside)} start after {cov_h:.0f} h, outside coverage); by episode order: "
            + ", ".join(f"#{k}: {v['sum']}/{v['count']}" for k, v in post["models"][name]["by_order"].items())
        )
    report["post_hoc_episodes"] = post
    args.report.write_text(json.dumps(report, indent=2, default=float))
    return 0


if __name__ == "__main__":
    from lakehouse.cli import exit_now

    exit_now(main())
