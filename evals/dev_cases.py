"""Development cases for the agents: realistic faults injected into calibration batches (never test batches).

    python -m evals.dev_cases --source <IndPenSim CSV or silver Delta> --out out/dev_cases.parquet

A real fault moves more than one signal: an air supply loss lowers aeration, then off-gas O2 and
uptake follow. Each pattern below shifts the signals a fault of that type moves, in units of the
normal spread at that process time (the detector's own profile), as a step or a ramp, with the
culture's response lagging the equipment signal. Seven fault types are used, more than the test
set contains, so prompts cannot be tuned to the four real ones.

Dev batches are renumbered 1001+ and written with their answer key; the historian reads the file
like any other source (HISTORIAN_SOURCE=out/dev_cases.parquet).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from models.data import load_readings, load_split, subset
from models.detector import ARTIFACT_DIR, Detector

# category -> list of (signal, sigma shift, lag in hours, shape)
PATTERNS = {
    "aeration_supply": [
        ("aeration_rate_lph", -10, 0, "step"),
        ("offgas_o2_pct", -6, 0.2, "step"),
        ("offgas_co2_pct", 6, 0.2, "step"),
        ("oxygen_uptake_rate", -4, 0.6, "step"),
        ("dissolved_oxygen_mgpl", -4, 1.0, "ramp"),
    ],
    "substrate_feed": [
        ("sugar_feed_rate_lph", 10, 0, "step"),
        ("oxygen_uptake_rate", 5, 0.6, "ramp"),
        ("carbon_evolution_rate", 5, 0.6, "ramp"),
        ("dissolved_oxygen_mgpl", -5, 1.0, "ramp"),
        ("base_flow_rate_lph", 4, 1.0, "ramp"),
    ],
    "ph_measurement": [
        ("ph", 10, 0, "step"),
        ("base_flow_rate_lph", -6, 0.4, "ramp"),
        ("acid_flow_rate_lph", 6, 0.4, "ramp"),
    ],
    "coolant_supply": [
        ("temperature_k", 10, 0, "step"),
        ("cooling_water_flow_lph", 1, 0, "step"),  # supply cannot deliver more: flow barely rises
    ],
    "agitation": [
        ("dissolved_oxygen_mgpl", -10, 0, "step"),
        ("offgas_o2_pct", 6, 0.2, "step"),
        ("oxygen_uptake_rate", -6, 0.2, "step"),
        ("temperature_k", 3, 0.6, "ramp"),
    ],
    "vessel_pressure": [
        ("air_head_pressure_bar", 10, 0, "step"),
        ("dissolved_oxygen_mgpl", 6, 0.2, "ramp"),
    ],
    "precursor_feed": [
        ("paa_flow_lph", 10, 0, "step"),
        ("oxygen_uptake_rate", -4, 2.0, "ramp"),
        ("carbon_evolution_rate", -4, 2.0, "ramp"),
    ],
}
DURATION_H = 16.0


def inject(frame: pd.DataFrame, det: Detector, category: str, onset_h: float) -> pd.DataFrame:
    out = frame.copy()
    feats = list(det.profile.features)
    rows = det.profile._rows(out["step"].to_numpy())
    t = out["time_h"].to_numpy()
    for sig, sigma, lag, shape in PATTERNS[category]:
        start, end = onset_h + lag, onset_h + DURATION_H
        m = (t >= start) & (t <= end)
        frac = np.clip((t - start) / max(end - start, 1e-9), 0, 1) if shape == "ramp" else np.ones_like(t)
        delta = sigma * det.profile.std[rows, feats.index(sig)] * frac
        out.loc[m, sig] = out.loc[m, sig].to_numpy() + delta[m]
    return out


def build(source: str, seed: int = 4) -> tuple[pd.DataFrame, dict]:
    df = load_readings(source)
    det = Detector.load(ARTIFACT_DIR / "pca")
    rng = np.random.default_rng(seed)
    calib = load_split()["calibration"]
    frames, key = [], {}
    cats = list(PATTERNS)
    for i, b in enumerate(calib):  # 15 calibration batches -> 2 cases per fault type, 1 left normal
        f = subset(df, [b]).reset_index(drop=True).drop(columns=["fault_ref"], errors="ignore")
        new_id = 1001 + i
        f["batch_id"] = new_id
        if i < 2 * len(cats):
            cat = cats[i % len(cats)]
            onset = float(rng.uniform(30, 150))
            f = inject(f, det, cat, onset)
            key[str(new_id)] = {"category": cat, "onset_h": round(onset, 1), "from_calibration_batch": int(b)}
        else:
            key[str(new_id)] = {"category": None, "from_calibration_batch": int(b)}
        f["fault_ref"] = 0  # file layout only; the historian drops label columns on load
        frames.append(f)
    return pd.concat(frames, ignore_index=True), key


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", required=True)
    p.add_argument("--out", type=Path, default=Path("out/dev_cases.parquet"))
    args = p.parse_args(argv)
    frame, key = build(args.source)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(args.out, index=False)
    args.out.with_suffix(".key.json").write_text(json.dumps(key, indent=2))
    for b, k in key.items():
        print(b, k["category"], k.get("onset_h", ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
