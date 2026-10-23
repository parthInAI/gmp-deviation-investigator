"""Synthetic faults injected into CALIBRATION batches, to tune alarm settings.

Batches 91-100 are the test and are never used to choose anything. To tune the
control limits and the persistence rule we still need faults to detect, so we
inject generic sensor/actuator faults into normal calibration batches:

  step   a sudden offset of `size` x the signal's normal spread
  drift  an offset growing linearly to `size` x spread over `ramp_h` hours
  stuck  the signal freezes at its value at onset

These are deliberately NOT modelled on IndPenSim's own fault types (that would
be tuning to the test). They also only change the faulty signal itself, with no
process propagation, so they are a harder, cleaner test of the monitor than real
faults, where a failing actuator drags other signals with it.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

KINDS = ("step", "drift", "stuck")


def inject(
    df: pd.DataFrame,
    batch_id: int,
    signal: str,
    kind: str,
    onset_h: float,
    size: float,
    spread: float,
    ramp_h: float = 20.0,
) -> pd.DataFrame:
    """Copy of df with one fault injected into one batch; fault_ref = 1 from onset (the ground truth)."""
    out = df.copy()
    m = (out["batch_id"] == batch_id) & (out["time_h"] >= onset_h)
    t = out.loc[m, "time_h"] - onset_h
    if kind == "step":
        out.loc[m, signal] = out.loc[m, signal] + size * spread
    elif kind == "drift":
        out.loc[m, signal] = out.loc[m, signal] + size * spread * np.minimum(t / ramp_h, 1.0)
    elif kind == "stuck":
        first = out.loc[m, signal].iloc[0]
        out.loc[m, signal] = first
    else:
        raise ValueError(kind)
    out.loc[m, "fault_ref"] = 1
    return out


def scenarios(batch_ids, features, n: int = 60, seed: int = 7, size: float = 2.0):
    """A fixed, reproducible list of (batch, signal, kind, onset_h, size)."""
    rng = np.random.default_rng(seed)
    out = []
    for i in range(n):
        out.append(
            {
                "id": i,
                "batch_id": int(rng.choice(batch_ids)),
                "signal": str(rng.choice(features)),
                "kind": KINDS[i % len(KINDS)],
                "onset_h": float(np.round(rng.uniform(20, 150) / 0.2) * 0.2),
                "size": float(size),
            }
        )
    return out
