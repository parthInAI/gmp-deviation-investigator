"""From a per-reading statistic to alarms, and from alarms to the gate's batch-level metrics.

One rule for every model: an alarm fires when a statistic stays above its
control limit for `persistence` consecutive readings. A single excursion is
noise; a run is a signal. The alarm time is the reading that completes the run,
which is when an operator would actually be told.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def run_alarm(exceed: np.ndarray, persistence: int) -> np.ndarray:
    """Boolean per reading: True where the current run of exceedances reaches `persistence`."""
    out = np.zeros(len(exceed), dtype=bool)
    run = 0
    for i, e in enumerate(exceed):
        run = run + 1 if e else 0
        out[i] = run >= persistence
    return out


def alarms(frame: pd.DataFrame, score: np.ndarray, limit: float, persistence: int) -> np.ndarray:
    """Per-reading alarm flags, computed batch by batch (runs never span two batches)."""
    out = np.zeros(len(frame), dtype=bool)
    exceed = score > limit
    for _, idx in frame.groupby("batch_id", sort=False).indices.items():
        out[idx] = run_alarm(exceed[idx], persistence)
    return out


def batch_events(frame: pd.DataFrame, alarm: np.ndarray) -> pd.DataFrame:
    """One row per batch: fault onset (if any), first alarm, first alarm after onset."""
    rows = []
    df = frame.assign(_alarm=alarm)
    for bid, g in df.groupby("batch_id", sort=True):
        fault = g[g["fault_ref"] > 0]
        onset = float(fault["time_h"].iloc[0]) if len(fault) else None
        al = g[g["_alarm"]]
        first = float(al["time_h"].iloc[0]) if len(al) else None
        before = al[al["time_h"] < onset] if onset is not None else al
        after = al[al["time_h"] >= onset] if onset is not None else al.iloc[0:0]
        rows.append(
            {
                "batch_id": int(bid),
                "fault_onset_h": onset,
                "first_alarm_h": first,
                "alarm_before_onset": bool(len(before)) if onset is not None else None,
                "detected": bool(len(after)) if onset is not None else None,
                "delay_h": (float(after["time_h"].iloc[0]) - onset) if (onset is not None and len(after)) else None,
                "any_alarm": bool(len(al)),
                "alarm_readings": int(g["_alarm"].sum()),
            }
        )
    return pd.DataFrame(rows)


def summarise(events: pd.DataFrame, fault_ids, normal_ids) -> dict:
    ev = events.set_index("batch_id")
    f = ev.loc[[b for b in fault_ids if b in ev.index]]
    n = ev.loc[[b for b in normal_ids if b in ev.index]]
    delays = f["delay_h"].dropna()
    return {
        "fault_batches": len(f),
        "fault_batches_detected": int(f["detected"].fillna(False).sum()),
        "normal_batches": len(n),
        "normal_batches_with_alarm": int(n["any_alarm"].sum()),
        "fault_batches_alarm_before_onset": int(f["alarm_before_onset"].fillna(False).sum()),
        "false_alarm_batches": int(n["any_alarm"].sum() + f["alarm_before_onset"].fillna(False).sum()),
        "median_delay_h": float(delays.median()) if len(delays) else None,
        "max_delay_h": float(delays.max()) if len(delays) else None,
    }
