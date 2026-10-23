"""Time-aligned deviations from the normal trajectory ("golden batch").

A fermentation looks completely different at hour 10 and hour 200, so a raw
value means nothing on its own. For every signal and every process step (0.2 h)
the profile stores the mean and spread across TRAINING normal batches, and a
reading becomes

    z = (value - mean_at_this_step) / spread_at_this_step

which is ~N(0, 1) for a normal batch at any point in its life. This is the
classic batch-evolution approach (Nomikos & MacGregor style, observation-wise),
and the same idea as gold.golden_profile, at full time resolution.

Details that matter:
  * the spread has a floor (a share of the signal's overall spread): at some steps a
    signal is identical in every training batch, and dividing by ~0 would turn any
    tiny difference into a huge z
  * mean and spread are smoothed over +/-`smooth` steps, so one odd batch at one
    step does not set the reference
  * after the last step with enough training batches (`min_batches`), the last
    reliable reference is carried forward (batches end between ~170 and 290 h)
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

Z_CLIP = 10.0  # one spike may not dominate a score


def smooth_causal(df: pd.DataFrame, features, window: int = 5) -> pd.DataFrame:
    """Trailing rolling median per batch (window readings, default 5 = 1 h).

    Causal on purpose: at time t it only uses readings up to t, exactly what a live
    monitor would have. Medians drop one-reading spikes from on/off events
    (broth dumps, water injections) whose timing varies between normal batches.
    """
    out = df.copy()
    cols = list(features)
    out[cols] = df.groupby("batch_id", sort=False)[cols].transform(lambda s: s.rolling(window, min_periods=1).median())
    return out


@dataclass
class TimeProfile:
    features: tuple[str, ...]
    mean: np.ndarray  # (n_steps, n_features)
    std: np.ndarray  # (n_steps, n_features)
    support: np.ndarray  # (n_steps,) training batches present at each step
    last_reliable: int

    @classmethod
    def fit(cls, train: pd.DataFrame, features, min_batches: int = 20, smooth: int = 2, floor: float = 0.1):
        features = tuple(features)
        n_steps = int(train["step"].max()) + 1
        g = train.groupby("step")
        mean = g[list(features)].mean().reindex(range(n_steps))
        std = g[list(features)].std(ddof=1).reindex(range(n_steps))
        support = g["batch_id"].nunique().reindex(range(n_steps)).fillna(0).to_numpy()
        if smooth:
            mean = mean.rolling(2 * smooth + 1, center=True, min_periods=1).mean()
            std = std.rolling(2 * smooth + 1, center=True, min_periods=1).mean()
        reliable = np.flatnonzero(support >= min_batches)
        last = int(reliable.max())
        mean, std = mean.to_numpy(copy=True), std.to_numpy(copy=True)  # pandas 3 returns read-only views
        mean[last + 1 :] = mean[last]
        std[last + 1 :] = std[last]
        global_std = train[list(features)].std(ddof=1).to_numpy()
        # absolute floor too: a signal constant across ALL training must not divide by zero
        std = np.maximum(np.maximum(np.nan_to_num(std, nan=0.0), floor * np.nan_to_num(global_std)), 1e-9)
        return cls(features, mean, std, support, last)

    def _rows(self, steps: np.ndarray) -> np.ndarray:
        return np.minimum(steps, self.last_reliable)

    def transform(self, df: pd.DataFrame) -> np.ndarray:
        """(n_readings, n_features) z-scores, aligned to each reading's process step."""
        rows = self._rows(df["step"].to_numpy())
        x = df[list(self.features)].to_numpy(dtype=float)
        return np.clip((x - self.mean[rows]) / self.std[rows], -Z_CLIP, Z_CLIP)

    def in_coverage(self, df: pd.DataFrame) -> np.ndarray:
        """True where the normal reference is backed by enough training batches.

        Past `last_reliable` (230 h with the default split) only a handful of training
        batches are still running, so a comparison there is not trustworthy. The monitor
        reports those readings as outside coverage instead of alarming on them.
        """
        return df["step"].to_numpy() <= self.last_reliable

    def to_dict(self) -> dict:
        return {
            "features": list(self.features),
            "mean": self.mean.tolist(),
            "std": self.std.tolist(),
            "support": self.support.tolist(),
            "last_reliable": self.last_reliable,
        }

    @classmethod
    def from_dict(cls, d: dict) -> TimeProfile:
        return cls(
            tuple(d["features"]),
            np.array(d["mean"]),
            np.array(d["std"]),
            np.array(d["support"]),
            int(d["last_reliable"]),
        )
