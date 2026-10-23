"""Multivariate statistical process control: PCA with Hotelling T² and SPE.

The classic process-monitoring baseline in pharma and chemicals. PCA on the
time-aligned deviations of NORMAL batches learns how the signals normally move
together. Two statistics per reading:

  T² (Hotelling)  how far the reading is from normal INSIDE the learned model
                  (a known pattern, pushed unusually far)
  SPE (Q)         how much of the reading the model cannot explain at all
                  (a new pattern: signals breaking their usual relationship)

Each gets a control limit from normal calibration batches. Both are
interpretable: per-signal contributions say WHICH signals drove an alarm, which
is what an investigator (and later the agents) needs.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class PCAMonitor:
    mean: np.ndarray  # (n_features,)
    components: np.ndarray  # (k, n_features)
    eigenvalues: np.ndarray  # (k,)
    explained: float

    @classmethod
    def fit(cls, z: np.ndarray, variance: float = 0.9) -> PCAMonitor:
        mean = z.mean(axis=0)
        x = z - mean
        _, s, vt = np.linalg.svd(x, full_matrices=False)
        eig = s**2 / (len(x) - 1)
        share = np.cumsum(eig) / eig.sum()
        k = int(np.searchsorted(share, variance) + 1)
        return cls(mean, vt[:k], eig[:k], float(share[k - 1]))

    @property
    def k(self) -> int:
        return len(self.eigenvalues)

    def scores(self, z: np.ndarray) -> dict[str, np.ndarray]:
        x = z - self.mean
        t = x @ self.components.T
        t2 = np.sum(t**2 / self.eigenvalues, axis=1)
        resid = x - t @ self.components
        spe = np.sum(resid**2, axis=1)
        return {"t2": t2, "spe": spe}

    def contributions(self, z: np.ndarray) -> dict[str, np.ndarray]:
        """Per-signal contribution to T² and SPE, (n, n_features) each: which signals drove it."""
        x = z - self.mean
        t = x @ self.components.T
        resid = x - t @ self.components
        t2c = (t / self.eigenvalues) @ self.components * x
        return {"t2": t2c, "spe": resid**2}

    def to_dict(self) -> dict:
        return {
            "mean": self.mean.tolist(),
            "components": self.components.tolist(),
            "eigenvalues": self.eigenvalues.tolist(),
            "explained": self.explained,
        }

    @classmethod
    def from_dict(cls, d: dict) -> PCAMonitor:
        return cls(np.array(d["mean"]), np.array(d["components"]), np.array(d["eigenvalues"]), d["explained"])
