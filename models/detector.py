"""The frozen fault detector: everything needed to score a batch, in one object.

A detector = time profile (normal trajectory) + monitor (PCA or LSTM AE) +
control limits + persistence. It is saved as plain files with a SHA-256
manifest, and loading refuses any file whose hash does not match. The model is
static: nothing in it changes after training (draft Annex 22 expects that of a
model used in a GMP context; a retrained model is a new, separately validated
version).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from models import lstm_ae
from models.alarms import alarms
from models.features import TimeProfile, smooth_causal
from models.pca_monitor import PCAMonitor

ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts"


def sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@dataclass
class Detector:
    kind: str  # "pca" | "lstm_ae"
    profile: TimeProfile
    limits: dict  # statistic -> control limit
    persistence: int
    smooth_window: int
    pca: PCAMonitor | None = None
    lstm: object | None = None
    window: int | None = None
    meta: dict = field(default_factory=dict)

    @property
    def features(self):
        return self.profile.features

    def statistics(self, frame: pd.DataFrame) -> dict[str, np.ndarray]:
        """Per-reading monitoring statistics, 0 outside coverage (never alarms there)."""
        z = self.profile.transform(smooth_causal(frame, self.features, self.smooth_window))
        cov = self.profile.in_coverage(frame)
        if self.kind == "pca":
            raw = self.pca.scores(z)
        else:
            raw = {"recon": lstm_ae.score(self.lstm, frame, z, self.window)}
        return {k: np.where(cov, v, 0.0) for k, v in raw.items()}

    def alarm(self, frame: pd.DataFrame, stats: dict | None = None) -> np.ndarray:
        stats = stats if stats is not None else self.statistics(frame)
        out = np.zeros(len(frame), dtype=bool)
        for k, limit in self.limits.items():
            out |= alarms(frame, stats[k], limit, self.persistence)
        return out

    def contributions(self, frame: pd.DataFrame) -> pd.DataFrame:
        """PCA only: per-signal share of (T² + SPE) per reading, for explaining an alarm."""
        z = self.profile.transform(smooth_causal(frame, self.features, self.smooth_window))
        c = self.pca.contributions(z)
        tot = np.abs(c["t2"]) / max(self.limits["t2"], 1e-9) + c["spe"] / max(self.limits["spe"], 1e-9)
        return pd.DataFrame(tot, columns=list(self.features), index=frame.index)

    # ---------------------------------------------------------------- persistence

    def save(self, directory: Path) -> dict:
        import torch

        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "profile.json").write_text(json.dumps(self.profile.to_dict()))
        if self.kind == "pca":
            (directory / "pca.json").write_text(json.dumps(self.pca.to_dict()))
        else:
            torch.save(self.lstm.state_dict(), directory / "lstm.pt")
        config = {
            "kind": self.kind,
            "limits": {k: float(v) for k, v in self.limits.items()},
            "persistence": self.persistence,
            "smooth_window": self.smooth_window,
            "window": self.window,
            "lstm": self.meta.get("lstm"),
        }
        (directory / "config.json").write_text(json.dumps(config, indent=2))
        files = sorted(p.name for p in directory.iterdir() if p.name != "manifest.json")
        manifest = {"files": {f: sha256(directory / f) for f in files}, **self.meta}
        (directory / "manifest.json").write_text(json.dumps(manifest, indent=2))
        return manifest

    @classmethod
    def load(cls, directory: Path) -> Detector:
        import torch

        directory = Path(directory)
        manifest = json.loads((directory / "manifest.json").read_text())
        for f, h in manifest["files"].items():
            if sha256(directory / f) != h:
                raise RuntimeError(f"{directory / f}: SHA-256 does not match the manifest; refusing to load")
        config = json.loads((directory / "config.json").read_text())
        profile = TimeProfile.from_dict(json.loads((directory / "profile.json").read_text()))
        det = cls(config["kind"], profile, config["limits"], config["persistence"], config["smooth_window"],
                  window=config["window"], meta=manifest)
        if det.kind == "pca":
            det.pca = PCAMonitor.from_dict(json.loads((directory / "pca.json").read_text()))
        else:
            m = lstm_ae.LSTMAutoencoder(len(profile.features), **config["lstm"])
            m.load_state_dict(torch.load(directory / "lstm.pt"))
            m.eval()
            det.lstm = m
        return det
