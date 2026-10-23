"""LSTM autoencoder: the deep-learning comparison to the PCA baseline.

Input is the same time-aligned deviations (z) the PCA sees, in windows of the
last `window` readings (default 24 = 4.8 h, causal: only past readings). The
network squeezes each window through a small bottleneck and rebuilds it. Trained
on normal batches only, it rebuilds normal behaviour well and abnormal behaviour
badly, so the reconstruction error is the anomaly score.

Why compare at all: PCA sees one reading at a time and only linear relations.
The LSTM sees a short history and can learn non-linear, time-dependent patterns.
Whether that buys anything on this data is exactly what Phase 3 measures.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import torch
from torch import nn

SCALE = 3.0  # z / 3 keeps inputs near [-1, 1] (z is clipped at +/-10)


class LSTMAutoencoder(nn.Module):
    def __init__(self, n_features: int, hidden: int = 32, latent: int = 8):
        super().__init__()
        self.encoder = nn.LSTM(n_features, hidden, batch_first=True)
        self.to_latent = nn.Linear(hidden, latent)
        self.from_latent = nn.Linear(latent, hidden)
        self.decoder = nn.LSTM(hidden, hidden, batch_first=True)
        self.out = nn.Linear(hidden, n_features)

    def forward(self, x):  # x: (batch, window, features)
        _, (h, _) = self.encoder(x)
        code = self.to_latent(h[-1])
        rep = self.from_latent(code).unsqueeze(1).repeat(1, x.shape[1], 1)
        dec, _ = self.decoder(rep)
        return self.out(dec)


def windows(frame: pd.DataFrame, z: np.ndarray, window: int) -> tuple[np.ndarray, np.ndarray]:
    """(n_windows, window, features) and the reading index each window ends on.

    Windows never span two batches. The first window-1 readings of a batch get no
    window (they are scored once enough history exists).
    """
    xs, ends = [], []
    for _, idx in frame.groupby("batch_id", sort=False).indices.items():
        idx = np.sort(idx)
        for j in range(window - 1, len(idx)):
            xs.append(z[idx[j - window + 1 : j + 1]])
            ends.append(idx[j])
    return np.asarray(xs, dtype=np.float32), np.asarray(ends)


def train(z_windows: np.ndarray, n_features: int, epochs: int = 8, hidden: int = 32, latent: int = 8,
          lr: float = 1e-3, batch_size: int = 256, seed: int = 2026, log=print) -> LSTMAutoencoder:
    torch.manual_seed(seed)
    np.random.seed(seed)
    torch.set_num_threads(max(1, torch.get_num_threads()))
    model = LSTMAutoencoder(n_features, hidden, latent)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    x = torch.from_numpy(z_windows / SCALE)
    gen = torch.Generator().manual_seed(seed)
    for ep in range(epochs):
        perm = torch.randperm(len(x), generator=gen)
        total = 0.0
        model.train()
        for i in range(0, len(x), batch_size):
            b = x[perm[i : i + batch_size]]
            loss = nn.functional.mse_loss(model(b), b)
            opt.zero_grad()
            loss.backward()
            opt.step()
            total += float(loss.detach()) * len(b)
        log(f"epoch {ep + 1}/{epochs}  train mse {total / len(x):.4f}")
    model.eval()
    return model


@torch.no_grad()
def score(model: LSTMAutoencoder, frame: pd.DataFrame, z: np.ndarray, window: int,
          batch_size: int = 2048) -> np.ndarray:
    """Per-reading score: reconstruction MSE of the window ending at that reading (0 before a full window)."""
    w, ends = windows(frame, z, window)
    out = np.zeros(len(frame))
    if not len(w):
        return out
    x = torch.from_numpy(w / SCALE)
    errs = []
    for i in range(0, len(x), batch_size):
        b = x[i : i + batch_size]
        errs.append(((model(b) - b) ** 2).mean(dim=(1, 2)).numpy())
    out[ends] = np.concatenate(errs)
    return out
