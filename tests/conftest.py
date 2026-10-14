from __future__ import annotations

import pytest

from streaming.sample_data import make_frame, write_csv


@pytest.fixture
def sample_csv(tmp_path):
    """10 small batches in the real IndPenSim V3 layout (broken header included); batch 10 has a fault."""
    path = tmp_path / "sample.csv"
    write_csv(make_frame(n_batches=10, rows=25, seed=1), path)
    return path
