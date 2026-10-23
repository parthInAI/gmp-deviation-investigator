"""Phase 3 detection model: alarm logic, features, PCA, synthetic faults, frozen artifacts, gate rules."""

import json
import shutil

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("torch")

from models import alarms as A  # noqa: E402
from models.data import FEATURES, MONITORED, load_split  # noqa: E402
from models.detector import ARTIFACT_DIR, Detector  # noqa: E402
from models.episodes import episodes  # noqa: E402
from models.features import Z_CLIP, TimeProfile, smooth_causal  # noqa: E402
from models.pca_monitor import PCAMonitor  # noqa: E402
from models.synthetic import inject  # noqa: E402


def frame(batches=3, n=50, seed=0, feats=("a", "b", "c")):
    rng = np.random.default_rng(seed)
    rows = []
    for b in range(1, batches + 1):
        for i in range(n):
            base = {"a": i * 1.0, "b": 10 - i * 0.1, "c": 5.0}
            rows.append(
                {
                    "batch_id": b,
                    "seq": i + 1,
                    "time_h": (i + 1) * 0.2,
                    "step": i + 1,
                    "fault_ref": 0,
                    **{f: base[f] + rng.normal(0, 0.1) for f in feats},
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- alarms


def test_run_alarm_needs_consecutive_exceedances():
    e = np.array([1, 1, 0, 1, 1, 1, 1, 0, 1], dtype=bool)
    assert A.run_alarm(e, 3).tolist() == [0, 0, 0, 0, 0, 1, 1, 0, 0]
    assert A.run_alarm(e, 1).tolist() == e.tolist()


def test_runs_never_span_batches():
    f = pd.DataFrame({"batch_id": [1, 1, 2, 2], "time_h": [0.2, 0.4, 0.2, 0.4]})
    assert A.alarms(f, np.array([5, 5, 5, 5.0]), 1, 3).tolist() == [False] * 4
    assert A.alarms(f, np.array([5, 5, 5, 5.0]), 1, 2).tolist() == [False, True, False, True]


def test_batch_events_and_summary():
    f = pd.DataFrame(
        {
            "batch_id": [1] * 4 + [2] * 4 + [3] * 4,
            "time_h": [1, 2, 3, 4] * 3,
            "fault_ref": [0, 0, 1, 1] + [0, 0, 1, 1] + [0] * 4,
        }
    )
    alarm = np.array([0, 0, 0, 1] + [1, 0, 0, 0] + [0, 0, 1, 0], dtype=bool)
    ev = A.batch_events(f, alarm).set_index("batch_id")
    assert ev.loc[1, "detected"] and ev.loc[1, "delay_h"] == 1.0 and not ev.loc[1, "alarm_before_onset"]
    assert not ev.loc[2, "detected"] and ev.loc[2, "alarm_before_onset"]
    s = A.summarise(ev.reset_index(), [1, 2], [3])
    assert s["fault_batches_detected"] == 1
    assert s["false_alarm_batches"] == 2  # normal batch 3 + batch 2 alarming before its fault


# ---------------------------------------------------------------- features


def test_profile_centres_normal_batches_and_clips():
    tr = frame(batches=25)
    prof = TimeProfile.fit(tr, ("a", "b", "c"), min_batches=20, smooth=0)
    z = prof.transform(tr)
    assert abs(z.mean()) < 0.05 and z.std() < 1.3
    assert 0.8 < z[:, 2].std() < 1.2  # c has no trend: the floor does not bind, z is ~N(0, 1)
    bad = tr.copy()
    bad["a"] += 1e6
    assert prof.transform(bad).max() == Z_CLIP


def test_profile_spread_floor_avoids_division_by_zero():
    tr = frame(batches=25)
    tr["c"] = 5.0  # identical in every batch at every step
    prof = TimeProfile.fit(tr, ("a", "b", "c"), min_batches=20)
    assert np.isfinite(prof.transform(tr)).all()


def test_coverage_stops_where_training_support_ends():
    tr = pd.concat([frame(batches=25, n=50), frame(batches=5, n=80).assign(batch_id=lambda d: d.batch_id + 100)])
    prof = TimeProfile.fit(tr, ("a", "b", "c"), min_batches=20)
    assert prof.last_reliable == 50
    late = frame(batches=1, n=80)
    assert prof.in_coverage(late).sum() == 50


def test_smoothing_is_causal():
    f = frame(batches=1, n=20)
    f.loc[10, "a"] = 1e6
    s = smooth_causal(f, ("a",), 5)
    assert s.loc[:9, "a"].equals(smooth_causal(f.iloc[:10], ("a",), 5).loc[:9, "a"])  # past unaffected by future
    assert s.loc[10, "a"] < 1e3  # a one-reading spike does not survive a median


# ---------------------------------------------------------------- PCA


def test_pca_t2_and_spe():
    rng = np.random.default_rng(1)
    latent = rng.normal(size=(2000, 2))
    z = latent @ rng.normal(size=(2, 6)) + rng.normal(0, 0.05, size=(2000, 6))
    m = PCAMonitor.fit(z, 0.95)
    assert m.k == 2
    normal = m.scores(z)
    off = z[:5] + np.array([0, 0, 0, 0, 0, 3.0])  # breaks the correlation structure
    assert m.scores(off)["spe"].min() > np.quantile(normal["spe"], 0.999)
    contrib = m.contributions(off)["spe"]
    assert (contrib.argmax(axis=1) == 5).all()  # the broken signal is named


# ---------------------------------------------------------------- synthetic faults


@pytest.mark.parametrize("kind", ["step", "drift", "stuck"])
def test_inject_marks_ground_truth_and_changes_only_one_batch(kind):
    f = frame(batches=2)
    g = inject(f, 1, "a", kind, onset_h=4.0, size=3.0, spread=1.0)
    after = (g.batch_id == 1) & (g.time_h >= 4.0)
    assert (g.loc[after, "fault_ref"] == 1).all() and (g.loc[~after, "fault_ref"] == 0).all()
    assert g.loc[g.batch_id == 2, "a"].equals(f.loc[f.batch_id == 2, "a"])
    assert not g.loc[after, "a"].equals(f.loc[after, "a"])


def test_inject_after_batch_end_is_a_no_op():
    f = frame(batches=1)
    assert inject(f, 1, "a", "stuck", onset_h=999, size=2, spread=1).equals(f)


def test_episodes_split_on_gaps():
    f = pd.DataFrame({"batch_id": [9] * 8, "time_h": np.arange(1, 9) * 1.0, "fault_ref": [0, 1, 1, 0, 0, 1, 0, 1]})
    e = episodes(f)
    assert e[["start_h", "end_h"]].values.tolist() == [[2, 3], [6, 6], [8, 8]]


# ---------------------------------------------------------------- gate rules and frozen artifacts


def test_split_is_disjoint_complete_and_holds_out_faults():
    s = load_split()
    ids = [b for k in s for b in s[k]]
    assert sorted(ids) == list(range(1, 101))
    assert s["test_fault"] == list(range(91, 101))
    assert len(s["train"]) == 60 and len(s["calibration"]) == 15 and len(s["test_normal"]) == 15


def test_labels_and_flags_are_never_features():
    forbidden = {
        "fault_ref",
        "fault_flag",
        "control_ref",
        "pat_ref",
        "batch_ref",
        "batch_id",
        "substrate_conc_gpl",
        "penicillin_conc_gpl",
    }
    assert not forbidden & set(FEATURES)
    assert set(MONITORED) <= set(FEATURES)


@pytest.mark.parametrize("name", ["pca", "lstm_ae"])
def test_frozen_artifacts_load_and_match_selection(name):
    det = Detector.load(ARTIFACT_DIR / name)
    sel = json.loads((ARTIFACT_DIR / "selection.json").read_text())
    assert det.meta["files"] == sel["manifests"][name]
    assert not set(det.meta["train_batches"]) & set(range(91, 101))
    assert det.features == MONITORED


def test_tampered_artifact_is_refused(tmp_path):
    shutil.copytree(ARTIFACT_DIR / "pca", tmp_path / "pca")
    cfg = json.loads((tmp_path / "pca" / "config.json").read_text())
    cfg["persistence"] = 1  # quietly make it more sensitive
    (tmp_path / "pca" / "config.json").write_text(json.dumps(cfg))
    with pytest.raises(RuntimeError, match="SHA-256"):
        Detector.load(tmp_path / "pca")
