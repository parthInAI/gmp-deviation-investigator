import math

import pandas as pd
import pytest

from streaming import indpensim
from streaming.sample_data import HEADERS, make_frame


@pytest.mark.parametrize(
    "header, expected",
    [
        ("Time (h)", "time_h"),
        ("Aeration rate(Fg:L/h)", "aeration_rate_lph"),
        ("Temperature(T:K)", "temperature_k"),
        ("PAA flow(Fpaa:PAA flow (L/h))", "paa_flow_lph"),
        ("Oxygen in percent in off-gas(O2:O2  (%))", "offgas_o2_pct"),
        ("0 - Recipe driven 1 - Operator controlled(Control_ref:Control ref)", "control_ref"),
        ("Fault reference(Fault_ref:Fault ref)", "fault_ref"),
        ("PAT control(PAT_ref:PAT ref)", "pat_ref"),
        ("Fault flag", "fault_flag"),
        ("Batch ID", "batch_id"),
        ("Fault_ref", "fault_ref"),  # exports that already use the bare code
        ("Something new(Zz:units)", None),
    ],
)
def test_canonical_name(header, expected):
    assert indpensim.canonical_name(header) == expected


def test_every_sample_header_maps():
    cmap = indpensim.build_column_map(HEADERS + ["2400", "2399.5"])
    assert not cmap.unknown
    assert cmap.raman == ["2400", "2399.5"]
    assert set(cmap.mapping.values()) == {"time_h", "batch_id", *indpensim.SENSOR_FIELDS, *indpensim.INT_FIELDS}


def test_duplicate_columns_keep_first():
    cmap = indpensim.build_column_map(["Time (h)", "Temperature(T:K)", "Temp again(T:K)"])
    assert cmap.duplicates == ["Temp again(T:K)"]


def test_v3_split_legend_is_repaired():
    header = ["Time (h)", "1- No Raman spec", " 1-Raman spec recorded", "2-PAT control(PAT_ref:PAT ref)", "Batch ID"]
    fixed, note = indpensim.repair_headers(header, n_data_fields=3)
    assert len(fixed) == 3 and indpensim.canonical_name(fixed[1]) == "pat_ref" and fixed[2] == "Batch ID"
    assert "merged" in note


def test_unknown_header_mismatch_is_refused():
    with pytest.raises(ValueError, match="refusing to guess"):
        indpensim.repair_headers(["Time (h)", "a", "b", "c"], n_data_fields=3)


def test_matching_header_is_untouched():
    assert indpensim.repair_headers(["a", "b"], 2) == (["a", "b"], None)


def test_sample_file_reproduces_the_v3_defect(sample_csv):
    import csv

    with open(sample_csv, newline="") as f:
        r = csv.reader(f)
        assert len(next(r)) == len(next(r)) + 2


def test_missing_time_column_is_an_error():
    with pytest.raises(ValueError):
        indpensim.build_column_map(["Temperature(T:K)"])


@pytest.mark.parametrize("chunksize", [7, 25, 1000])  # boundaries inside, at, and beyond a batch
def test_batches_split_on_time_reset_across_chunks(sample_csv, chunksize):
    expected = make_frame(n_batches=10, rows=25, seed=1)
    resets = (expected["Time (h)"].diff().fillna(-1) <= 0).cumsum()
    expected_sizes = resets.value_counts().sort_index().tolist()

    got = list(indpensim.iter_batches(sample_csv, chunksize=chunksize))
    assert [b for b, _ in got] == list(range(1, 11))
    assert [len(f) for _, f in got] == expected_sizes
    for _, frame in got:
        assert frame["time_h"].is_monotonic_increasing
        assert not any(c.isdigit() for c in frame.columns)  # no Raman columns


def test_batch_id_column_must_agree_with_time_resets(tmp_path):
    # e.g. a mislabelled column: IDs change where time does not reset
    df = pd.DataFrame({"Batch ID": [1, 2, 2, 2], "Time (h)": [0.2, 0.4, 0.2, 0.4]})
    path = tmp_path / "bad_ids.csv"
    df.to_csv(path, index=False)
    with pytest.raises(ValueError, match="disagrees with time resets"):
        list(indpensim.iter_batches(path))


def test_batch_ids_must_increase(tmp_path):
    df = pd.DataFrame({"Batch ID": [2, 2, 1, 1], "Time (h)": [0.2, 0.4, 0.2, 0.4]})
    path = tmp_path / "decreasing.csv"
    df.to_csv(path, index=False)
    with pytest.raises(ValueError, match="strictly increasing"):
        list(indpensim.iter_batches(path))


def test_time_reset_split_without_id_column(tmp_path):
    df = pd.DataFrame({"Time (h)": [0.2, 0.4, 0.6, 0.2, 0.4], "Temperature(T:K)": [298.0] * 5})
    path = tmp_path / "no_ids.csv"
    df.to_csv(path, index=False)
    assert [(b, len(f)) for b, f in indpensim.iter_batches(path)] == [(1, 3), (2, 2)]


def test_explicit_batch_id_column_is_used(tmp_path):
    df = pd.DataFrame(
        {"Batch ID": [5, 5, 6, 6], "Time (h)": [0.2, 0.4, 0.2, 0.4], "Temperature(T:K)": [298.0, 298.1, 297.9, 298.0]}
    )
    path = tmp_path / "with_ids.csv"
    df.to_csv(path, index=False)
    got = [(b, len(f)) for b, f in indpensim.iter_batches(path)]
    assert got == [(5, 2), (6, 2)]


def test_records_match_contract_and_clean_nans(sample_csv):
    _, frame = next(indpensim.iter_batches(sample_csv))
    recs = list(indpensim.to_records(1, frame, "r1"))
    assert [r["seq"] for r in recs] == list(range(1, len(frame) + 1))
    assert recs[0]["event_time"] == indpensim.PLANT_EPOCH_MS + round(recs[0]["time_h"] * 3_600_000)
    for r in recs:
        for v in r.values():
            assert not (isinstance(v, float) and math.isnan(v))
    # offline assays are sparse: most rows carry None, a few carry a value
    offline = [r["penicillin_offline_gpl"] for r in recs]
    assert any(v is None for v in offline) and any(v is not None for v in offline)


def test_event_time_is_deterministic_per_batch(sample_csv):
    batches = dict(indpensim.iter_batches(sample_csv))
    a = next(indpensim.to_records(3, batches[3], "run-a"))
    b = next(indpensim.to_records(3, batches[3], "run-b"))
    assert a["event_time"] == b["event_time"]
    later = next(indpensim.to_records(4, batches[4], "run-a"))
    assert later["event_time"] - a["event_time"] == pytest.approx(240 * 3_600_000, abs=3_600_000)
