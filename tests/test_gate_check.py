"""Gate check against real Delta tables written with delta-rs, in bronze's shape."""

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from lakehouse import gate_check

PAYLOAD = pa.struct(
    [("batch_id", pa.int32()), ("seq", pa.int32()), ("fault_ref", pa.int32()), ("replay_id", pa.string())]
)


def write_bronze(path, rows):
    """rows: list of (batch_id, seq, fault_ref, replay_id, decode_ok)."""
    payloads = [
        None if not ok else {"batch_id": b, "seq": s, "fault_ref": f, "replay_id": r} for b, s, f, r, ok in rows
    ]
    table = pa.table(
        {"payload": pa.array(payloads, type=PAYLOAD), "decode_ok": pa.array([ok for *_, ok in rows], pa.bool_())}
    )
    write_deltalake(str(path), table, mode="append")


def manifest(batches, rows_each, faults, replay_id="r1"):
    return {
        "replay_id": replay_id,
        "total_rows": len(batches) * rows_each,
        "rows_per_batch": {str(b): rows_each for b in batches},
        "fault_batches": faults,
    }


def full_rows(batches, rows_each, faults, replay_id="r1"):
    return [(b, s, int(b in faults), replay_id, True) for b in batches for s in range(1, rows_each + 1)]


@pytest.fixture
def table(tmp_path):
    return tmp_path / "bronze" / "process_sensor"


def run(table, m, expect):
    return gate_check.check(gate_check.load_bronze(str(table)), m, expect)


def test_pass_when_everything_arrived(table):
    batches, faults = range(1, 11), [9, 10]
    write_bronze(table, full_rows(batches, 5, faults))
    r = run(table, manifest(batches, 5, faults), 10)
    assert r["passed"], r["failures"]
    assert r["batches_in_bronze"] == 10 and r["readings_in_bronze"] == 50 and r["fault_batches"] == [9, 10]


def test_duplicates_are_reported_not_failed(table):
    rows = full_rows(range(1, 4), 3, [])
    write_bronze(table, rows + rows[:2])
    r = run(table, manifest(range(1, 4), 3, []), 3)
    assert r["passed"] and r["duplicate_rows"] == 2


def test_missing_batch_fails(table):
    write_bronze(table, full_rows(range(1, 10), 5, []))
    r = run(table, manifest(range(1, 11), 5, []), 10)
    assert not r["passed"]
    assert any("expected 10 batches, found 9" in f for f in r["failures"])


def test_incomplete_batch_fails(table):
    rows = [r for r in full_rows(range(1, 4), 5, []) if not (r[0] == 2 and r[1] == 5)]
    write_bronze(table, rows)
    r = run(table, manifest(range(1, 4), 5, []), 3)
    assert not r["passed"] and any("incomplete" in f for f in r["failures"])


def test_undecodable_rows_fail(table):
    write_bronze(table, full_rows(range(1, 3), 2, []) + [(None, None, None, None, False)])
    r = run(table, manifest(range(1, 3), 2, []), 2)
    assert not r["passed"] and r["undecoded_rows"] == 1


def test_other_replays_in_the_table_do_not_count(table):
    write_bronze(table, full_rows(range(1, 6), 4, [], replay_id="old"))
    write_bronze(table, full_rows(range(1, 3), 4, [], replay_id="new"))
    r = run(table, manifest(range(1, 6), 4, [], replay_id="new"), 5)
    assert not r["passed"] and r["batches_in_bronze"] == 2


def test_wrong_fault_batches_fail(table):
    write_bronze(table, full_rows(range(1, 4), 2, [3]))
    r = run(table, manifest(range(1, 4), 2, [2, 3]), 3)
    assert not r["passed"] and any("fault batches differ" in f for f in r["failures"])
