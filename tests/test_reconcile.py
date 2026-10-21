import pandas as pd

from lakehouse.reconcile import reconcile_table

KEY = ["batch_id", "seq"]


def bronze(rows):
    return pd.DataFrame(rows, columns=["topic", "partition", "offset", "batch_id", "seq"])


def test_everything_accounted_for():
    b = bronze([
        ("t", 0, 1, 1, 1), ("t", 0, 2, 1, 2),
        ("t", 0, 3, 1, 1),            # a replayed duplicate of (1, 1)
        ("t", 0, 4, None, None),      # undecodable, quarantined
        ("t", 0, 5, 2, 1),            # failed the contract, quarantined
    ])  # fmt: skip
    silver = pd.DataFrame({"batch_id": [1, 1], "seq": [1, 2]})
    quarantine = pd.DataFrame({"_topic": ["t", "t"], "_partition": [0, 0], "_offset": [4, 5]})
    r = reconcile_table(b, silver, quarantine, KEY)
    assert r == {
        "bronze_rows": 5,
        "quarantined": 2,
        "in_silver": 2,
        "duplicates_of_silver": 1,
        "unaccounted": 0,
        "silver_rows": 2,
    }


def test_a_silently_dropped_record_is_found():
    b = bronze([("t", 0, 1, 1, 1), ("t", 0, 2, 1, 2)])
    silver = pd.DataFrame({"batch_id": [1], "seq": [1]})  # (1, 2) vanished
    r = reconcile_table(b, silver, None, KEY)
    assert r["unaccounted"] == 1
