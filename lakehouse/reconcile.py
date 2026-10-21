"""Reconcile bronze against silver + quarantine: no record may disappear.

    python -m lakehouse.reconcile [--lakehouse /lakehouse]

Every bronze row must be accounted for in exactly one of three ways:

  quarantined   its (topic, partition, offset) is in the quarantine table
  in silver     it passed the contract and its key is in silver
  duplicate     it passed, and an identical key is already in silver (a replay)

"unaccounted" must be 0. That is the data-integrity claim behind the pipeline
(ALCOA+: complete): a record is never silently dropped between layers. Exit
code 1 if anything is unaccounted, so the Airflow task fails loudly.

Reads Delta with delta-rs (no Spark), so it runs in the small app image.
Writes out/quality/reconcile.json.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

TABLES = {"process_sensor": ["batch_id", "seq"], "fda_recall": ["recall_number"]}


def _load(path: Path, columns: list[str]) -> pd.DataFrame | None:
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    try:
        return DeltaTable(str(path)).to_pandas(columns=columns)
    except TableNotFoundError:
        return None


def reconcile_table(bronze: pd.DataFrame, silver: pd.DataFrame | None, quarantine: pd.DataFrame | None, key) -> dict:
    """bronze: topic, partition, offset + key columns; quarantine: _topic, _partition, _offset."""
    pos = ["topic", "partition", "offset"]
    b = bronze.drop_duplicates(pos)
    if quarantine is not None and len(quarantine):
        q = quarantine.rename(columns={"_topic": "topic", "_partition": "partition", "_offset": "offset"})[pos]
        b = b.merge(q.drop_duplicates().assign(_q=True), on=pos, how="left")
    else:
        b = b.assign(_q=False)
    b["_q"] = b["_q"].fillna(False).astype(bool)
    passed = b[~b["_q"]]
    if silver is not None and len(silver):
        passed = passed.merge(silver[key].drop_duplicates().assign(_s=True), on=key, how="left")
        passed["_s"] = passed["_s"].fillna(False).astype(bool)
    else:
        passed = passed.assign(_s=False)
    in_silver_keys = passed[passed["_s"]]
    unique_in_silver = len(in_silver_keys.drop_duplicates(key))
    return {
        "bronze_rows": int(len(b)),
        "quarantined": int(b["_q"].sum()),
        "in_silver": unique_in_silver,
        "duplicates_of_silver": int(len(in_silver_keys) - unique_in_silver),
        "unaccounted": int((~passed["_s"]).sum()),
        "silver_rows": 0 if silver is None else int(len(silver)),
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lakehouse", type=Path, default=Path("/lakehouse"))
    p.add_argument("--report", type=Path, default=Path("out/quality/reconcile.json"))
    args = p.parse_args(argv)
    lh = args.lakehouse

    report = {"lakehouse": str(lh), "checked_at": datetime.now(UTC).isoformat(), "tables": {}}
    for table, key in TABLES.items():
        bronze = _load(lh / "bronze" / table, ["topic", "partition", "offset", "payload"])
        if bronze is None:
            continue
        keys = pd.json_normalize(bronze["payload"].map(lambda x: x or {}).tolist())
        bronze = pd.concat([bronze[["topic", "partition", "offset"]], keys.reindex(columns=key)], axis=1)
        silver = _load(lh / "silver" / table, key)
        quarantine = _load(lh / "quarantine" / table, ["_topic", "_partition", "_offset"])
        report["tables"][table] = reconcile_table(bronze, silver, quarantine, key)

    report["passed"] = bool(report["tables"]) and all(
        t["unaccounted"] == 0 and t["in_silver"] == t["silver_rows"] for t in report["tables"].values()
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2))
    for table, t in report["tables"].items():
        print(
            f"{table:15} bronze {t['bronze_rows']:>7} = silver {t['in_silver']:>7} + duplicates "
            f"{t['duplicates_of_silver']:>5} + quarantined {t['quarantined']:>5} + unaccounted {t['unaccounted']}"
        )
    print("RECONCILED: every bronze record is accounted for" if report["passed"] else "RECONCILIATION FAILED")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
