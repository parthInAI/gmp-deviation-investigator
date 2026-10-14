"""Phase 1 gate: did all 100 batches replay into bronze, completely and decodably?

    python -m lakehouse.gate_check --lakehouse /lakehouse --manifest out/manifests/latest_replay.json

Reads the bronze Delta table with delta-rs (no Spark needed) and compares it
against the replayer's manifest. Only rows from the manifest's replay_id count,
so an earlier test replay sitting in the same table cannot fake a pass.

Checks:
  1. every bronze row decoded (decode_ok)
  2. the expected number of distinct batches arrived (default 100)
  3. each batch has exactly the readings the replayer sent, by distinct seq
  4. the fault batches seen in bronze match the ones sent (91-100 in IndPenSim)
Duplicates (same batch_id + seq twice) are reported, not failed: bronze is
allowed to hold them; silver deduplicates.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pyarrow.compute as pc
from deltalake import DeltaTable


def load_bronze(table_path: str):
    ds = DeltaTable(table_path).to_pyarrow_dataset()
    return ds.to_table(
        columns={
            "decode_ok": pc.field("decode_ok"),
            "replay_id": pc.field("payload", "replay_id"),
            "batch_id": pc.field("payload", "batch_id"),
            "seq": pc.field("payload", "seq"),
            "fault_ref": pc.field("payload", "fault_ref"),
        }
    ).to_pandas()


def check(df, manifest: dict | None, expect_batches: int) -> dict:
    failures: list[str] = []
    undecoded = int((~df["decode_ok"].astype(bool)).sum())
    if undecoded:
        failures.append(f"{undecoded} bronze rows failed to decode")

    ok = df[df["decode_ok"].astype(bool)]
    if manifest:
        ok = ok[ok["replay_id"] == manifest["replay_id"]]
    if ok.empty:
        failures.append("no decoded rows for this replay in bronze")

    distinct = ok.drop_duplicates(["batch_id", "seq"])
    per_batch = distinct.groupby("batch_id")["seq"].count().to_dict()
    duplicates = len(ok) - len(distinct)

    if len(per_batch) != expect_batches:
        failures.append(f"expected {expect_batches} batches, found {len(per_batch)}")

    mismatched = {}
    if manifest:
        sent = {int(k): v for k, v in manifest["rows_per_batch"].items()}
        for b, n in sent.items():
            got = per_batch.get(b, 0)
            if got != n:
                mismatched[b] = {"sent": n, "in_bronze": got}
        extra = sorted(set(per_batch) - set(sent))
        if extra:
            failures.append(f"batches in bronze that were not sent: {extra[:10]}")
        if mismatched:
            failures.append(f"{len(mismatched)} batches incomplete, e.g. {dict(list(mismatched.items())[:3])}")

    fault_seen = sorted(int(b) for b in ok.loc[ok["fault_ref"].fillna(0) > 0, "batch_id"].unique())
    if manifest and fault_seen != sorted(manifest.get("fault_batches", [])):
        failures.append(f"fault batches differ: sent {manifest.get('fault_batches')}, bronze {fault_seen}")

    return {
        "passed": not failures,
        "failures": failures,
        "replay_id": manifest["replay_id"] if manifest else None,
        "batches_in_bronze": len(per_batch),
        "readings_in_bronze": int(len(distinct)),
        "readings_sent": manifest["total_rows"] if manifest else None,
        "duplicate_rows": int(duplicates),
        "undecoded_rows": undecoded,
        "fault_batches": fault_seen,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lakehouse", default="/lakehouse")
    p.add_argument("--table", default="bronze/process_sensor")
    p.add_argument("--manifest", type=Path, default=Path("out/manifests/latest_replay.json"))
    p.add_argument("--expect-batches", type=int, default=100)
    p.add_argument("--report", type=Path, default=Path("out/gate/phase1_gate.json"))
    args = p.parse_args(argv)

    manifest = json.loads(args.manifest.read_text()) if args.manifest.exists() else None
    if manifest is None:
        print(f"warning: no manifest at {args.manifest}; checking batch count only", file=sys.stderr)
    result = check(load_bronze(f"{args.lakehouse}/{args.table}"), manifest, args.expect_batches)

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    print("\nPHASE 1 GATE:", "PASS" if result["passed"] else "FAIL")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
