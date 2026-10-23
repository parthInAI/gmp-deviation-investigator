"""The Phase 2 gate: was every injected schema change caught?

    python -m drift.check            # after drift.inject, bronze and silver on the .driftgate topics

For each scenario in out/drift/injection_manifest.json it looks up the injected
keys in silver, quarantine and alerts (read with delta-rs, no Spark):

  control      every record in silver, none in quarantine (no false alarms)
  quarantine   NO record in silver, EVERY record in quarantine with an expected
               reason, and an alert raised for that reason
  rejected     the registry refused the schema, and nothing reached any table

The gate passes only if every scenario passes. Report -> out/gate/phase2_gate.json.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

TABLES = {
    "process_sensor": "batch_id",
    "fda_recall": "recall_number",
}


def _table_for(scenario: dict) -> str:
    return "process_sensor" if "batch_id" in scenario["keys"] else "fda_recall"


def evaluate(manifest: dict, silver: dict, quarantine: dict, alert_reasons: set[str]) -> dict:
    """silver/quarantine: table -> list of row dicts (key column + 'reasons' for quarantine)."""
    rows = []
    for sc in manifest["scenarios"]:
        table = _table_for(sc)
        key = TABLES[table]
        keys = set(sc["keys"][key])
        in_silver = sum(1 for r in silver.get(table, []) if r[key] in keys)
        # Match on the payload key, or on the Kafka key for records that could not be decoded.
        kafka_keys = {str(k) for k in keys}
        quarantined = [r for r in quarantine.get(table, []) if r[key] in keys or r.get("_kafka_key") in kafka_keys]
        reasons = sorted({x for r in quarantined for x in r["reasons"]})
        expected = sc["expected_reasons"]

        def matches(rs, expected=expected):
            return any(x.startswith(e) for x in rs for e in expected)

        signal = sc.get("source_signal")
        at_source = bool(signal and (sc.get("source_evidence") or {}).get(signal))
        if sc["expect"] == "silver":
            ok = in_silver == sc["sent"] and not quarantined and sc["sent"] > 0
            caught_by = "n/a (control)"
        elif sc["expect"] == "quarantine":
            alerted = any(a.startswith(e) for a in alert_reasons for e in expected)
            ok = (
                sc["sent"] > 0
                and in_silver == 0
                and len(quarantined) == sc["sent"]
                and all(matches(r["reasons"]) for r in quarantined)
                and alerted
            )
            caught_by = ("source manifest + " if at_source else "") + "silver contract"
        else:  # rejected_by_registry
            ok = not sc.get("registry", {}).get("accepted", True) and in_silver == 0 and not quarantined
            caught_by = "schema registry (BACKWARD)"
        rows.append(
            {
                "id": sc["id"],
                "title": sc["title"],
                "layer": sc["layer"],
                "expect": sc["expect"],
                "sent": sc["sent"],
                "in_silver": in_silver,
                "quarantined": len(quarantined),
                "reasons": reasons,
                "caught_at_source": at_source,
                "caught_by": caught_by,
                "passed": ok,
            }
        )
    drift = [r for r in rows if r["expect"] != "silver"]
    controls = [r for r in rows if r["expect"] == "silver"]
    return {
        "gate": "Phase 2: injected schema changes are caught",
        "passed": all(r["passed"] for r in rows),
        "drift_scenarios_caught": f"{sum(r['passed'] for r in drift)}/{len(drift)}",
        "controls_clean": f"{sum(r['passed'] for r in controls)}/{len(controls)}",
        "drifted_records_sent": sum(r["sent"] for r in drift),
        "drifted_records_in_silver": sum(r["in_silver"] for r in drift),
        "scenarios": rows,
    }


def _read(path: Path, columns: list[str]) -> list[dict]:
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    try:
        df = DeltaTable(str(path)).to_pandas(columns=columns)
    except TableNotFoundError:
        return []
    out = df.to_dict("records")
    for r in out:
        if "reasons" in r:
            r["reasons"] = list(r["reasons"])
    return out


def load_tables(lakehouse: Path) -> tuple[dict, dict, set[str]]:
    silver = {t: _read(lakehouse / "silver" / t, [k]) for t, k in TABLES.items()}
    quarantine = {t: _read(lakehouse / "quarantine" / t, [k, "_kafka_key", "reasons"]) for t, k in TABLES.items()}
    alerts = {r["reason"] for r in _read(lakehouse / "alerts" / "contract_violations", ["reason"])}
    return silver, quarantine, alerts


def print_report(report: dict) -> None:
    print(f"\n{report['gate']}")
    print(f"{'id':3} {'expect':21} {'sent':>5} {'silver':>6} {'quar.':>5}  {'caught by':34} reasons")
    for r in report["scenarios"]:
        mark = "PASS" if r["passed"] else "FAIL"
        reasons = ", ".join(r["reasons"])[:60]
        print(
            f"{r['id']:3} {r['expect']:21} {r['sent']:5} {r['in_silver']:6} {r['quarantined']:5}  "
            f"{r['caught_by']:34} {reasons}  [{mark}]"
        )
    print(
        f"\ndrift scenarios caught: {report['drift_scenarios_caught']}   controls clean: {report['controls_clean']}   "
        f"drifted records in silver: {report['drifted_records_in_silver']} of {report['drifted_records_sent']}"
    )
    print("GATE PASSED" if report["passed"] else "GATE FAILED")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--manifest", type=Path, default=Path("out/drift/injection_manifest.json"))
    p.add_argument("--lakehouse", type=Path, help="default: the one recorded in the manifest")
    p.add_argument("--report", type=Path, default=Path("out/gate/phase2_gate.json"))
    args = p.parse_args(argv)

    manifest = json.loads(args.manifest.read_text())
    lakehouse = args.lakehouse or Path(manifest["lakehouse"])
    report = evaluate(manifest, *load_tables(lakehouse))
    report["injected_at"] = manifest["injected_at"]
    report["source_file"] = manifest["source_file"]
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2))
    print_report(report)
    print(f"report -> {args.report}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    from lakehouse.cli import exit_now

    exit_now(main())
