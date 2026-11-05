"""Phase 4 evaluation: investigations scored against the answer key, citations re-checked independently.

    python -m evals.investigations --split dev                 # development: injected faults in calibration batches
    python -m evals.investigations --split test --gate   # THE GATE: batches 91-100 once + regulation test questions

Scoring (evals/gate_criteria.json):
  root cause      an accepted category among the report's top 3 hypotheses (also: strict key, top-1)
  citations       every citation of every claim in the final report re-checked here, not by the pipeline:
                  SENSOR windows against the historian data, other ids against what that run retrieved
  human stop      the run ends awaiting approval, and an attempted release is refused
  cost            each run at or under EUR 0.50, at the ECB EUR/USD rate on the run date
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GATE = json.loads((ROOT / "evals" / "gate_criteria.json").read_text())
ALTERNATES = {
    "ph_measurement": ["ph_measurement", "ph_dosing"],
    "coolant_supply": ["coolant_supply", "temperature_measurement"],
}


def ecb_usd_to_eur() -> tuple[float, str]:
    """EUR per USD from the ECB daily reference rates (falls back to GMPDI_USD_EUR, flagged)."""
    try:
        url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
        xml = urllib.request.urlopen(url, timeout=20).read().decode()
        import re

        day = re.search(r"time='([\d-]+)'", xml).group(1)
        usd = float(re.search(r"currency='USD' rate='([\d.]+)'", xml).group(1))
        return 1 / usd, f"ECB reference rate {day}: 1 EUR = {usd} USD"
    except Exception as e:
        return float(os.environ.get("GMPDI_USD_EUR", "0.92")), f"fallback GMPDI_USD_EUR (ECB unavailable: {e})"


def answer_key(split: str) -> dict[int, dict]:
    if split == "test":
        key = json.loads((ROOT / "evals" / "answer_key.json").read_text())["batches"]
        return {int(b): {"accepted": k["accepted_categories"], "strict": k["strict_category"]} for b, k in key.items()}
    key = json.loads((ROOT / "out" / "dev_cases.key.json").read_text())
    out = {}
    for b, k in key.items():
        c = k["category"]
        out[int(b)] = {"accepted": ALTERNATES.get(c, [c]) if c else [], "strict": c}
    return out


def recheck_citations(result: dict) -> dict:
    from agents.citations import all_claims, kind
    from agents.schemas import Report
    from mcp_servers.historian import core as H

    if not result.get("report"):
        return {"claims": 0, "valid_claims": 0, "invalid": []}
    retrieved = set(result.get("retrieved") or [])
    claims = all_claims(Report(**result["report"]))
    invalid = []
    for where, cl in claims:
        bad = [
            c
            for c in cl.citations
            if not (
                H.check_sensor_citation(c).valid if kind(c) == "sensor" else (kind(c) != "invalid" and c in retrieved)
            )
        ]
        if bad or not cl.citations:
            invalid.append({"where": where, "text": cl.text, "bad": bad})
    return {"claims": len(claims), "valid_claims": len(claims) - len(invalid), "invalid": invalid}


def score(result: dict, key: dict) -> dict:
    hyps = sorted((result.get("report") or {}).get("hypotheses", []), key=lambda h: h["rank"])
    cats = [h["category"] for h in hyps]
    return {
        "categories": cats,
        "top3": any(c in key["accepted"] for c in cats[:3]),
        "top3_strict": key["strict"] in cats[:3],
        "top1": bool(cats) and cats[0] in key["accepted"],
    }


async def run_all(batches: list[int], analyst: str, offline: bool) -> list[dict]:
    from agents.run import investigate

    out = []
    for b in batches:
        r = await investigate(b, analyst=analyst, offline=offline)
        print(
            f"batch {b}: {r['status']} {[h['category'] for h in (r['report'] or {}).get('hypotheses', [])]} "
            f"EUR {r['cost']['eur']} {r['seconds']} s" + (f" ERROR {r['error']}" if r["error"] else "")
        )
        out.append(r)
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--split", choices=["dev", "test"], required=True)
    p.add_argument("--gate", action="store_true", help="test split only: also score regulation questions and decide")
    p.add_argument("--analyst", choices=["llm", "rules"], default="llm")
    p.add_argument("--offline", action="store_true")
    p.add_argument("--batches", type=int, nargs="*")
    args = p.parse_args(argv)
    if args.gate and args.split != "test":
        p.error("--gate is for the test split")

    rate, rate_note = ecb_usd_to_eur()
    os.environ["GMPDI_USD_EUR"] = str(rate)
    keys = answer_key(args.split)
    batches = args.batches or sorted(keys)
    results = asyncio.run(run_all(batches, args.analyst, args.offline))

    rows, faults = [], [b for b in batches if keys[b]["accepted"]]
    for r in results:
        k = keys[r["batch_id"]]
        cit = recheck_citations(r)
        rows.append(
            {
                "batch_id": r["batch_id"],
                "thread": r["thread"],
                "expected": k["accepted"],
                **score(r, k),
                "status": r["status"],
                "error": r["error"],
                "stopped_for_approval": r["status"] == "awaiting_approval" and not r["released_without_approval"],
                "citations": cit,
                "draft_validity": [s["draft_validity"] for s in (r.get("strip_log") or [])],
                "claims_stripped": sum(len(s["removed"]) for s in (r.get("strip_log") or [])),
                "revisions": r["revisions"],
                "cost_eur": round(r["cost"]["usd"] * rate, 4),
                "seconds": r["seconds"],
            }
        )
    fr = [x for x in rows if x["batch_id"] in faults]
    claims = sum(x["citations"]["claims"] for x in rows)
    valid = sum(x["citations"]["valid_claims"] for x in rows)
    summary = {
        "split": args.split,
        "analyst": args.analyst,
        "evaluated_at": datetime.now(UTC).isoformat(),
        "usd_to_eur": rate,
        "rate_source": rate_note,
        "root_cause_top3": sum(x["top3"] for x in fr),
        "root_cause_top3_strict": sum(x["top3_strict"] for x in fr),
        "root_cause_top1": sum(x["top1"] for x in fr),
        "fault_investigations": len(fr),
        "citation_validity": round(valid / claims, 4) if claims else None,
        "claims": claims,
        "stopped_for_approval": sum(x["stopped_for_approval"] for x in fr),
        "max_cost_eur": max((x["cost_eur"] for x in rows), default=0),
        "mean_cost_eur": round(sum(x["cost_eur"] for x in rows) / len(rows), 4) if rows else 0,
        "mean_seconds": round(sum(x["seconds"] for x in rows) / len(rows), 1) if rows else 0,
        "errors": [x["batch_id"] for x in rows if x["error"]],
        "per_batch": rows,
    }
    out_dir = ROOT / "out" / "gate"
    out_dir.mkdir(parents=True, exist_ok=True)
    name = "phase4_gate.json" if args.gate else f"phase4_{args.split}_{datetime.now(UTC):%Y%m%dT%H%M%S}.json"
    print(
        f"\nroot cause in top 3: {summary['root_cause_top3']}/{len(fr)} (strict {summary['root_cause_top3_strict']}, "
        f"top-1 {summary['root_cause_top1']}); citations valid {summary['citation_validity']} of {claims} claims; "
        f"stopped for approval {summary['stopped_for_approval']}/{len(fr)}; max cost EUR {summary['max_cost_eur']}"
    )
    rc = 0
    if args.gate:
        from evals.retrieval import run as retrieval_run

        reg = retrieval_run("test")
        c = GATE["criteria"]
        checks = {
            "root_cause_top3": summary["root_cause_top3"] >= c["root_cause_top3"]["pass_if_at_least"],
            "citation_validity": (summary["citation_validity"] or 0) >= c["citation_validity"]["pass_if_at_least"],
            "regulation_recall_at_5": reg["recall_at_5"] >= c["regulation_recall_at_5"]["pass_if_at_least"],
            "stops_for_human_approval": summary["stopped_for_approval"] == len(fr),
            "cost_per_investigation": summary["max_cost_eur"]
            <= c["cost_per_investigation"]["pass_if_every_run_at_most_eur"],
        }
        summary.update({"regulation_retrieval": reg, "criteria_met": checks, "passed": all(checks.values())})
        print(
            f"regulation Recall@5 {reg['recall_at_5']} (Recall@1 {reg['recall_at_1']}) "
            f"on {reg['questions']} test questions"
        )
        for k, v in checks.items():
            print(f"  {k:<26} {'PASS' if v else 'FAIL'}")
        print(f"PHASE 4 GATE: {'PASS' if summary['passed'] else 'FAIL'}")
        rc = 0 if summary["passed"] else 1
    (out_dir / name).write_text(json.dumps(summary, indent=2, default=str))
    print(f"report -> {out_dir / name}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
