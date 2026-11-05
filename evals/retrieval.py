"""Regulation retrieval eval: Recall@1 and Recall@5 on the pre-registered questions.

    python -m evals.retrieval --split dev     # tuning loop: dev questions only
    python -m evals.retrieval --split test    # the gate: once, after SETTINGS are frozen

A question counts as found if any of its expected clauses is among the top k distinct clauses.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

QUESTIONS = Path(__file__).parent / "regulation_questions.json"
GATE = json.loads((Path(__file__).parent / "gate_criteria.json").read_text())


def run(split: str, store: str = "auto", k: int = 5) -> dict:
    from agents.regulatory.retriever import SETTINGS, retrieve

    qs = [q for q in json.loads(QUESTIONS.read_text())["questions"] if q["split"] == split]
    rows = []
    for q in qs:
        hits = [h.cite for h in retrieve(q["question"], k=k, store=store)]
        rank = next((i + 1 for i, c in enumerate(hits) if c in q["expected"]), None)
        rows.append({"id": q["id"], "expected": q["expected"], "rank": rank, "top5": hits})
    n = len(rows)
    r1 = sum(r["rank"] == 1 for r in rows) / n
    r5 = sum(r["rank"] is not None and r["rank"] <= 5 for r in rows) / n
    return {
        "split": split,
        "questions": n,
        "recall_at_1": round(r1, 3),
        "recall_at_5": round(r5, 3),
        "settings": SETTINGS,
        "evaluated_at": datetime.now(UTC).isoformat(),
        "per_question": rows,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--split", choices=["dev", "test"], required=True)
    p.add_argument("--store", default="auto", choices=["auto", "memory", "pgvector"])
    p.add_argument("--out", type=Path)
    args = p.parse_args(argv)
    res = run(args.split, args.store)
    for r in res["per_question"]:
        if r["rank"] != 1:
            print(f"{r['id']}: rank {r['rank']}  expected {r['expected']}  got {r['top5'][:3]}")
    r1, r5, n = res["recall_at_1"], res["recall_at_5"], res["questions"]
    print(f"{args.split}: Recall@1 {r1:.2f}  Recall@5 {r5:.2f}  ({n} questions)")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(res, indent=2))
    if args.split == "test":
        bar = GATE["criteria"]["regulation_recall_at_5"]["pass_if_at_least"]
        print(f"criterion: Recall@5 >= {bar}: {'PASS' if res['recall_at_5'] >= bar else 'FAIL'}")
        return 0 if res["recall_at_5"] >= bar else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
