"""Phase 4 pre-registration: the answer key and the gate agree with each other and with Phase 3's split."""

import json
from pathlib import Path

from models.data import load_split

EVALS = Path(__file__).resolve().parent.parent / "evals"
KEY = json.loads((EVALS / "answer_key.json").read_text())
GATE = json.loads((EVALS / "gate_criteria.json").read_text())


def test_key_covers_exactly_the_test_fault_batches():
    assert sorted(int(b) for b in KEY["batches"]) == sorted(load_split()["test_fault"])


def test_accepted_categories_are_in_the_taxonomy():
    taxonomy = set(GATE["root_cause_taxonomy"])
    for b, k in KEY["batches"].items():
        assert k["accepted_categories"], b
        assert set(k["accepted_categories"]) <= taxonomy, b
        assert k["strict_category"] == k["accepted_categories"][0]
        assert len(k["accepted_categories"]) <= 2  # keeps top-3 from being trivial


def test_every_batch_has_one_consistent_fault_type():
    for b, k in KEY["batches"].items():
        assert k["consistent_across_episodes"], b
        assert k["episodes"], b


def test_gate_bars_are_the_agreed_ones():
    c = GATE["criteria"]
    assert c["root_cause_top3"]["pass_if_at_least"] == 7
    assert c["citation_validity"]["pass_if_at_least"] == 0.95
    assert c["regulation_recall_at_5"]["pass_if_at_least"] == 0.8
    assert c["cost_per_investigation"]["pass_if_every_run_at_most_eur"] == 0.5
