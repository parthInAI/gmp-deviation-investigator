"""Synthetic QMS: well-formed, clearly synthetic, consistent references, and no copy of a test fault's timing."""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QMS = ROOT / "qms"
DEVS = [json.loads(line) for line in (QMS / "deviations.jsonl").read_text().splitlines() if line.strip()]
SOPS = {p.stem: p.read_text() for p in (QMS / "sops").glob("*.md")}
TAXONOMY = set(json.loads((ROOT / "evals" / "gate_criteria.json").read_text())["root_cause_taxonomy"])
KEY = json.loads((ROOT / "evals" / "answer_key.json").read_text())["batches"]


def test_every_document_is_marked_synthetic():
    assert len(SOPS) >= 10
    for sid, text in SOPS.items():
        assert "synthetic: true" in text and "SYNTHETIC DOCUMENT" in text, sid
    assert all(d["synthetic"] for d in DEVS)


def test_deviations_are_closed_categorised_and_unique():
    assert len(DEVS) == 40
    assert len({d["id"] for d in DEVS}) == 40
    for d in DEVS:
        assert d["category"] in TAXONOMY, d["id"]
        assert d["status"] == "closed" and d["capa"], d["id"]


def test_every_cited_sop_section_exists():
    for d in DEVS:
        for ref in d["sops"]:
            m = re.match(r"(SOP-[A-Z]+-\d+) §(\d+\.\d+)", ref)
            assert m, ref
            sop, sec = m.groups()
            assert sop in SOPS, (d["id"], sop)
            assert re.search(rf"^{re.escape(sec)} ", SOPS[sop], re.M), (d["id"], ref)


def _hours(text):
    """Process times only ('at 47 h', 'from 128 h', '112–119 h'), not durations ('for 18 h', 'over 6 hours')."""
    out = []
    for a, b in re.findall(r"(\d+(?:\.\d+)?)\s*[–-]\s*(\d+(?:\.\d+)?)\s*h\b", text):
        out += [float(a), float(b)]
    out += [float(h) for h in re.findall(r"\b(?:at|from|by|to|until)\s+(\d+(?:\.\d+)?)\s*h\b", text)]
    return out


def test_no_past_case_copies_a_test_fault_in_both_category_and_timing():
    for d in DEVS:
        hours = _hours(d["symptoms"] + " " + d["investigation"])
        for b, k in KEY.items():
            if d["category"] not in k["accepted_categories"]:
                continue
            for e in k["episodes"]:
                for h in hours:
                    assert not (abs(h - e["start_h"]) <= 3 or abs(h - e["end_h"]) <= 3), (d["id"], b, h)
