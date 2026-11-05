"""Regulation corpus: stable, citable chunk ids; the question set only points at clauses that exist."""

import json
from pathlib import Path

from regulatory.corpus import BARE_SECTION, chunk_eudralex, citation_id, load_chunks

ROOT = Path(__file__).resolve().parent.parent
CHUNKS = load_chunks()
QUESTIONS = json.loads((ROOT / "evals" / "regulation_questions.json").read_text())["questions"]


def test_chunk_ids_are_unique_and_namespaced():
    ids = [c["id"] for c in CHUNKS]
    assert len(ids) == len(set(ids))
    assert all(":" in i for i in ids)


def test_draft_annex_is_always_labelled_draft():
    a22 = [c for c in CHUNKS if "Annex22" in c["doc_id"]]
    assert a22 and all("draft" in c["id"] and c["status"].startswith("DRAFT") for c in a22)


def test_every_question_points_at_existing_clauses():
    valid = {citation_id(c["id"]) for c in CHUNKS}
    for q in QUESTIONS:
        assert q["expected"] and set(q["expected"]) <= valid, q["id"]


def test_split_is_15_dev_50_test():
    assert sum(q["split"] == "dev" for q in QUESTIONS) == 15
    assert sum(q["split"] == "test" for q in QUESTIONS) == 50


def test_chunks_fit_a_small_embedding_model():
    assert max(len(c["text"].split()) for c in CHUNKS) <= 300


def test_chunker_handles_bare_section_numbers_and_principle():
    doc = {"id": "X", "title": "t", "status": "s"}
    text = (
        "Cover\nPrinciple\nThis annex applies to all computerised systems used in GMP.\n"
        "1.\nRisk Management\nRisk management should be applied throughout the lifecycle of the system.\n"
        "4.1 The validation documentation should cover the relevant steps of the life cycle.\n"
    )
    got = {c.clause: c.text for c in chunk_eudralex(doc, text)}
    assert set(got) == {"Principle", "1", "4.1"}
    assert got["1"].startswith("Risk Management")
    assert BARE_SECTION.match("12.") and not BARE_SECTION.match("12.1")
