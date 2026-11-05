"""Regulatory retrieval: hybrid search returns distinct citable clauses; dev recall does not regress."""

import pytest

pytest.importorskip("llama_index.core")
pytest.importorskip("llama_index.retrievers.bm25")
pytest.importorskip("fastembed")

from agents.regulatory.retriever import SETTINGS, retrieve  # noqa: E402
from evals.retrieval import run  # noqa: E402


def test_hits_are_distinct_clauses_with_status():
    hits = retrieve("Can a large language model be used in a critical GMP application?", k=5, store="memory")
    assert len({h.cite for h in hits}) == len(hits) == 5
    assert hits[0].cite == "EU-GMP-Annex22-draft:1"
    assert hits[0].status.startswith("DRAFT")


def test_frozen_settings_hold_dev_recall():
    assert SETTINGS["mode"] == "hybrid"
    res = run("dev", store="memory")
    assert res["recall_at_5"] >= 0.93  # 1.00 when frozen; dev only, the test split is scored once at the gate
