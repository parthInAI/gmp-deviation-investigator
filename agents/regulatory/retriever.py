"""Regulatory analyst's retrieval (LlamaIndex): GMP clauses and FDA findings for an investigation.

    python -m agents.regulatory.retriever index        # embed regulatory/chunks.jsonl into pgvector
    python -m agents.regulatory.retriever ask "..."    # top clauses for a question

Retrieval only: this agent never generates text, it returns citable clauses. Embeddings are a
local open model (BAAI/bge-small-en-v1.5 via fastembed), so no text leaves the machine to be
embedded and no API key is needed. Vectors live in Postgres + pgvector (the same database
that will hold the audit log); tests and the dev loop can use an in-memory store.

Settings that change results (model, k, how a chunk is turned into text) are in SETTINGS and
are frozen before the test questions are scored.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from functools import lru_cache

from regulatory.corpus import citation_id, load_chunks

SETTINGS = {
    "embedding_model": "BAAI/bge-small-en-v1.5",
    "query_prefix": "Represent this sentence for searching relevant passages: ",  # bge retrieval instruction
    "candidates": 20,  # hits fetched per retriever before clause-level de-duplication
    "mode": os.environ.get("RETRIEVAL_MODE", "hybrid"),  # "dense" or "hybrid" (dense + BM25, reciprocal rank fusion)
    "rrf_k": 60,
    "table": "regulation_chunks",
}


@dataclass
class Hit:
    cite: str  # citable clause id, e.g. EU-GMP-Ch1:1.4
    chunk_id: str
    title: str
    status: str
    text: str
    score: float

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def chunk_text(c: dict) -> str:
    """What gets embedded: where the clause comes from, then the clause."""
    return f"{c['title']} - {c['clause']}. {c['text']}"


@lru_cache(maxsize=1)
def embed_model():
    from llama_index.embeddings.fastembed import FastEmbedEmbedding

    return FastEmbedEmbedding(model_name=SETTINGS["embedding_model"], cache_dir=os.environ.get("FASTEMBED_CACHE"))


def _nodes():
    from llama_index.core.schema import TextNode

    return [
        TextNode(
            id_=c["id"],
            text=chunk_text(c),
            metadata={"cite": citation_id(c["id"]), "chunk_id": c["id"], "title": c["title"], "status": c["status"]},
            excluded_embed_metadata_keys=["cite", "chunk_id", "title", "status"],
            excluded_llm_metadata_keys=["chunk_id"],
        )
        for c in load_chunks()
    ]


def pg_store():
    from llama_index.vector_stores.postgres import PGVectorStore

    return PGVectorStore.from_params(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        database=os.environ.get("PGDATABASE", "gmpdi"),
        user=os.environ.get("PGUSER", "gmpdi"),
        password=os.environ.get("PGPASSWORD", "gmpdi"),
        table_name=SETTINGS["table"],
        embed_dim=384,
    )


@lru_cache(maxsize=2)
def index(store: str = "auto"):
    """store: 'memory' builds in-process; 'pgvector' reads (or fills) Postgres; 'auto' = pgvector if PGHOST set."""
    from llama_index.core import StorageContext, VectorStoreIndex

    if store == "auto":
        store = "pgvector" if os.environ.get("PGHOST") else "memory"
    if store == "memory":
        return VectorStoreIndex(_nodes(), embed_model=embed_model())
    vs = pg_store()
    return (
        VectorStoreIndex.from_vector_store(vs, embed_model=embed_model())
        if _pg_filled(vs)
        else VectorStoreIndex(
            _nodes(), storage_context=StorageContext.from_defaults(vector_store=vs), embed_model=embed_model()
        )
    )


def _pg_filled(vs) -> bool:
    try:
        from sqlalchemy import text

        vs._initialize()
        with vs._engine.connect() as c:
            n = c.execute(text(f"SELECT count(*) FROM {vs.schema_name}.data_{SETTINGS['table']}")).scalar()
        return n == len(load_chunks())
    except Exception:
        return False


@lru_cache(maxsize=1)
def bm25():
    from llama_index.retrievers.bm25 import BM25Retriever

    return BM25Retriever.from_defaults(nodes=_nodes(), similarity_top_k=SETTINGS["candidates"])


def _ranked(query: str, store: str) -> list:
    dense_retriever = index(store).as_retriever(similarity_top_k=SETTINGS["candidates"])
    dense = dense_retriever.retrieve(SETTINGS["query_prefix"] + query)
    if SETTINGS["mode"] == "dense":
        return dense
    # Reciprocal rank fusion: each list votes 1 / (rrf_k + rank); no score scales to calibrate.
    fused: dict[str, list] = {}
    for hits in (dense, bm25().retrieve(query)):
        for rank, n in enumerate(hits, 1):
            entry = fused.setdefault(n.node.node_id, [n, 0.0])
            entry[1] += 1.0 / (SETTINGS["rrf_k"] + rank)
    out = []
    for n, score in sorted(fused.values(), key=lambda e: -e[1]):
        n.score = score
        out.append(n)
    return out


def retrieve(query: str, k: int = 5, store: str = "auto") -> list[Hit]:
    """Top k distinct clauses for a question (parts of one long clause count once)."""
    out: dict[str, Hit] = {}
    for n in _ranked(query, store):
        m = n.node.metadata
        if m["cite"] not in out:
            text = n.node.get_content()
            out[m["cite"]] = Hit(m["cite"], m["chunk_id"], m["title"], m["status"], text, float(n.score))
        if len(out) == k:
            break
    return list(out.values())


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("index")
    a = sub.add_parser("ask")
    a.add_argument("question")
    a.add_argument("-k", type=int, default=5)
    args = p.parse_args(argv)
    if args.cmd == "index":
        index("pgvector")
        print(f"{len(load_chunks())} chunks in pgvector table data_{SETTINGS['table']}")
        return 0
    for h in retrieve(args.question, args.k):
        print(json.dumps({"cite": h.cite, "score": round(h.score, 3), "status": h.status, "text": h.text[:160]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
