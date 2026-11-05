"""QMS: read-only search over the synthetic SOPs and closed deviations (qms/).

Search is BM25 over plain words: deterministic, no model, no network, easy to test. Each SOP is
split into its numbered sections so a hit is directly citable:

    SOP-FER-001 §4.4        a section of an SOP
    DEV-2024-007            a closed deviation record (with its CAPA)
"""

from __future__ import annotations

import json
import math
import os
import re
from collections import Counter
from functools import lru_cache
from pathlib import Path

QMS_DIR = Path(os.environ.get("QMS_DIR", Path(__file__).resolve().parents[2] / "qms"))
WORD = re.compile(r"[a-z0-9]+(?:\.[0-9]+)?")
STOP = set("a an and are as at be by for from has have in is it its of on or that the this to was were with".split())
SEC = re.compile(r"^(\d+\.\d+) (.+)$")
CITE_SOP = re.compile(r"^(SOP-[A-Z]+-\d{3}) §(\d+\.\d+)$")
CITE_DEV = re.compile(r"^DEV-\d{4}-\d{3}$")


def tokens(text: str) -> list[str]:
    return [w for w in WORD.findall(text.lower()) if w not in STOP]


class BM25:
    def __init__(self, docs: list[str], k1: float = 1.4, b: float = 0.75):
        self.toks = [tokens(d) for d in docs]
        self.k1, self.b = k1, b
        self.avg = sum(map(len, self.toks)) / max(len(self.toks), 1)
        df = Counter(w for t in self.toks for w in set(t))
        n = len(self.toks)
        self.idf = {w: math.log(1 + (n - f + 0.5) / (f + 0.5)) for w, f in df.items()}
        self.tf = [Counter(t) for t in self.toks]

    def scores(self, query: str) -> list[float]:
        q = tokens(query)
        out = []
        for tf, t in zip(self.tf, self.toks, strict=True):
            s = 0.0
            for w in q:
                if w in tf:
                    f = tf[w]
                    s += self.idf[w] * f * (self.k1 + 1) / (f + self.k1 * (1 - self.b + self.b * len(t) / self.avg))
            out.append(s)
        return out

    def top(self, query: str, k: int) -> list[tuple[int, float]]:
        s = self.scores(query)
        order = sorted(range(len(s)), key=lambda i: -s[i])
        return [(i, s[i]) for i in order[:k] if s[i] > 0]


def _front_matter(text: str) -> dict:
    m = re.match(r"^---\n(.*?)\n---\n", text, re.S)
    return dict(line.split(": ", 1) for line in m.group(1).splitlines() if ": " in line) if m else {}


@lru_cache(maxsize=1)
def sop_sections() -> list[dict]:
    out = []
    for p in sorted((QMS_DIR / "sops").glob("*.md")):
        text = p.read_text()
        meta = _front_matter(text)
        sop_id, title = meta["id"], meta["title"]
        heading, current = "", None
        for line in text.splitlines():
            if line.startswith("## "):
                heading = line[3:].strip()
                current = None
                if not re.match(r"^\d+ (Procedure|Responsibilities)", heading):
                    current = {
                        "sop": sop_id,
                        "title": title,
                        "section": heading.split(" ", 1)[0],
                        "heading": heading,
                        "text": "",
                    }
                    out.append(current)
                continue
            m = SEC.match(line)
            if m:
                current = {"sop": sop_id, "title": title, "section": m.group(1), "heading": heading, "text": m.group(2)}
                out.append(current)
            elif current is not None and line.strip() and not line.startswith(("---", ">")):
                current["text"] = (current["text"] + " " + line.strip()).strip()
    for s in out:
        s["cite"] = f"{s['sop']} §{s['section']}"
    return [s for s in out if s["text"]]


@lru_cache(maxsize=1)
def deviations() -> list[dict]:
    return [json.loads(line) for line in (QMS_DIR / "deviations.jsonl").read_text().splitlines() if line.strip()]


@lru_cache(maxsize=1)
def _sop_index() -> BM25:
    return BM25([f"{s['title']} {s['heading']} {s['text']}" for s in sop_sections()])


@lru_cache(maxsize=1)
def _dev_index() -> BM25:
    return BM25([f"{d['title']} {d['symptoms']} {d['investigation']} {d['root_cause']}" for d in deviations()])


# ---------------------------------------------------------------- tools


def search_sops(query: str, k: int = 5) -> list[dict]:
    secs = sop_sections()
    return [{**secs[i], "score": round(s, 2)} for i, s in _sop_index().top(query, k)]


def get_sop_section(cite: str) -> dict:
    m = CITE_SOP.match(cite.strip())
    if not m:
        raise ValueError("cite an SOP section as 'SOP-FER-001 §4.4'")
    for s in sop_sections():
        if s["cite"] == cite.strip():
            return s
    raise ValueError(f"no such SOP section: {cite}")


def search_deviations(query: str, k: int = 5, category: str | None = None) -> list[dict]:
    devs = deviations()
    hits = _dev_index().top(query, len(devs))
    out = []
    for i, s in hits:
        d = devs[i]
        if category and d["category"] != category:
            continue
        out.append(
            {
                "id": d["id"],
                "title": d["title"],
                "category": d["category"],
                "symptoms": d["symptoms"],
                "root_cause": d["root_cause"],
                "score": round(s, 2),
            }
        )
        if len(out) == k:
            break
    return out


def get_deviation(dev_id: str) -> dict:
    for d in deviations():
        if d["id"] == dev_id.strip():
            return d
    raise ValueError(f"no such deviation: {dev_id}")


def citation_exists(cite: str) -> bool:
    cite = cite.strip()
    if CITE_DEV.match(cite):
        return any(d["id"] == cite for d in deviations())
    return any(s["cite"] == cite for s in sop_sections())
