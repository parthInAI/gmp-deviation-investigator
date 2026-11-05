"""Build the regulation corpus: download, pin, extract, chunk by clause.

    python -m regulatory.corpus build     # download (or reuse cache), check hashes, write chunks.jsonl
    python -m regulatory.corpus pin       # first run only: record each file's SHA-256 in sources.lock.json

Every chunk is one citable unit with a stable id:

    EU-GMP-Ch1:1.4            a numbered clause of EudraLex Vol. 4
    EU-GMP-Annex22-draft:9.1  (draft Annex 22 ids carry "draft" so a report can never cite it as law)
    EU-GMP-Ch4:Principle
    FDA-WL-Strides-2019:2     the 2nd numbered violation in that warning letter

The chunks file is committed, so the agents and the evals always run on the same
text; rebuilding is how the Airflow re-indexing DAG picks up a new revision, and
a changed download fails the hash check until someone re-pins it on purpose.
PDF text comes from poppler's pdftotext, which keeps words intact where pure-
Python extractors split them.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import subprocess
import sys
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path

HERE = Path(__file__).parent
SOURCES = HERE / "sources.json"
LOCK = HERE / "sources.lock.json"
CHUNKS = HERE / "chunks.jsonl"
CACHE = Path("out/regulatory_cache")
UA = "gmp-deviation-investigator corpus builder (github.com/parthInAI/gmp-deviation-investigator)"

CLAUSE = re.compile(r"^(\d{1,2}\.\d{1,2})\.?(?=\s|$)")
SECTION = re.compile(r"^(\d{1,2})\.\s+([A-Z][A-Za-z ,&-]{2,60})$")
BARE_SECTION = re.compile(r"^(\d{1,2})\.$")  # Annex 11: the number alone, the title on the next line
MAX_WORDS = 300
FINDING = re.compile(r"^\s*(\d{1,2})\.\s+(Your firm|You |Your |Failure|Our investigator|FDA )")
NOISE = re.compile(
    r"^(\d{1,3}|Page \d+ of \d+|General|Project Phase|Operational Phase|EudraLex|Volume 4"
    r"|Commission Européenne.*|.*B-1049 Bruxelles.*|Telephone: .*)$"
)


@dataclass
class Chunk:
    id: str
    doc_id: str
    title: str
    status: str
    clause: str
    text: str


def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def download(doc: dict) -> Path:
    CACHE.mkdir(parents=True, exist_ok=True)
    ext = ".pdf" if doc["kind"] == "eudralex" else ".html"
    path = CACHE / f"{doc['id']}{ext}"
    if not path.exists():
        req = urllib.request.Request(doc["url"], headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=60) as r:
            path.write_bytes(r.read())
    return path


def pdf_text(path: Path) -> str:
    return subprocess.run(["pdftotext", str(path), "-"], check=True, capture_output=True, text=True).stdout


def html_text(path: Path) -> str:
    t = path.read_text(encoding="utf-8", errors="ignore")
    m = re.search(r"<article.*?</article>", t, re.S)
    t = m.group(0) if m else t
    t = re.sub(r"<(script|style).*?</\1>", "", t, flags=re.S)
    t = re.sub(r"<br\s*/?>|</p>|</li>|</h\d>", "\n", t)
    return html.unescape(re.sub(r"<[^>]+>", "", t))


def _clean_lines(text: str) -> list[str]:
    out = []
    for line in text.replace(" ", " ").splitlines():
        s = re.sub(r"\s+", " ", line).strip()
        if s and not NOISE.match(s):
            out.append(s)
    return out


def _join(lines: list[str]) -> str:
    return re.sub(r"\s+([,.;:)])", r"\1", " ".join(lines)).strip()


def chunk_eudralex(doc: dict, text: str) -> list[Chunk]:
    """Split at 'Principle', numbered clauses (1.4) and, for Annex 22, numbered sections without sub-clauses."""
    lines = _clean_lines(text)
    # Skip the cover page / document map: start at the first 'Principle' or first section heading after it.
    start = next((i for i, s in enumerate(lines) if s.lower() == "principle"), None)
    if start is None:  # Annex 22 has no Principle: start at the second '1. Scope' (the first is the document map)
        hits = [i for i, s in enumerate(lines) if SECTION.match(s) and SECTION.match(s).group(1) == "1"]
        start = hits[1] if len(hits) > 1 else hits[0]
    chunks: list[tuple[str, list[str]]] = []
    section_title = ""
    title_next = False
    for s in lines[start:]:
        sec = SECTION.match(s)
        if title_next:
            section_title, title_next = s, False
            chunks[-1][1].append(s)
            continue
        if BARE_SECTION.match(s):
            chunks.append((BARE_SECTION.match(s).group(1), []))
            title_next = True
        elif s.lower() == "principle":
            chunks.append(("Principle", []))
        elif s.lower() == "glossary":
            chunks.append(("Glossary", []))
        elif sec and not CLAUSE.match(s):
            section_title = sec.group(2)
            chunks.append((sec.group(1), [s]))  # a section that has no sub-clauses is cited by its number
        elif CLAUSE.match(s):
            num = CLAUSE.match(s).group(1)
            rest = s[CLAUSE.match(s).end() :].strip()
            chunks.append((num, ([f"[{section_title}]"] if section_title else []) + ([rest] if rest else [])))
        elif chunks:
            chunks[-1][1].append(s)
    out, seen = [], {}
    for clause, body in chunks:
        txt = _join(body)
        if len(txt.split()) < 6:  # a heading followed straight by its sub-clauses
            continue
        if clause == "Principle" and clause in seen:  # Chapter 4 has a second 'Principle' heading in its index
            out[seen[clause]].text += " " + txt
            continue
        seen[clause] = len(out)
        out.append(Chunk(f"{doc['id']}:{clause}", doc["id"], doc["title"], doc["status"], clause, txt))
    return out


def chunk_warning_letter(doc: dict, text: str) -> list[Chunk]:
    """One chunk per numbered violation; the opening and the remediation requests as their own chunks."""
    lines = _clean_lines(text)
    start = next((i for i, s in enumerate(lines) if s.startswith("Dear ")), 0)
    end = next((i for i, s in enumerate(lines) if s.startswith(("Conclusion", "Sincerely"))), len(lines))
    parts: list[tuple[str, list[str]]] = [("intro", [])]
    for s in lines[start:end]:
        m = FINDING.match(s)
        if m:
            parts.append((m.group(1), [s]))
        elif re.match(r"^(In response to this letter|CGMP consultant|Data Integrity Remediation)", s):
            parts.append((f"remediation-{len(parts)}", [s]))
        else:
            parts[-1][1].append(s)
    return [
        Chunk(f"{doc['id']}:{c}", doc["id"], doc["title"], doc["status"], c, _join(b))
        for c, b in parts
        if len(_join(b).split()) >= 20
    ]


def split_long(c: Chunk) -> list[Chunk]:
    """Chunks over MAX_WORDS become parts of ~250 words, cut at sentence ends; ids get #1, #2, ..."""
    words = c.text.split()
    if len(words) <= MAX_WORDS:
        return [c]
    sentences = re.split(r"(?<=[.;:])\s+", c.text)
    parts, cur = [], []
    for sent in sentences:
        if cur and len(" ".join(cur + [sent]).split()) > 250:
            parts.append(" ".join(cur))
            cur = []
        cur.append(sent)
    parts.append(" ".join(cur))
    return [Chunk(f"{c.id}#{i}", c.doc_id, c.title, c.status, c.clause, t) for i, t in enumerate(parts, 1)]


def citation_id(chunk_id: str) -> str:
    """The clause a chunk belongs to: 'FDA-WL-Akorn-2019:1#2' -> 'FDA-WL-Akorn-2019:1'."""
    return chunk_id.split("#", 1)[0]


def build(pin: bool = False) -> list[Chunk]:
    docs = json.loads(SOURCES.read_text())["documents"]
    lock = json.loads(LOCK.read_text()) if LOCK.exists() else {}
    chunks = []
    for doc in docs:
        path = download(doc)
        digest = sha256(path)
        if pin:
            lock[doc["id"]] = digest
        elif lock.get(doc["id"]) != digest:
            raise SystemExit(
                f"{doc['id']}: downloaded file hash {digest[:12]} != pinned {str(lock.get(doc['id']))[:12]}. "
                "The source changed: review it, then re-pin with `python -m regulatory.corpus pin`."
            )
        if doc["kind"] == "eudralex":
            chunks += chunk_eudralex(doc, pdf_text(path))
        else:
            chunks += chunk_warning_letter(doc, html_text(path))
    chunks = [part for c in chunks for part in split_long(c)]
    ids = [c.id for c in chunks]
    dupes = {i for i in ids if ids.count(i) > 1}
    if dupes:
        raise SystemExit(f"duplicate chunk ids: {sorted(dupes)}")
    if pin:
        LOCK.write_text(json.dumps(lock, indent=2) + "\n")
    CHUNKS.write_text("".join(json.dumps(asdict(c), ensure_ascii=False) + "\n" for c in chunks))
    return chunks


def load_chunks(path: Path = CHUNKS) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("action", choices=["build", "pin"])
    args = p.parse_args(argv)
    chunks = build(pin=args.action == "pin")
    by_doc: dict[str, int] = {}
    for c in chunks:
        by_doc[c.doc_id] = by_doc.get(c.doc_id, 0) + 1
    for d, n in by_doc.items():
        print(f"{d:<32} {n:>3} chunks")
    print(f"{len(chunks)} chunks -> {CHUNKS}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
