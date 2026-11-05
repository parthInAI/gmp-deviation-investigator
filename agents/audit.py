"""Signed, append-only audit trail of reviewer decisions.

Each entry records who decided, when, what (approve / edit / reject and why), on which report
(SHA-256 of its JSON), and with which models and prompt versions. Entries are chained: each one
includes the previous entry's signature, and the signature is an HMAC over the entry with a
secret key (GMPDI_AUDIT_KEY), so editing or deleting an entry breaks verification.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import UTC, datetime
from pathlib import Path

AUDIT_FILE = Path(os.environ.get("GMPDI_AUDIT_FILE", "out/audit/decisions.jsonl"))


def _key() -> bytes:
    key = os.environ.get("GMPDI_AUDIT_KEY")
    if not key:
        raise RuntimeError("GMPDI_AUDIT_KEY is not set: decisions cannot be signed")
    return key.encode()


def _sign(body: dict) -> str:
    return hmac.new(_key(), json.dumps(body, sort_keys=True).encode(), hashlib.sha256).hexdigest()


def report_hash(report_json: str) -> str:
    return hashlib.sha256(report_json.encode()).hexdigest()


def _entries(path: Path | None = None) -> list[dict]:
    path = path or AUDIT_FILE
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()] if path.exists() else []


def append(entry: dict, path: Path | None = None) -> dict:
    path = path or AUDIT_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    prev = _entries(path)
    body = {**entry, "at": datetime.now(UTC).isoformat(), "prev": prev[-1]["signature"] if prev else None}
    signed = {**body, "signature": _sign(body)}
    with path.open("a") as f:
        f.write(json.dumps(signed, sort_keys=True) + "\n")
    return signed


def verify(path: Path | None = None) -> tuple[bool, str]:
    prev = None
    for i, e in enumerate(_entries(path or AUDIT_FILE)):
        body = {k: v for k, v in e.items() if k != "signature"}
        if body.get("prev") != prev:
            return False, f"entry {i}: chain broken"
        if not hmac.compare_digest(_sign(body), e["signature"]):
            return False, f"entry {i}: signature does not match"
        prev = e["signature"]
    return True, "ok"
