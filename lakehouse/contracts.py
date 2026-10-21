"""Data contracts: load them, and turn their rules into reason codes.

A contract (contracts/<name>.v<N>.json) says, per field: required or not,
numeric range, allowed values, or a format pattern. Every rule a record breaks
becomes a reason code, for example:

    required_null:temperature_k
    out_of_range:temperature_k
    not_allowed:classification
    bad_format:report_date

plus record-level reasons added by silver:

    decode_failed            bronze could not decode the message
    unapproved_schema:<id>   written with a schema version the contract has not approved
    rescued_data             (Databricks/Auto Loader) fields outside the contract

This module has no Spark dependency, so it can be tested and used anywhere.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

CONTRACT_DIR = Path(os.environ.get("GMPDI_CONTRACT_DIR", Path(__file__).resolve().parent.parent / "contracts"))


@dataclass(frozen=True)
class FieldRule:
    name: str
    required: bool = False
    min: float | None = None
    max: float | None = None
    allowed: tuple | None = None
    pattern: str | None = None


@dataclass(frozen=True)
class Contract:
    name: str
    version: int
    key: tuple[str, ...]
    approved_schemas: tuple[str, ...]
    rules: tuple[FieldRule, ...] = field(default_factory=tuple)

    @property
    def tag(self) -> str:
        return f"{self.name}.v{self.version}"


def load_contract(name: str, version: int = 1, directory: Path | None = None) -> Contract:
    path = (directory or CONTRACT_DIR) / f"{name}.v{version}.json"
    raw = json.loads(Path(path).read_text())
    rules = tuple(
        FieldRule(
            name=f,
            required=bool(spec.get("required", False)),
            min=spec.get("min"),
            max=spec.get("max"),
            allowed=tuple(spec["allowed"]) if "allowed" in spec else None,
            pattern=spec.get("pattern"),
        )
        for f, spec in raw["fields"].items()
    )
    return Contract(raw["contract"], int(raw["version"]), tuple(raw["key"]), tuple(raw["approved_schemas"]), rules)


def check_record(contract: Contract, record: dict) -> list[str]:
    """Pure-Python reference implementation of the rules (used by tests and producers).

    silver.py implements the same rules as Spark expressions; a test keeps the two in step.
    """
    import re

    reasons = []
    for r in contract.rules:
        v = record.get(r.name)
        if v is None:
            if r.required:
                reasons.append(f"required_null:{r.name}")
            continue
        if (r.min is not None and v < r.min) or (r.max is not None and v > r.max):
            reasons.append(f"out_of_range:{r.name}")
        if r.allowed is not None and v not in r.allowed:
            reasons.append(f"not_allowed:{r.name}")
        if r.pattern is not None and not re.fullmatch(r.pattern.strip("^$"), str(v)):
            reasons.append(f"bad_format:{r.name}")
    return reasons


def _strip_docs(obj):
    if isinstance(obj, dict):
        return {k: _strip_docs(v) for k, v in obj.items() if k != "doc"}
    if isinstance(obj, list):
        return [_strip_docs(x) for x in obj]
    return obj


def same_schema(a: str | dict, b: str | dict) -> bool:
    """Structural comparison of two Avro schemas, ignoring docs and formatting."""
    a = json.loads(a) if isinstance(a, str) else a
    b = json.loads(b) if isinstance(b, str) else b
    return _strip_docs(a) == _strip_docs(b)


def approved_schema_ids(registry_url: str, subject: str, approved_avsc: list[Path]) -> set[int]:
    """Registry ids of every registered version of `subject` that matches an approved .avsc.

    Uses only the standard library, so it runs inside the plain Spark image.
    """
    import urllib.error
    import urllib.request

    def get(path):
        with urllib.request.urlopen(f"{registry_url.rstrip('/')}{path}", timeout=15) as r:
            return json.loads(r.read())

    approved = [p.read_text() for p in approved_avsc]
    try:
        versions = get(f"/subjects/{subject}/versions")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return set()
        raise
    ids = set()
    for v in versions:
        entry = get(f"/subjects/{subject}/versions/{v}")
        if any(same_schema(entry["schema"], a) for a in approved):
            ids.add(int(entry["id"]))
    return ids
