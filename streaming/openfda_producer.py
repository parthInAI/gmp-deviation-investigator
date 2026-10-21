"""Pull drug recall (enforcement) reports from openFDA onto the recall topic.

    python -m streaming.openfda_producer                       # last 365 days
    python -m streaming.openfda_producer --since 20250101
    python -m streaming.openfda_producer --fixture tests/fixtures/openfda_enforcement.json

Incremental: the highest report_date seen is stored in a small state file, and
the next run starts from there. Records are keyed by recall_number, so a record
fetched twice lands on the same partition and silver can deduplicate on it.

This is the project's second, real-world, messier feed. In Phase 2 it is the
topic used to test schema drift (renamed, dropped and retyped fields).

openFDA limits: 1,000 records per page, skip <= 25,000, 240 requests/minute
without an API key. Set OPENFDA_API_KEY to raise the daily quota.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import requests

from streaming.config import RECALL_TOPIC, bootstrap_servers, schema_registry_url
from streaming.sinks import KafkaSink, NdjsonSink, Sink

log = logging.getLogger("openfda")

API_URL = "https://api.fda.gov/drug/enforcement.json"
PAGE_SIZE = 1000
MAX_SKIP = 25_000

STRING_FIELDS = [
    f["name"]
    for f in RECALL_TOPIC.schema_dict()["fields"]
    if f["type"] in ("string", ["null", "string"])
]
# Keys openFDA sends that the producer reads, or knowingly ignores (address and
# extra code details are not used anywhere downstream).
IGNORED_KEYS = {"address_1", "address_2", "postal_code", "more_code_info"}
OPENFDA_ARRAYS = {
    "openfda_brand_name": "brand_name",
    "openfda_generic_name": "generic_name",
    "openfda_manufacturer_name": "manufacturer_name",
}


KNOWN_KEYS = set(STRING_FIELDS) | IGNORED_KEYS | {"openfda"}


def normalise(raw: dict, ingested_at_ms: int) -> dict | None:
    """Shape one openFDA result into the RecallEvent contract. None if unusable."""
    if not raw.get("recall_number"):
        return None
    rec: dict = {}
    for f in STRING_FIELDS:
        v = raw.get(f)
        rec[f] = None if v in (None, "") else str(v)
    of = raw.get("openfda") or {}
    for out, src in OPENFDA_ARRAYS.items():
        v = of.get(src) or []
        rec[out] = [str(x) for x in (v if isinstance(v, list) else [v])]
    rec["ingested_at"] = ingested_at_ms
    return rec


def fetch_pages(
    since: str,
    until: str,
    session: requests.Session | None = None,
    api_key: str | None = None,
    pause_s: float = 0.3,
) -> Iterator[dict]:
    """Yield raw results with report_date in [since, until], oldest first."""
    session = session or requests.Session()
    skip = 0
    while True:
        params = {
            "search": f"report_date:[{since} TO {until}]",
            "sort": "report_date:asc",
            "limit": PAGE_SIZE,
            "skip": skip,
        }
        if api_key:
            params["api_key"] = api_key
        resp = session.get(API_URL, params=params, timeout=30)
        if resp.status_code == 404:  # openFDA returns 404 for "no matches"
            return
        resp.raise_for_status()
        body = resp.json()
        results = body.get("results", [])
        yield from results
        total = body.get("meta", {}).get("results", {}).get("total", 0)
        skip += len(results)
        if not results or skip >= total:
            return
        if skip > MAX_SKIP:
            log.warning("hit openFDA skip limit at %d of %d; narrow the date range", skip, total)
            return
        time.sleep(pause_s)


def load_state(path: Path) -> str | None:
    if path.exists():
        return json.loads(path.read_text()).get("max_report_date")
    return None


def save_state(path: Path, max_report_date: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"max_report_date": max_report_date}))


def run(raw_records, sink: Sink) -> dict:
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    sent = skipped = 0
    max_date = ""
    seen: set[str] = set()
    unknown_keys: dict[str, int] = {}  # keys openFDA sent that the contract does not know
    for raw in raw_records:
        for k in raw:
            if k not in KNOWN_KEYS:
                unknown_keys[k] = unknown_keys.get(k, 0) + 1
        rec = normalise(raw, now_ms)
        if rec is None or rec["recall_number"] in seen:
            skipped += 1
            continue
        seen.add(rec["recall_number"])
        sink.send(rec["recall_number"], rec)
        sent += 1
        max_date = max(max_date, rec["report_date"] or "")
    sink.close()
    if unknown_keys:
        log.warning("openFDA keys not in the contract (dropped): %s", unknown_keys)
    return {"sent": sent, "skipped": skipped, "max_report_date": max_date or None, "unknown_keys": unknown_keys}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--since", help="YYYYMMDD; default: state file, else 365 days ago")
    p.add_argument("--until", default=date.today().strftime("%Y%m%d"))
    p.add_argument("--fixture", type=Path, help="read an openFDA JSON response from disk instead")
    p.add_argument("--sink", choices=["kafka", "ndjson"], default="kafka")
    p.add_argument("--out-dir", type=Path, default=Path("out/ndjson/fda_recall"))
    p.add_argument("--state", type=Path, default=Path("out/state/openfda.json"))
    p.add_argument("--bootstrap", default=bootstrap_servers())
    p.add_argument("--schema-registry", default=schema_registry_url())
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.fixture:
        raw = json.loads(args.fixture.read_text()).get("results", [])
    else:
        since = args.since or load_state(args.state) or (date.today() - timedelta(days=365)).strftime("%Y%m%d")
        log.info("fetching recalls reported %s..%s", since, args.until)
        raw = fetch_pages(since, args.until, api_key=os.environ.get("OPENFDA_API_KEY"))

    sink: Sink = (
        KafkaSink(args.bootstrap, args.schema_registry, RECALL_TOPIC)
        if args.sink == "kafka"
        else NdjsonSink(args.out_dir, prefix=f"recalls_{args.until}", per_key=False)
    )
    result = run(raw, sink)
    if result["max_report_date"] and not args.fixture:
        save_state(args.state, result["max_report_date"])
    log.info("sent %d recalls (%d skipped); watermark %s", result["sent"], result["skipped"], result["max_report_date"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
