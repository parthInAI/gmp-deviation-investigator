"""Inject every drift scenario into an ISOLATED copy of the pipeline.

    GMPDI_TOPIC_SUFFIX=.driftgate python -m drift.inject --csv data/raw/<file>.csv

Runs only with a topic suffix set, so it can never touch the real topics or
lakehouse: everything goes to process.sensor.v1.driftgate / quality.fda_recall.v1.driftgate,
their own registry subjects, and /lakehouse/driftgate. --reset (the default)
deletes those first, so every gate run starts clean and is reproducible.

Writes out/drift/injection_manifest.json: for each scenario, what was sent, the
replayer/producer evidence, and what the gate should find. Then run bronze and
silver against the suffix topics and `python -m drift.check`.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from drift import scenarios as S
from streaming import indpensim, openfda_producer, registry, replayer
from streaming.config import RECALL_TOPIC, SENSOR_TOPIC, TOPIC_SUFFIX, TOPICS, bootstrap_servers, schema_registry_url
from streaming.sinks import KafkaSink

log = logging.getLogger("drift.inject")


class CollectSink:
    """Collects records in memory (to reuse the replayer's exact output for Avro scenarios)."""

    def __init__(self):
        self.records = []

    def send(self, key, record):
        self.records.append(record)

    def close(self):
        pass


def reset(bootstrap: str, sr: SchemaRegistryClient, lakehouse: Path) -> None:
    if not TOPIC_SUFFIX:
        raise SystemExit("refusing to reset without GMPDI_TOPIC_SUFFIX: that would delete the real topics")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    doomed = [t for t in TOPICS if t in existing]
    if doomed:
        for t, fut in admin.delete_topics(doomed).items():
            fut.result()
            log.info("deleted topic %s", t)
    for spec in TOPICS.values():
        try:
            sr.delete_subject(spec.subject)
            sr.delete_subject(spec.subject, permanent=True)
            log.info("deleted subject %s", spec.subject)
        except SchemaRegistryError as e:
            if e.http_status_code != 404:
                raise
    if lakehouse.exists():
        shutil.rmtree(lakehouse)
        log.info("deleted %s", lakehouse)


def wait_for_topics(admin: AdminClient, timeout_s: float = 30) -> None:
    """Topic deletion is asynchronous; recreate only once the old ones are gone."""
    import time

    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        try:
            registry.create_topics(admin)
            return
        except Exception as e:  # noqa: BLE001 - "marked for deletion" while the delete finishes
            log.info("waiting for topic deletion to finish: %s", e)
            time.sleep(1)
    registry.create_topics(admin)


def source_slice(csv_path: Path, rows: int) -> tuple[list[str], list[list[str]]]:
    """Repaired header + the first `rows` data rows (the start of batch 1), read raw."""
    header, _ = indpensim._read_header_info(csv_path)
    out = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            out.append(row)
            if len(out) >= rows:
                break
    return header, out


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def inject_source_csv(sc, header, rows, work: Path, bootstrap, sr_url) -> dict:
    h, r = S.drift_csv(sc.id, header, rows)
    path = work / f"{sc.id}.csv"
    write_csv(path, h, r)
    manifest = replayer.replay(
        path, KafkaSink(bootstrap, sr_url, SENSOR_TOPIC), batch_id_offset=S.batch_offset(sc.id), replay_id=sc.id
    )
    return {
        "keys": {"batch_id": sorted(int(b) for b in manifest["rows_per_batch"])},
        "sent": manifest["total_rows"],
        "source_evidence": manifest["source_schema"],
    }


def inject_avro(sc, clean_csv: Path, sr: SchemaRegistryClient, bootstrap, sr_url) -> dict:
    collected = CollectSink()
    replayer.replay(clean_csv, collected, batch_id_offset=S.batch_offset(sc.id), replay_id=sc.id)
    drifted = S.drift_schema(sc.id, SENSOR_TOPIC.schema_str())
    batch_ids = sorted({r["batch_id"] for r in collected.records})
    # The producer team registers its new schema (producers cannot auto-register).
    try:
        new_id = sr.register_schema(SENSOR_TOPIC.subject, Schema(drifted, "AVRO"))
    except SchemaRegistryError as e:
        log.info("%s: registry rejected the schema (HTTP %s): %s", sc.id, e.http_status_code, str(e)[:160])
        return {
            "keys": {"batch_id": batch_ids},
            "sent": 0,
            "registry": {
                "accepted": False,
                "http_status": e.http_status_code,
                "message": str(e).split(", details")[0][:300],
            },
        }
    log.info("%s: registry accepted the changed schema as id %s (BACKWARD-compatible)", sc.id, new_id)
    sink = KafkaSink(bootstrap, sr_url, SENSOR_TOPIC, schema_str=drifted)
    for rec in collected.records:
        sink.send(str(rec["batch_id"]), S.drift_avro_record(sc.id, rec))
    sink.close()
    return {
        "keys": {"batch_id": batch_ids},
        "sent": len(collected.records),
        "registry": {"accepted": True, "schema_id": new_id},
    }


def inject_recall(sc, fixture: list[dict], bootstrap, sr_url) -> dict:
    good = [r for r in fixture if r.get("recall_number") and r.get("recalling_firm")]
    raws = [S.drift_recall(sc.id, r, n) for n, r in enumerate(good)]
    result = openfda_producer.run(raws, KafkaSink(bootstrap, sr_url, RECALL_TOPIC))
    return {
        "keys": {"recall_number": sorted(r["recall_number"] for r in raws)},
        "sent": result["sent"],
        "source_evidence": {"unknown_keys": result["unknown_keys"]},
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", type=Path, required=True, help="IndPenSim CSV (real or sample)")
    p.add_argument("--rows", type=int, default=200, help="readings per scenario, from the start of batch 1")
    p.add_argument("--fixture", type=Path, default=Path("tests/fixtures/openfda_enforcement.json"))
    p.add_argument("--lakehouse", type=Path, default=Path("/lakehouse/driftgate"))
    p.add_argument("--out", type=Path, default=Path("out/drift"))
    p.add_argument("--no-reset", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    for noisy in ("httpx", "httpx2", "replayer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    if not TOPIC_SUFFIX:
        log.error("set GMPDI_TOPIC_SUFFIX (e.g. .driftgate): the drift test never writes to the real topics")
        return 2
    bootstrap, sr_url = bootstrap_servers(), schema_registry_url()
    sr = SchemaRegistryClient.new_client({"url": sr_url})
    if not args.no_reset:
        reset(bootstrap, sr, args.lakehouse)
    wait_for_topics(AdminClient({"bootstrap.servers": bootstrap}))
    approved_ids = registry.register_all(sr)

    work = args.out / "csv"
    header, rows = source_slice(args.csv, args.rows)
    clean_csv = work / "clean.csv"
    write_csv(clean_csv, header, rows)
    fixture = json.loads(args.fixture.read_text())["results"]

    results = []
    for sc in S.ALL_SCENARIOS:
        if sc.layer == "source_csv":
            got = inject_source_csv(sc, header, rows, work, bootstrap, sr_url)
        elif sc.layer == "avro_schema":
            got = inject_avro(sc, clean_csv, sr, bootstrap, sr_url)
        else:
            got = inject_recall(sc, fixture, bootstrap, sr_url)
        entry = {
            "id": sc.id,
            "title": sc.title,
            "layer": sc.layer,
            "expect": sc.expect,
            "expected_reasons": sc.expected_reasons,
            "source_signal": sc.source_signal,
            **got,
        }
        results.append(entry)
        log.info("%s %-70s sent %d", sc.id, sc.title, got["sent"])

    manifest = {
        "topic_suffix": TOPIC_SUFFIX,
        "source_file": args.csv.name,
        "lakehouse": str(args.lakehouse),
        "approved_schema_ids": approved_ids,
        "injected_at": datetime.now(UTC).isoformat(),
        "scenarios": results,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "injection_manifest.json").write_text(json.dumps(manifest, indent=2))
    log.info("manifest -> %s", args.out / "injection_manifest.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
