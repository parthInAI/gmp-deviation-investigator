"""Replay IndPenSim batches onto Kafka as if the fermenter were running now.

    python -m streaming.replayer --csv data/raw/100_Batches_IndPenSim_V3.csv
    python -m streaming.replayer --csv ... --batches 91-100 --speed 3600
    python -m streaming.replayer --csv ... --sink ndjson --out-dir out/ndjson/process_sensor
    python -m streaming.replayer --csv ... --inspect      # show the column mapping, send nothing

--speed is a time-compression factor on the simulated clock: IndPenSim samples
every 0.2 h, so --speed 720 sends one reading per batch per second and
--speed 0 sends as fast as the broker accepts (what the Phase 1 gate uses).

Every run writes a manifest (rows per batch, throughput) that the gate check
compares against what actually landed in bronze.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from streaming import indpensim
from streaming.config import SENSOR_TOPIC, bootstrap_servers, schema_registry_url
from streaming.sinks import KafkaSink, NdjsonSink, Sink

log = logging.getLogger("replayer")


def parse_batches(spec: str | None) -> set[int] | None:
    """'1-30,91,95-100' -> {1..30, 91, 95..100}. None means all batches."""
    if not spec:
        return None
    out: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = (int(x) for x in part.split("-", 1))
            out.update(range(lo, hi + 1))
        elif part:
            out.add(int(part))
    return out


def sleep_for(prev_time_h: float | None, time_h: float, speed: float) -> float:
    """Seconds to wait before sending a reading, given the time compression factor."""
    if speed <= 0 or prev_time_h is None:
        return 0.0
    return max(0.0, (time_h - prev_time_h) * 3600.0 / speed)


def inspect(csv_path: Path) -> dict:
    headers, repair = indpensim._read_header_info(csv_path)
    cmap = indpensim.build_column_map(headers)
    report = {
        "header_repair": repair,
        "mapped": cmap.mapping,
        "raman_columns_dropped": len(cmap.raman),
        "unknown_columns_dropped": cmap.unknown,
        "duplicate_columns_dropped": cmap.duplicates,
        "batch_id_source": "column" if cmap.has_batch_id else "time-reset detection",
        "contract_fields_missing_from_file": sorted(
            set(indpensim.SENSOR_FIELDS + indpensim.INT_FIELDS) - set(cmap.mapping.values())
        ),
    }
    return report


def replay(
    csv_path: Path,
    sink: Sink,
    batches: set[int] | None = None,
    speed: float = 0.0,
    replay_id: str | None = None,
    log_every: int = 50_000,
    batch_id_offset: int = 0,
) -> dict:
    """Send the batches to `sink`; return the manifest.

    batch_id_offset shifts the published batch ids (the drift harness uses 9000+,
    so injected batches can never be confused with real ones). Event times are
    still computed from the source batch id.
    """
    replay_id = replay_id or f"replay-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
    started = time.monotonic()
    rows_per_batch: dict[int, int] = {}
    fault_batches: list[int] = []
    coerced: dict[str, int] = {}
    sent = 0

    # Source-schema check, recorded in every manifest: drift caught at the door.
    source = inspect(csv_path)
    for what in ("unknown_columns_dropped", "contract_fields_missing_from_file"):
        if source[what]:
            log.warning("source schema: %s = %s", what, source[what])

    for source_batch_id, frame in indpensim.iter_batches(csv_path):
        if batches is not None and source_batch_id not in batches:
            continue
        frame, bad = indpensim.coerce_numeric(frame)
        for col, k in bad.items():
            coerced[col] = coerced.get(col, 0) + k
            log.warning("batch %d: %d non-numeric values in %s set to null", source_batch_id, k, col)
        batch_id = source_batch_id + batch_id_offset
        prev_t = None
        n = 0
        has_fault = False
        for rec in indpensim.to_records(source_batch_id, frame, replay_id):
            rec["batch_id"] = batch_id
            wait = sleep_for(prev_t, rec["time_h"], speed)
            if wait:
                time.sleep(wait)
            prev_t = rec["time_h"]
            sink.send(str(batch_id), rec)
            n += 1
            sent += 1
            has_fault |= bool(rec["fault_ref"])
            if sent % log_every == 0:
                el = time.monotonic() - started
                log.info("sent %d events (%.0f ev/s), at batch %d", sent, sent / el, batch_id)
        rows_per_batch[batch_id] = n
        if has_fault:
            fault_batches.append(batch_id)
        log.info("batch %3d: %d readings%s", batch_id, n, "  [fault]" if has_fault else "")

    sink.close()
    elapsed = time.monotonic() - started
    return {
        "replay_id": replay_id,
        "topic": SENSOR_TOPIC.name,
        "source_file": csv_path.name,
        "speed": speed,
        "batches": len(rows_per_batch),
        "total_rows": sent,
        "rows_per_batch": {str(k): v for k, v in sorted(rows_per_batch.items())},
        "fault_batches": fault_batches,
        "batch_id_offset": batch_id_offset,
        "source_schema": {
            "header_repair": source["header_repair"],
            "unknown_columns_dropped": source["unknown_columns_dropped"],
            "contract_fields_missing_from_file": source["contract_fields_missing_from_file"],
            "non_numeric_values_nulled": coerced,
        },
        "elapsed_s": round(elapsed, 2),
        "events_per_s": round(sent / elapsed, 1) if elapsed else None,
        "finished_at": datetime.now(UTC).isoformat(),
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", type=Path, required=True)
    p.add_argument("--batches", help="e.g. 1-100 or 91-100 or 1,5,94 (default: all)")
    p.add_argument("--speed", type=float, default=0.0, help="time compression factor; 0 = max")
    p.add_argument("--sink", choices=["kafka", "ndjson"], default="kafka")
    p.add_argument("--out-dir", type=Path, default=Path("out/ndjson/process_sensor"))
    p.add_argument("--bootstrap", default=bootstrap_servers())
    p.add_argument("--schema-registry", default=schema_registry_url())
    p.add_argument("--manifest", type=Path, default=Path("out/manifests/latest_replay.json"))
    p.add_argument("--inspect", action="store_true", help="print the column mapping and exit")
    p.add_argument("--batch-id-offset", type=int, default=0, help="add this to every published batch id")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.inspect:
        print(json.dumps(inspect(args.csv), indent=2))
        return 0

    sink: Sink = (
        KafkaSink(args.bootstrap, args.schema_registry, SENSOR_TOPIC)
        if args.sink == "kafka"
        else NdjsonSink(args.out_dir, prefix="batch")
    )
    manifest = replay(args.csv, sink, parse_batches(args.batches), args.speed, batch_id_offset=args.batch_id_offset)
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(manifest, indent=2))
    log.info(
        "done: %d batches, %d events in %.1fs (%.0f ev/s); manifest -> %s",
        manifest["batches"], manifest["total_rows"], manifest["elapsed_s"],
        manifest["events_per_s"] or 0, args.manifest,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
