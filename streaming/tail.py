"""Watch sensor readings arrive on Kafka, live, in your terminal.

    python -m streaming.tail                     # new readings from now on, all batches
    python -m streaming.tail --batch 94          # only batch 94
    python -m streaming.tail --from-beginning    # replay what is already on the topic

Each line is one reading as it lands on process.sensor.v1. Fault readings
(fault_ref > 0) are marked, so during a replay of batches 91-100 you can watch
the fault start. A read-only consumer with its own throwaway group: it never
commits offsets that would affect the Spark job or anything else.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import UTC, datetime

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from streaming.config import SENSOR_TOPIC, bootstrap_servers, schema_registry_url

COLUMNS = [
    ("temperature_k", "T(K)", "{:7.2f}"),
    ("ph", "pH", "{:5.2f}"),
    ("dissolved_oxygen_mgpl", "DO2", "{:6.2f}"),
    ("agitator_rpm", "RPM", "{:6.1f}"),
    ("aeration_rate_lph", "Air(L/h)", "{:8.1f}"),
    ("penicillin_conc_gpl", "Pen(g/L)", "{:8.2f}"),
]
HEADER = f"{'plant time (UTC)':19} {'batch':>5} {'seq':>5} {'t(h)':>6}  " + "  ".join(
    f"{label:>{len(fmt.format(0.0))}}" for _, label, fmt in COLUMNS
)


def format_reading(rec: dict) -> str:
    ts = datetime.fromtimestamp(rec["event_time"] / 1000, tz=UTC) if isinstance(
        rec["event_time"], (int, float)
    ) else rec["event_time"]
    values = []
    for field, _, fmt in COLUMNS:
        v = rec.get(field)
        width = len(fmt.format(0.0))
        values.append(fmt.format(v) if v is not None else "-".rjust(width))
    flag = f"  << FAULT {rec['fault_ref']}" if rec.get("fault_ref") else ""
    return (
        f"{ts:%Y-%m-%d %H:%M:%S} {rec['batch_id']:>5} {rec['seq']:>5} {rec['time_h']:>6.1f}  "
        + "  ".join(values)
        + flag
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--batch", type=int, help="only show this batch")
    p.add_argument("--from-beginning", action="store_true", help="start at the oldest message on the topic")
    p.add_argument("--every", type=int, default=1, help="print every Nth reading (useful at high replay speed)")
    p.add_argument("--bootstrap", default=bootstrap_servers())
    p.add_argument("--schema-registry", default=schema_registry_url())
    args = p.parse_args(argv)

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"tail-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )
    deser = AvroDeserializer(SchemaRegistryClient.new_client({"url": args.schema_registry}))
    ctx = SerializationContext(SENSOR_TOPIC.name, MessageField.VALUE)
    consumer.subscribe([SENSOR_TOPIC.name])
    want_key = str(args.batch).encode() if args.batch is not None else None

    print(f"watching {SENSOR_TOPIC.name}" + (f", batch {args.batch}" if want_key else "") + "  (Ctrl+C to stop)")
    print(HEADER, flush=True)
    seen = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}", file=sys.stderr)
                continue
            if want_key is not None and msg.key() != want_key:
                continue
            seen += 1
            rec = deser(msg.value(), ctx)
            if seen % args.every == 0 or rec.get("fault_ref"):
                print(format_reading(rec), flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    print(f"\n{seen} readings seen")
    return 0


if __name__ == "__main__":
    sys.exit(main())
