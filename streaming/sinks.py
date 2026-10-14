"""Where producers send records: Kafka (normal path) or NDJSON files.

The NDJSON sink exists because Databricks Free Edition cannot reach a broker on
your laptop. The same records are written to files, uploaded to a Unity Catalog
Volume, and picked up by Auto Loader (lakehouse/databricks/bronze_autoloader.py).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Protocol

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from streaming.config import TopicSpec

log = logging.getLogger(__name__)


class Sink(Protocol):
    def send(self, key: str, record: dict) -> None: ...
    def close(self) -> None: ...


def ensure_topic(admin: AdminClient, spec: TopicSpec) -> None:
    """Create the topic if it does not exist (idempotent)."""
    if spec.name in admin.list_topics(timeout=10).topics:
        return
    fut = admin.create_topics(
        [
            NewTopic(
                spec.name,
                num_partitions=spec.partitions,
                replication_factor=1,
                config={"retention.ms": str(spec.retention_ms), "cleanup.policy": "delete"},
            )
        ]
    )[spec.name]
    try:
        fut.result()
        log.info("created topic %s (%d partitions)", spec.name, spec.partitions)
    except KafkaException as e:  # raced with another creator
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise


def make_value_serializer(sr_client: SchemaRegistryClient, spec: TopicSpec) -> AvroSerializer:
    # auto.register.schemas=False: producers may only use a schema that was
    # registered on purpose (python -m streaming.registry register). A producer
    # with a changed schema fails fast instead of silently creating a new version.
    return AvroSerializer(
        sr_client,
        spec.schema_str(),
        conf={"auto.register.schemas": False, "use.latest.version": False},
    )


class KafkaSink:
    def __init__(self, bootstrap: str, sr_url: str, spec: TopicSpec, create_topic: bool = True):
        self.spec = spec
        conf = {
            "bootstrap.servers": bootstrap,
            # Idempotent producer: retries cannot create duplicates or reorder a partition.
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "batch.num.messages": 10_000,
            "compression.type": "lz4",
        }
        if create_topic:
            ensure_topic(AdminClient({"bootstrap.servers": bootstrap}), spec)
        self.producer = Producer(conf)
        self.key_ser = StringSerializer("utf_8")
        self.value_ser = make_value_serializer(SchemaRegistryClient.new_client({"url": sr_url}), spec)
        self.errors = 0
        self.delivered = 0

    def _on_delivery(self, err, _msg):
        if err is not None:
            self.errors += 1
            log.error("delivery failed: %s", err)
        else:
            self.delivered += 1

    def send(self, key: str, record: dict) -> None:
        ctx = SerializationContext(self.spec.name, MessageField.VALUE)
        value = self.value_ser(record, ctx)
        while True:
            try:
                # Key = batch_id, so every reading of a batch lands on one partition, in order.
                self.producer.produce(
                    self.spec.name, key=self.key_ser(key), value=value, on_delivery=self._on_delivery
                )
                break
            except BufferError:  # local queue full: let it drain, then retry
                self.producer.poll(0.5)
        self.producer.poll(0)

    def close(self) -> None:
        remaining = self.producer.flush(60)
        if remaining or self.errors:
            raise RuntimeError(f"{remaining} undelivered, {self.errors} failed deliveries")


class NdjsonSink:
    """NDJSON files for upload to a Databricks Volume.

    per_key=True writes one file per key (batch_007.jsonl); per_key=False writes
    everything to a single <prefix>.jsonl (used for recalls, keyed by recall number).
    """

    def __init__(self, out_dir: str | Path, prefix: str, per_key: bool = True):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.prefix = prefix
        self.per_key = per_key
        self._files: dict[str, object] = {}
        self.delivered = 0

    def _fh(self, key: str):
        slot = key if self.per_key else ""
        if slot not in self._files:
            if not self.per_key:
                name = f"{self.prefix}.jsonl"
            elif key.isdigit():
                name = f"{self.prefix}_{int(key):03d}.jsonl"
            else:
                name = f"{self.prefix}_{key}.jsonl"
            self._files[slot] = open(self.out_dir / name, "w", encoding="utf-8")
        return self._files[slot]

    def send(self, key: str, record: dict) -> None:
        self._fh(key).write(json.dumps(record, separators=(",", ":")) + "\n")
        self.delivered += 1

    def close(self) -> None:
        for fh in self._files.values():
            fh.close()
