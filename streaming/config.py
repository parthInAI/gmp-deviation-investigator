"""Topic names, schema locations and connection defaults, in one place."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

SCHEMA_DIR = Path(__file__).parent / "schemas"


@dataclass(frozen=True)
class TopicSpec:
    name: str
    schema_file: str
    partitions: int
    retention_ms: int  # -1 = keep forever, so bronze can always be rebuilt from the topic

    @property
    def subject(self) -> str:
        # Confluent TopicNameStrategy: one value schema per topic
        return f"{self.name}-value"

    @property
    def schema_path(self) -> Path:
        return SCHEMA_DIR / self.schema_file

    def schema_str(self) -> str:
        # Stripped, because the Avro serializer strips before lookup: registering
        # and looking up must send byte-identical text.
        return self.schema_path.read_text().strip()

    def schema_dict(self) -> dict:
        return json.loads(self.schema_str())


# GMPDI_TOPIC_SUFFIX isolates a test environment on the same broker, e.g.
# ".driftgate" -> process.sensor.v1.driftgate, with its own registry subject.
TOPIC_SUFFIX = os.environ.get("GMPDI_TOPIC_SUFFIX", "")

SENSOR_TOPIC = TopicSpec(f"process.sensor.v1{TOPIC_SUFFIX}", "sensor_reading.avsc", partitions=6, retention_ms=-1)
RECALL_TOPIC = TopicSpec(f"quality.fda_recall.v1{TOPIC_SUFFIX}", "recall_event.avsc", partitions=1, retention_ms=-1)
TOPICS = {t.name: t for t in (SENSOR_TOPIC, RECALL_TOPIC)}


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")


def schema_registry_url() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081")
