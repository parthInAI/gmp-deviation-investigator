"""Set up the topics and register the Avro contracts, deliberately.

    python -m streaming.registry register     # create topics, set BACKWARD compatibility, register schemas
    python -m streaming.registry check        # would the local .avsc files be accepted?

Producers never auto-register (see sinks.make_value_serializer), so this is the
one place a schema version can be created. Topics are created here too, so the
bronze job finds every topic it subscribes to even before a producer has run.
In CI this becomes the gate for contract changes.
"""

from __future__ import annotations

import argparse
import logging
import sys

from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from streaming.config import TOPICS, bootstrap_servers, schema_registry_url
from streaming.sinks import ensure_topic

log = logging.getLogger(__name__)

# BACKWARD: consumers on the new schema can read data written with the old one.
# Right default for a lakehouse that replays history from the start of the topic.
COMPATIBILITY = "BACKWARD"


def register_all(client: SchemaRegistryClient) -> dict[str, int]:
    ids = {}
    for spec in TOPICS.values():
        try:
            client.set_compatibility(subject_name=spec.subject, level=COMPATIBILITY)
        except SchemaRegistryError as e:
            log.warning("could not set compatibility on %s: %s", spec.subject, e)
        schema_id = client.register_schema(spec.subject, Schema(spec.schema_str(), "AVRO"))
        ids[spec.subject] = schema_id
        log.info("%s -> schema id %s", spec.subject, schema_id)
    return ids


def create_topics(admin: AdminClient) -> None:
    for spec in TOPICS.values():
        ensure_topic(admin, spec)
        log.info("topic %s ready", spec.name)


def check_all(client: SchemaRegistryClient) -> bool:
    ok = True
    for spec in TOPICS.values():
        try:
            compatible = client.test_compatibility(spec.subject, Schema(spec.schema_str(), "AVRO"))
        except SchemaRegistryError as e:
            if e.http_status_code == 404:  # subject not registered yet: nothing to break
                log.info("%s: not registered yet", spec.subject)
                continue
            raise
        log.info("%s: %s", spec.subject, "compatible" if compatible else "INCOMPATIBLE")
        ok &= bool(compatible)
    return ok


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("action", choices=["register", "check"])
    p.add_argument("--url", default=schema_registry_url())
    p.add_argument("--bootstrap", default=bootstrap_servers())
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    client = SchemaRegistryClient.new_client({"url": args.url})
    if args.action == "register":
        create_topics(AdminClient({"bootstrap.servers": args.bootstrap}))
        register_all(client)
        return 0
    return 0 if check_all(client) else 1


if __name__ == "__main__":
    sys.exit(main())
