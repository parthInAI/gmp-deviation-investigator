"""Set up the topics and register the Avro contracts, deliberately.

    python -m streaming.registry register      # create topics, keep _schemas forever, set BACKWARD, register
    python -m streaming.registry check         # would the local .avsc files be accepted?
    python -m streaming.registry repair --yes  # ONLY if the registry is broken (see below)

Producers never auto-register (see sinks.make_value_serializer), so this is the
one place a schema version can be created. Topics are created here too, so the
bronze job finds every topic it subscribes to even before a producer has run.
In CI this becomes the gate for contract changes.

The registry's own storage. Redpanda keeps every registered schema in the
internal topic `_schemas`, and (v25.3, dev-container mode) creates it with the
cluster defaults: cleanup.policy=delete, retention 7 days. After a week the
oldest schema records are deleted, the registry can no longer replay its log
from the start, and every request fails with error 40002 "Fetch returned with
error". `register` therefore makes `_schemas` compacted with unlimited
retention. Redpanda protects `_schemas` from config changes, so the protection
is lifted for that one change and always restored.

`repair` is for a stack that already lost records: it deletes the broken
`_schemas` topic (the schemas in it are unreadable anyway). Then restart
Redpanda and run `register`: schemas register again in the same order and get
the same ids (1 = sensor, 2 = recall), so every message already on the topics
still decodes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request

from confluent_kafka.admin import AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from streaming.config import TOPICS, bootstrap_servers, schema_registry_url
from streaming.sinks import ensure_topic

log = logging.getLogger(__name__)

# BACKWARD: consumers on the new schema can read data written with the old one.
# Right default for a lakehouse that replays history from the start of the topic.
COMPATIBILITY = "BACKWARD"
SCHEMAS_TOPIC = "_schemas"
KEEP_FOREVER = {"cleanup.policy": "compact", "retention.ms": "-1"}
_NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def admin_url() -> str:
    return os.environ.get("REDPANDA_ADMIN_URL", "http://localhost:19644")


def _admin(method: str, url: str, path: str, body: dict | None = None):
    req = urllib.request.Request(
        f"{url.rstrip('/')}{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with _NO_PROXY.open(req, timeout=20) as r:
        raw = r.read()
    return json.loads(raw) if raw else None


class _Unprotected:
    """Context manager: lift Redpanda's protection of _schemas, always restore it."""

    def __init__(self, url: str):
        self.url = url
        self.original: list[str] | None = None

    def __enter__(self):
        cfg = _admin("GET", self.url, "/v1/cluster_config")
        self.original = list(cfg.get("kafka_nodelete_topics") or [])
        if SCHEMAS_TOPIC in self.original:
            keep = [t for t in self.original if t != SCHEMAS_TOPIC]
            _admin("PUT", self.url, "/v1/cluster_config", {"upsert": {"kafka_nodelete_topics": keep}, "remove": []})
            time.sleep(1)
        return self

    def __exit__(self, *exc):
        if self.original is not None and SCHEMAS_TOPIC in self.original:
            _admin(
                "PUT",
                self.url,
                "/v1/cluster_config",
                {"upsert": {"kafka_nodelete_topics": self.original}, "remove": []},
            )
        return False


def schemas_topic_config(admin: AdminClient) -> dict[str, str] | None:
    if SCHEMAS_TOPIC not in admin.list_topics(timeout=10).topics:
        return None
    res = ConfigResource(ConfigResource.Type.TOPIC, SCHEMAS_TOPIC)
    entries = admin.describe_configs([res])[res].result()
    return {k: entries[k].value for k in KEEP_FOREVER if k in entries}


def ensure_schemas_kept_forever(admin: AdminClient, url: str) -> bool:
    """Make _schemas compacted with unlimited retention. True if a change was made."""
    current = schemas_topic_config(admin)
    if current is None:
        log.info("%s does not exist yet (the registry creates it on first use)", SCHEMAS_TOPIC)
        return False
    if current == KEEP_FOREVER:
        log.info("%s already kept forever %s", SCHEMAS_TOPIC, current)
        return False
    log.warning("%s has %s; setting %s so registered schemas never expire", SCHEMAS_TOPIC, current, KEEP_FOREVER)
    res = ConfigResource(
        ConfigResource.Type.TOPIC,
        SCHEMAS_TOPIC,
        incremental_configs=[
            ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET) for k, v in KEEP_FOREVER.items()
        ],
    )
    with _Unprotected(url):
        admin.incremental_alter_configs([res])[res].result()
    after = schemas_topic_config(admin)
    if after != KEEP_FOREVER:
        raise RuntimeError(f"{SCHEMAS_TOPIC} config is {after}, expected {KEEP_FOREVER}")
    return True


def registry_status(registry: str) -> tuple[bool, str]:
    try:
        with _NO_PROXY.open(f"{registry.rstrip('/')}/subjects", timeout=15) as r:
            return True, r.read().decode()[:200]
    except urllib.error.HTTPError as e:
        return False, e.read().decode("utf-8", "replace")[:200]


def repair(admin: AdminClient, url: str, registry: str, yes: bool) -> int:
    ok, detail = registry_status(registry)
    if ok:
        log.info("registry is healthy (%s); nothing to repair", detail)
        return 0
    if '"error_code":40002' not in detail.replace(" ", ""):
        log.error("registry error is not the expired-_schemas case (%s); not touching anything", detail)
        return 2
    if not yes:
        log.error("registry broken (%s). Re-run with --yes to delete the unreadable %s topic.", detail, SCHEMAS_TOPIC)
        return 2
    with _Unprotected(url):
        admin.delete_topics([SCHEMAS_TOPIC])[SCHEMAS_TOPIC].result()
    log.warning("deleted %s. Now restart Redpanda, then run: python -m streaming.registry register", SCHEMAS_TOPIC)
    return 0


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
    p.add_argument("action", choices=["register", "check", "repair"])
    p.add_argument("--url", default=schema_registry_url())
    p.add_argument("--bootstrap", default=bootstrap_servers())
    p.add_argument("--admin-url", default=admin_url())
    p.add_argument("--yes", action="store_true", help="repair: really delete the unreadable _schemas topic")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    if args.action == "repair":
        return repair(admin, args.admin_url, args.url, args.yes)
    client = SchemaRegistryClient.new_client({"url": args.url})
    if args.action == "register":
        create_topics(admin)
        register_all(client)  # first use creates _schemas if it does not exist yet
        ensure_schemas_kept_forever(admin, args.admin_url)
        return 0
    return 0 if check_all(client) else 1


if __name__ == "__main__":
    sys.exit(main())
