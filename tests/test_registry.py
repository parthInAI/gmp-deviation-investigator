from confluent_kafka.schema_registry import SchemaRegistryClient

from streaming import registry
from streaming.config import TOPICS


def test_register_all_registers_every_contract():
    client = SchemaRegistryClient.new_client({"url": "mock://registry-test"})
    client.set_compatibility = lambda **kw: None  # not supported by the mock client
    ids = registry.register_all(client)
    assert set(ids) == {spec.subject for spec in TOPICS.values()}


def test_create_topics_covers_every_topic(monkeypatch):
    created = []
    monkeypatch.setattr(registry, "ensure_topic", lambda admin, spec: created.append(spec.name))
    registry.create_topics(admin=object())
    assert sorted(created) == sorted(TOPICS)
