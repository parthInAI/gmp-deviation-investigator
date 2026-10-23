import pytest
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


# ---------------------------------------------------------------- _schemas retention and repair


def test_repair_leaves_a_healthy_registry_alone(monkeypatch):
    from streaming import registry as R

    monkeypatch.setattr(R, "registry_status", lambda url: (True, "[]"))
    monkeypatch.setattr(R, "_Unprotected", lambda url: pytest.fail("must not touch protection"))
    assert R.repair(object(), "http://admin", "http://reg", yes=True) == 0


def test_repair_needs_yes_and_only_handles_the_expired_log_case(monkeypatch):
    from streaming import registry as R

    monkeypatch.setattr(R, "_Unprotected", lambda url: pytest.fail("must not touch protection"))
    broken = '{"error_code":40002,"message":"Fetch returned with error"}'
    monkeypatch.setattr(R, "registry_status", lambda url: (False, broken))
    assert R.repair(object(), "http://admin", "http://reg", yes=False) == 2
    monkeypatch.setattr(R, "registry_status", lambda url: (False, '{"error_code":50001,"message":"other"}'))
    assert R.repair(object(), "http://admin", "http://reg", yes=True) == 2


def test_protection_is_restored_even_if_the_change_fails(monkeypatch):
    from streaming import registry as R

    calls = []
    original = ["_redpanda.audit_log", "__consumer_offsets", "_schemas"]

    def fake_admin(method, url, path, body=None):
        calls.append((method, body))
        return {"kafka_nodelete_topics": original} if method == "GET" else None

    monkeypatch.setattr(R, "_admin", fake_admin)
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError):
        with R._Unprotected("http://admin"):
            raise RuntimeError("alter failed")
    puts = [b["upsert"]["kafka_nodelete_topics"] for m, b in calls if m == "PUT"]
    assert puts == [["_redpanda.audit_log", "__consumer_offsets"], original]
