"""Turn an Avro contract into a Spark DDL schema string.

Used where Spark reads the same records as JSON instead of Avro (Databricks
Auto Loader over NDJSON), so both paths share one contract: the .avsc file.
Covers the types our contracts use; anything else raises rather than guessing.
"""

from __future__ import annotations

import json
from pathlib import Path

_PRIMITIVES = {
    "string": "STRING",
    "int": "INT",
    "long": "BIGINT",
    "double": "DOUBLE",
    "float": "FLOAT",
    "boolean": "BOOLEAN",
}


def _spark_type(avro_type) -> str:
    if isinstance(avro_type, list):  # union: only ["null", X] is supported
        non_null = [t for t in avro_type if t != "null"]
        if len(non_null) != 1:
            raise ValueError(f"unsupported union {avro_type}")
        return _spark_type(non_null[0])
    if isinstance(avro_type, dict):
        if avro_type.get("type") == "array":
            return f"ARRAY<{_spark_type(avro_type['items'])}>"
        # timestamp-millis travels as epoch millis in JSON; keep it BIGINT in bronze.
        return _spark_type(avro_type["type"])
    if avro_type in _PRIMITIVES:
        return _PRIMITIVES[avro_type]
    raise ValueError(f"unsupported Avro type {avro_type!r}")


def avro_to_ddl(schema: dict | str | Path) -> str:
    if isinstance(schema, Path):
        schema = schema.read_text()
    if isinstance(schema, str):
        schema = json.loads(schema)
    return ", ".join(f"`{f['name']}` {_spark_type(f['type'])}" for f in schema["fields"])
