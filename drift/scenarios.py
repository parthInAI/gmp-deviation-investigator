"""The drift scenarios: each one breaks the data the way a real upstream change would.

Three places drift can enter, and every scenario says where it should be caught:

  source CSV    a column renamed, dropped, retyped to text, or its unit changed
  Avro schema   a producer team ships a changed schema (registry-compatible or not)
  openFDA JSON  the API renames a key, changes a date format or a code list

Pure functions only (no Kafka, no files), so they are unit-tested directly.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field

from streaming import indpensim

# Published batch ids for the sensor scenarios: 9000 + 100 * scenario number + source batch.
OFFSET_BASE = 9000


@dataclass
class Scenario:
    id: str
    title: str
    layer: str  # "source_csv" | "avro_schema" | "openfda_json"
    expect: str  # "silver" (control) | "quarantine" | "rejected_by_registry"
    expected_reasons: list[str] = field(default_factory=list)  # any of these (prefix match)
    source_signal: str | None = None  # manifest field that should already flag it at ingest


SENSOR_SCENARIOS = [
    Scenario("S0", "control: unchanged batch", "source_csv", "silver"),
    Scenario(
        "S1",
        "column renamed: DO2 header code changed to DOT",
        "source_csv",
        "quarantine",
        ["required_null:dissolved_oxygen_mgpl"],
        "unknown_columns_dropped",
    ),
    Scenario(
        "S2",
        "column dropped: pH",
        "source_csv",
        "quarantine",
        ["required_null:ph"],
        "contract_fields_missing_from_file",
    ),
    Scenario(
        "S3",
        "column retyped: agitator speed sent as text ('100 rpm')",
        "source_csv",
        "quarantine",
        ["required_null:agitator_rpm"],
        "non_numeric_values_nulled",
    ),
    Scenario(
        "S4",
        "unit changed: temperature in degC instead of K (same header)",
        "source_csv",
        "quarantine",
        ["out_of_range:temperature_k"],
        None,  # invisible at the source: same column, same type. Only the contract range sees it.
    ),
    Scenario(
        "S5",
        "Avro field renamed: temperature_k -> temperature (registry-compatible)",
        "avro_schema",
        "quarantine",
        ["unapproved_schema:", "decode_failed"],
    ),
    Scenario(
        "S6",
        "Avro field dropped: ph (registry-compatible)",
        "avro_schema",
        "quarantine",
        ["unapproved_schema:", "decode_failed"],
    ),
    Scenario(
        "S7",
        "Avro field retyped: temperature_k double -> string (incompatible)",
        "avro_schema",
        "rejected_by_registry",
    ),
]

RECALL_SCENARIOS = [
    Scenario("R0", "control: unchanged recall", "openfda_json", "silver"),
    Scenario(
        "R1",
        "key renamed: report_date -> reportDate",
        "openfda_json",
        "quarantine",
        ["required_null:report_date"],
        "unknown_keys",
    ),
    Scenario(
        "R2", "date format changed: 20260211 -> 2026-02-11", "openfda_json", "quarantine", ["bad_format:report_date"]
    ),
    Scenario(
        "R3", "code list changed: 'Class II' -> 'Class 2'", "openfda_json", "quarantine", ["not_allowed:classification"]
    ),
]

ALL_SCENARIOS = SENSOR_SCENARIOS + RECALL_SCENARIOS


def batch_offset(scenario_id: str) -> int:
    return OFFSET_BASE + 100 * int(scenario_id[1:])


# ---------------------------------------------------------------- source CSV


def _col(header: list[str], field_name: str) -> int:
    for i, h in enumerate(header):
        if indpensim.canonical_name(h) == field_name:
            return i
    raise KeyError(field_name)


def drift_csv(scenario_id: str, header: list[str], rows: list[list[str]]) -> tuple[list[str], list[list[str]]]:
    """Return (header, rows) with the scenario's change applied. Input is not modified."""
    header, rows = list(header), [list(r) for r in rows]
    if scenario_id == "S0":
        return header, rows
    if scenario_id == "S1":
        i = _col(header, "dissolved_oxygen_mgpl")
        header[i] = header[i].replace("(DO2:", "(DOT:")
        return header, rows
    if scenario_id == "S2":
        i = _col(header, "ph")
        return header[:i] + header[i + 1 :], [r[:i] + r[i + 1 :] for r in rows]
    if scenario_id == "S3":
        i = _col(header, "agitator_rpm")
        for r in rows:
            r[i] = f"{float(r[i]):.0f} rpm"
        return header, rows
    if scenario_id == "S4":
        i = _col(header, "temperature_k")
        for r in rows:
            r[i] = f"{float(r[i]) - 273.15:.4f}"
        return header, rows
    raise ValueError(f"{scenario_id} is not a source-CSV scenario")


# ---------------------------------------------------------------- Avro schema


def drift_schema(scenario_id: str, approved_schema: str) -> str:
    """The changed schema a producer team ships."""
    s = json.loads(approved_schema)
    fields = s["fields"]
    if scenario_id == "S5":
        for f in fields:
            if f["name"] == "temperature_k":
                f["name"] = "temperature"
    elif scenario_id == "S6":
        s["fields"] = [f for f in fields if f["name"] != "ph"]
    elif scenario_id == "S7":
        for f in fields:
            if f["name"] == "temperature_k":
                f["type"] = ["null", "string"]
    else:
        raise ValueError(f"{scenario_id} is not an Avro scenario")
    return json.dumps(s)


def drift_avro_record(scenario_id: str, record: dict) -> dict:
    r = dict(record)
    if scenario_id == "S5":
        r["temperature"] = r.pop("temperature_k")
    elif scenario_id == "S6":
        r.pop("ph")
    elif scenario_id == "S7":
        r["temperature_k"] = str(r["temperature_k"])
    return r


# ---------------------------------------------------------------- openFDA JSON


def drift_recall(scenario_id: str, raw: dict, n: int) -> dict:
    """An openFDA result with the scenario's change; recall number made unique per scenario."""
    r = copy.deepcopy(raw)
    r["recall_number"] = f"D-9{int(scenario_id[1:]):01d}{n:02d}-2026"
    if scenario_id == "R0":
        pass
    elif scenario_id == "R1":
        r["reportDate"] = r.pop("report_date")
    elif scenario_id == "R2":
        d = r["report_date"]
        r["report_date"] = f"{d[:4]}-{d[4:6]}-{d[6:]}"
    elif scenario_id == "R3":
        r["classification"] = r["classification"].replace("Class III", "Class 3").replace("Class II", "Class 2")
        r["classification"] = r["classification"].replace("Class I", "Class 1")
    else:
        raise ValueError(f"{scenario_id} is not a recall scenario")
    return r
