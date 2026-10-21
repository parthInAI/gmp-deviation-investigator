"""Read the IndPenSim CSV and turn it into per-batch sensor records.

The raw file (100_Batches_IndPenSim_V3.csv, ~2.5 GB) has long, inconsistent
headers such as "Aeration rate(Fg:L/h)" plus ~2,200 Raman spectrum columns whose
headers are wavenumbers. We:

  * map each header to a stable snake_case name via the short code inside the
    parentheses (Fg, RPM, T, Fault_ref, ...),
  * drop the Raman columns from the sensor stream (they belong on their own topic),
  * repair the V3 file's broken header row (see repair_headers),
  * assign a batch_id: from the batch-ID column if the file has one, checked
    against the time column resetting at each new batch; otherwise from the
    time resets alone.

Everything streams through the CSV in chunks so the 2.5 GB file never sits in memory.
"""

from __future__ import annotations

import csv
import math
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

# Short code (as it appears in the IndPenSim header) -> canonical field name.
# Units are part of the canonical name so nobody has to guess later.
CODE_TO_FIELD: dict[str, str] = {
    "Fg": "aeration_rate_lph",
    "RPM": "agitator_rpm",
    "Fs": "sugar_feed_rate_lph",
    "Fa": "acid_flow_rate_lph",
    "Fb": "base_flow_rate_lph",
    "Fc": "cooling_water_flow_lph",
    "Fh": "heating_water_flow_lph",
    "Fw": "water_injection_flow_lph",
    "pressure": "air_head_pressure_bar",
    "Fremoved": "dumped_broth_flow_lph",
    "S": "substrate_conc_gpl",
    "DO2": "dissolved_oxygen_mgpl",
    "P": "penicillin_conc_gpl",
    "V": "vessel_volume_l",
    "Wt": "vessel_weight_kg",
    "pH": "ph",
    "T": "temperature_k",
    "Q": "generated_heat_kj",
    "CO2outgas": "offgas_co2_pct",
    "Fpaa": "paa_flow_lph",
    "PAA_offline": "paa_offline_gpl",
    "Foil": "oil_flow_lph",
    "NH3_offline": "nh3_offline_gpl",
    "OUR": "oxygen_uptake_rate",
    "O2": "offgas_o2_pct",
    "P_offline": "penicillin_offline_gpl",
    "X_offline": "biomass_offline_gpl",
    "CER": "carbon_evolution_rate",
    "NH3_shots": "ammonia_shots_kg",
    "Viscosity_offline": "viscosity_offline_cp",
    "Fault_ref": "fault_ref",
    "Control_ref": "control_ref",
    "PAT_ref": "pat_ref",
    "Batch_ref": "batch_ref",
}

# Fields that carry integer flags rather than measurements.
INT_FIELDS = ("fault_ref", "fault_flag", "control_ref", "pat_ref", "batch_ref")

# Order of measurement fields in the Avro contract.
SENSOR_FIELDS: tuple[str, ...] = tuple(
    f for f in CODE_TO_FIELD.values() if f not in INT_FIELDS
)

_CODE_RE = re.compile(r"\(\s*([^:()]+?)\s*:")
_LOWER_CODES = {k.lower(): v for k, v in CODE_TO_FIELD.items()}


def is_raman_column(header: str) -> bool:
    """Raman spectrum columns are headed by a bare wavenumber, e.g. '2400'."""
    try:
        float(str(header).strip())
        return True
    except ValueError:
        return False


def canonical_name(header: str) -> str | None:
    """Map a raw IndPenSim header to its canonical field name, or None if unknown."""
    h = str(header).strip()
    low = h.lower()
    if low.startswith("time"):
        return "time_h"
    if low.replace(" ", "_") in ("batch_id", "batch_number", "batch_no"):
        return "batch_id"
    if low == "fault flag":
        return "fault_flag"
    m = _CODE_RE.search(h)
    code = m.group(1) if m else h  # some exports already use the bare code
    return CODE_TO_FIELD.get(code) or _LOWER_CODES.get(code.lower())


@dataclass
class ColumnMap:
    """How the raw headers of one file map onto canonical fields."""

    mapping: dict[str, str]  # raw header -> canonical name (kept columns only)
    raman: list[str] = field(default_factory=list)
    unknown: list[str] = field(default_factory=list)
    duplicates: list[str] = field(default_factory=list)

    @property
    def has_batch_id(self) -> bool:
        return "batch_id" in self.mapping.values()


def build_column_map(headers: list[str]) -> ColumnMap:
    mapping: dict[str, str] = {}
    raman, unknown, dupes = [], [], []
    seen: set[str] = set()
    for h in headers:
        if is_raman_column(h):
            raman.append(h)
            continue
        name = canonical_name(h)
        if name is None:
            unknown.append(h)
        elif name in seen:
            dupes.append(h)  # e.g. two Raman-flag columns: keep the first
        else:
            seen.add(name)
            mapping[h] = name
    if "time_h" not in seen:
        raise ValueError("No time column found; is this an IndPenSim file?")
    return ColumnMap(mapping, raman, unknown, dupes)


# In 100_Batches_IndPenSim_V3.csv the legend of ONE column (the PAT/Raman control
# flag) was split by commas into three header cells, so the header row has two more
# fields than every data row. Read naively, every column from there on is
# mislabelled: "Batch ID" ends up holding Raman intensities, and the last two
# Raman columns come out empty.
_V3_SPLIT_LEGEND = ["1- No Raman spec", "1-Raman spec recorded", "2-PAT control(PAT_ref:PAT ref)"]
_V3_REPAIRED = "PAT control(PAT_ref:1-No Raman spec 2-Raman spec recorded and PAT control)"


def repair_headers(header: list[str], n_data_fields: int) -> tuple[list[str], str | None]:
    """Return headers aligned to the data, and a note if a repair was made.

    Only the known IndPenSim V3 defect is repaired. Any other mismatch raises:
    guessing column alignment is how silently wrong data gets in.
    """
    if len(header) == n_data_fields:
        return header, None
    stripped = [h.strip() for h in header]
    for i in range(len(stripped) - 2):
        if stripped[i : i + 3] == _V3_SPLIT_LEGEND and len(header) - n_data_fields == 2:
            fixed = header[:i] + [_V3_REPAIRED] + header[i + 3 :]
            return fixed, f"merged split PAT/Raman legend at columns {i}-{i + 2}"
    raise ValueError(
        f"header has {len(header)} fields but data rows have {n_data_fields}; "
        "not a known IndPenSim layout, refusing to guess the column alignment"
    )


def read_headers(csv_path: str | Path) -> list[str]:
    """Header names aligned with the data rows (repaired if needed)."""
    return _read_header_info(csv_path)[0]


def _read_header_info(csv_path: str | Path) -> tuple[list[str], str | None]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        first = next(reader, None)
    return repair_headers(header, len(first) if first is not None else len(header))


def iter_batches(
    csv_path: str | Path, chunksize: int = 20_000
) -> Iterator[tuple[int, pd.DataFrame]]:
    """Yield (batch_id, frame) for each batch, in file order, with canonical columns.

    Raman columns are never parsed (usecols), which is what keeps this fast.
    If the file has a batch-ID column, every ID change must coincide with the
    time column resetting, and IDs must increase; otherwise this raises. That
    check is what catches a mislabelled ID column.
    """
    headers = read_headers(csv_path)
    cmap = build_column_map(headers)
    keep = list(cmap.mapping)
    # skiprows=1 + names: replace the (possibly broken) header row with the repaired one
    reader = pd.read_csv(csv_path, names=headers, header=None, skiprows=1, usecols=keep, chunksize=chunksize)

    batch_id = 0  # last batch id seen
    prev_time = math.inf  # forces the first row to start a batch
    rows_seen = 0
    pending: list[pd.DataFrame] = []

    for chunk in reader:
        chunk = chunk.rename(columns=cmap.mapping)
        t = chunk["time_h"].to_numpy(dtype=float)
        resets = t <= np.concatenate(([prev_time], t[:-1]))  # a time reset starts a new batch
        if cmap.has_batch_id:
            col = chunk["batch_id"].to_numpy(dtype=float)
            if np.isnan(col).any() or (col != np.round(col)).any():
                raise ValueError("batch_id column has missing or non-integer values")
            ids = col.astype(int)
            changes = ids != np.concatenate(([batch_id], ids[:-1]))
            if not np.array_equal(changes, resets):
                row = rows_seen + int(np.argmax(changes != resets)) + 2  # +2: header line, 1-based
                raise ValueError(f"batch_id column disagrees with time resets at CSV line {row}")
            new_ids = ids[changes]
            if (np.diff(np.concatenate(([batch_id], new_ids))) <= 0).any():
                raise ValueError("batch_id values are not strictly increasing between batches")
        else:
            ids = batch_id + np.cumsum(resets)
        prev_time = float(t[-1])
        rows_seen += len(chunk)
        chunk = chunk.assign(batch_id=ids)

        for bid, part in chunk.groupby("batch_id", sort=False):
            bid = int(bid)
            if pending and int(pending[0]["batch_id"].iat[0]) != bid:
                done = pd.concat(pending, ignore_index=True)
                yield int(done["batch_id"].iat[0]), done
                pending = []
            pending.append(part)
        batch_id = int(ids[-1])

    if pending:
        done = pd.concat(pending, ignore_index=True)
        yield int(done["batch_id"].iat[0]), done


def _clean(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def coerce_numeric(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Force every measurement/flag column to numbers.

    A column that arrives as text (a retyped column upstream, e.g. "6.5 pH") would
    otherwise crash the replay or, worse, be half-parsed. Values that are not numbers
    become null, which the data contract then rejects for required fields. Returns
    the frame and, per column, how many non-empty values had to be nulled.
    """
    coerced: dict[str, int] = {}
    out = frame
    for col in frame.columns:
        if col in ("time_h", "batch_id") or pd.api.types.is_numeric_dtype(frame[col]):
            continue
        num = pd.to_numeric(frame[col], errors="coerce")
        bad = int((num.isna() & frame[col].notna()).sum())
        if bad:
            coerced[col] = bad
        out = out.assign(**{col: num})
    return out, coerced


# Simulated plant clock: batch k starts at BASE + (k-1) * BATCH_SPACING_H hours.
# Deterministic, so a re-replay produces identical event times (needed for dedup).
PLANT_EPOCH_MS = 1_672_531_200_000  # 2023-01-01T00:00:00Z; 100 batches x 240 h ends Sep 2025
BATCH_SPACING_H = 240.0


def to_records(batch_id: int, frame: pd.DataFrame, replay_id: str) -> Iterator[dict]:
    """Turn one batch frame into dicts matching streaming/schemas/sensor_reading.avsc."""
    start_ms = PLANT_EPOCH_MS + int((batch_id - 1) * BATCH_SPACING_H * 3_600_000)
    cols = set(frame.columns)
    for seq, row in enumerate(frame.itertuples(index=False), start=1):
        r = row._asdict()
        time_h = float(r["time_h"])
        rec = {
            "batch_id": batch_id,
            "seq": seq,
            "time_h": time_h,
            "event_time": start_ms + int(round(time_h * 3_600_000)),
            "replay_id": replay_id,
        }
        for f in INT_FIELDS:
            v = _clean(r.get(f)) if f in cols else None
            rec[f] = int(v) if v is not None else None
        for f in SENSOR_FIELDS:
            v = _clean(r.get(f)) if f in cols else None
            rec[f] = float(v) if v is not None else None
        yield rec
