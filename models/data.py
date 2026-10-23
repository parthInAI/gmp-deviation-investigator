"""What the detection model reads: which readings, which signals, which batches.

The model reads silver (contract-checked, deduplicated) through delta-rs, or the
IndPenSim CSV through the same reader the replayer uses (for CI and quick runs).
Both give identical readings.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

MODELS_DIR = Path(__file__).resolve().parent
GATE_FILE = MODELS_DIR / "gate_criteria.json"

# Online signals a plant historian would actually record. Left out on purpose:
#   agitator_rpm, ammonia_shots_kg  - constant in every normal batch (no information)
#   *_offline_*                     - lab assays, ~98% empty (every ~12 h)
#   substrate_conc_gpl, penicillin_conc_gpl - simulator states, not online sensors in a real
#       plant (estimating them is what Raman/PAT is for); using them would flatter the model
#   fault_ref, fault_flag, control_ref, pat_ref, batch_ref - labels and recipe flags, never features
FEATURES: tuple[str, ...] = (
    "aeration_rate_lph",
    "sugar_feed_rate_lph",
    "acid_flow_rate_lph",
    "base_flow_rate_lph",
    "cooling_water_flow_lph",
    "heating_water_flow_lph",
    "water_injection_flow_lph",
    "air_head_pressure_bar",
    "dumped_broth_flow_lph",
    "dissolved_oxygen_mgpl",
    "vessel_volume_l",
    "vessel_weight_kg",
    "ph",
    "temperature_k",
    "generated_heat_kj",
    "offgas_co2_pct",
    "paa_flow_lph",
    "oil_flow_lph",
    "oxygen_uptake_rate",
    "offgas_o2_pct",
    "carbon_evolution_rate",
)
STEP_H = 0.2  # IndPenSim sampling interval
# Monitored subset. Broth dumps and water injections are on/off event commands whose
# timing varies between normal batches; their spikes dominate a PCA and blind it to
# real faults on those very signals. Their effect is measured anyway, through vessel
# volume and weight. Chosen on calibration data (docs/phase3/NOTES.md).
EVENT_COMMANDS = ("dumped_broth_flow_lph", "water_injection_flow_lph")
MONITORED: tuple[str, ...] = tuple(f for f in FEATURES if f not in EVENT_COMMANDS)
KEEP = ("batch_id", "seq", "time_h", "fault_ref", *FEATURES)


def load_split() -> dict:
    """The pre-registered split (models/gate_criteria.json): train / calibration / test_normal / test_fault."""
    g = json.loads(GATE_FILE.read_text())
    return {k: list(g["split"][k]) for k in ("train", "calibration", "test_normal", "test_fault")}


def load_readings(source: str | Path) -> pd.DataFrame:
    """Readings sorted by batch and seq, with only the columns the model may use (+ fault_ref for scoring).

    source: a silver Delta table directory, a .parquet cache, or the IndPenSim CSV.
    """
    source = Path(source)
    if source.is_dir():
        from deltalake import DeltaTable

        df = DeltaTable(str(source)).to_pandas(columns=list(KEEP))
    elif source.suffix == ".parquet":
        df = pd.read_parquet(source, columns=list(KEEP))
    else:
        from streaming import indpensim

        df = pd.DataFrame([r for b, f in indpensim.iter_batches(source) for r in indpensim.to_records(b, f, "model")])[
            list(KEEP)
        ]
    df = df.sort_values(["batch_id", "seq"]).reset_index(drop=True)
    df["step"] = (df["time_h"] / STEP_H).round().astype(int)
    df["fault_ref"] = df["fault_ref"].fillna(0).astype(int)
    return df


def subset(df: pd.DataFrame, batch_ids) -> pd.DataFrame:
    return df[df["batch_id"].isin(set(batch_ids))].reset_index(drop=True)
