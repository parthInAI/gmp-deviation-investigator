"""Derive the Phase 4 root-cause answer key from the raw data, without any model.

    python -m evals.answer_key --source <IndPenSim CSV or silver Delta> [--write]

IndPenSim marks faulty readings with a 0/1 flag but does not say which fault was
injected. The simulator's paper (Goldrick et al. 2019, Comput. Chem. Eng. 130,
106471) names the fault types it uses: aeration, substrate feed, pH sensor
drift and coolant (seen as a temperature shift). For every fault episode this
script measures how far each manipulated input and controlled variable sits
from its normal recipe at the same process time (the same time-aligned
z-score as Phase 3, fitted on the training batches only), and takes the one
furthest away. That variable maps to a fault type.

It uses only raw signals and the training profile: not the PCA, not the LSTM,
not their contributions, so the key is independent of what the agents are
given. The result is written to evals/answer_key.json and committed before any
agent runs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from models.data import load_readings, load_split, subset
from models.features import TimeProfile

KEY_FILE = Path(__file__).parent / "answer_key.json"

# Manipulated inputs and directly controlled variables. A fault shows first and
# largest on the variable it was injected into.
CANDIDATES = [
    "aeration_rate_lph",
    "sugar_feed_rate_lph",
    "cooling_water_flow_lph",
    "heating_water_flow_lph",
    "acid_flow_rate_lph",
    "base_flow_rate_lph",
    "ph",
    "temperature_k",
    "air_head_pressure_bar",
    "paa_flow_lph",
    "oil_flow_lph",
]

# Variable furthest from recipe -> fault type -> taxonomy categories accepted as correct.
FAULT_TYPES = {
    "aeration_rate_lph": ("aeration fault", ["aeration_supply"]),
    "sugar_feed_rate_lph": ("substrate feed fault", ["substrate_feed"]),
    # pH sensor drift (paper). From online data alone a drifting pH probe and a
    # real pH excursion look the same (the controller doses against either), so
    # both pH categories are accepted; the strict score counts only the first.
    "ph": ("pH sensor drift", ["ph_measurement", "ph_dosing"]),
    # Coolant fault, seen as a temperature shift (paper). A failing temperature
    # probe would look alike, so both are accepted; strict = coolant_supply only.
    "temperature_k": ("coolant fault (temperature shift)", ["coolant_supply", "temperature_measurement"]),
    "cooling_water_flow_lph": ("coolant fault", ["coolant_supply", "temperature_measurement"]),
}


def episodes(flag: np.ndarray) -> list[tuple[int, int]]:
    out, i = [], 0
    while i < len(flag):
        if flag[i]:
            j = i
            while j + 1 < len(flag) and flag[j + 1]:
                j += 1
            out.append((i, j))
            i = j + 1
        else:
            i += 1
    return out


def derive(source: str) -> dict:
    df = load_readings(source)
    split = load_split()
    prof = TimeProfile.fit(subset(df, split["train"]), CANDIDATES)
    batches = {}
    for b in split["test_fault"]:
        f = subset(df, [b]).reset_index(drop=True)
        z = np.abs(prof.transform(f))
        eps, votes = [], []
        for i, j in episodes(f["fault_ref"].to_numpy() > 0):
            m = np.nanmean(z[i : j + 1], axis=0)
            order = np.argsort(-m)
            top = CANDIDATES[order[0]]
            votes.append(top)
            eps.append(
                {
                    "start_h": round(float(f["time_h"][i]), 1),
                    "end_h": round(float(f["time_h"][j]), 1),
                    "furthest_from_recipe": [
                        {"signal": CANDIDATES[k], "mean_abs_z": round(float(m[k]), 2)} for k in order[:3]
                    ],
                }
            )
        main = max(set(votes), key=votes.count)
        if main not in FAULT_TYPES:
            raise RuntimeError(f"batch {b}: furthest variable {main} has no fault type mapping")
        name, accepted = FAULT_TYPES[main]
        batches[str(b)] = {
            "fault_type": name,
            "accepted_categories": accepted,
            "strict_category": accepted[0],
            "evidence_variable": main,
            "consistent_across_episodes": len(set(votes)) == 1,
            "episodes": eps,
        }
    return {
        "derived_by": "evals/answer_key.py (raw signals vs training-batch recipe; no model involved)",
        "source_of_fault_types": "Goldrick et al. 2019, Computers & Chemical Engineering 130, 106471",
        "batches": batches,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", required=True)
    p.add_argument("--write", action="store_true", help="write evals/answer_key.json")
    args = p.parse_args(argv)
    key = derive(args.source)
    for b, k in key["batches"].items():
        spans = ", ".join(f"{e['start_h']:.0f}-{e['end_h']:.0f} h" for e in k["episodes"])
        print(f"batch {b}: {k['fault_type']:<36} via {k['evidence_variable']:<20} {spans}")
    if args.write:
        KEY_FILE.write_text(json.dumps(key, indent=2) + "\n")
        print(f"written {KEY_FILE}")
    elif not KEY_FILE.exists():
        print("no committed key yet (run with --write)")
        return 1
    elif json.loads(KEY_FILE.read_text()) != key:
        print("DIFFERS from the committed evals/answer_key.json")
        return 1
    else:
        print("matches the committed evals/answer_key.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
