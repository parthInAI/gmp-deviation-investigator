"""Generate a small CSV in the same shape as the real IndPenSim file.

Used by the tests, by CI, and by you to run the whole pipeline in a couple of
minutes before (or instead of) handling the 2.5 GB download:

    python -m streaming.sample_data --out data/raw/sample_indpensim.csv --batches 100 --rows 60

It copies the real file's quirks on purpose: long headers with codes in
parentheses, numeric Raman columns, the V3 header row that has two more fields
than the data (one column's legend split into three cells), batch-ID and
fault-flag columns, and the campaign structure (1-30 recipe, 31-60 operator,
61-90 Raman/PAT control, faults in the last 10%). The values are rough shapes,
not a process model; the real data comes from IndPenSim.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import numpy as np
import pandas as pd

HEADERS = [
    "Time (h)",
    "Aeration rate(Fg:L/h)",
    "Agitator RPM(RPM:RPM)",
    "Sugar feed rate(Fs:L/h)",
    "Acid flow rate(Fa:L/h)",
    "Base flow rate(Fb:L/h)",
    "Heating/cooling water flow rate(Fc:L/h)",
    "Heating water flow rate(Fh:L/h)",
    "Water for injection/dilution(Fw:L/h)",
    "Air head pressure(pressure:bar)",
    "Dumped broth flow(Fremoved:L/h)",
    "Substrate concentration(S:g/L)",
    "Dissolved oxygen concentration(DO2:mg/L)",
    "Penicillin concentration(P:g/L)",
    "Vessel Volume(V:L)",
    "Vessel Weight(Wt:Kg)",
    "pH(pH:pH)",
    "Temperature(T:K)",
    "Generated heat(Q:kJ)",
    "carbon dioxide percent in off-gas(CO2outgas:%)",
    "PAA flow(Fpaa:PAA flow (L/h))",
    "PAA concentration offline(PAA_offline:PAA (g L^{-1}))",
    "Oil flow(Foil:L/hr)",
    "NH_3 concentration off-line(NH3_offline:NH3 (g L^{-1}))",
    "Oxygen Uptake Rate(OUR:(g min^{-1}))",
    "Oxygen in percent in off-gas(O2:O2  (%))",
    "Offline Penicillin concentration(P_offline:P(g L^{-1}))",
    "Offline Biomass concentratio(X_offline:X(g L^{-1}))",
    "Carbon evolution rate(CER:g/h)",
    "Ammonia shots(NH3_shots:kgs)",
    "Viscosity(Viscosity_offline:centPoise)",
    "Fault reference(Fault_ref:Fault ref)",
    "0 - Recipe driven 1 - Operator controlled(Control_ref:Control ref)",
    "PAT control(PAT_ref:PAT ref)",
    "Batch reference(Batch_ref:Batch ref)",
    "Batch ID",
    "Fault flag",
]
RAMAN_HEADERS = ["2400", "2399", "2398", "2397"]

# What the real V3 file's header row says instead of the single PAT column above.
BROKEN_PAT_LEGEND = ["1- No Raman spec", " 1-Raman spec recorded", "2-PAT control(PAT_ref:PAT ref)"]


def make_frame(n_batches: int = 100, rows: int = 60, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n_fault = max(1, n_batches // 10)
    parts = []
    for b in range(1, n_batches + 1):
        n = rows + int(rng.integers(-5, 6))  # batches differ in length, like the real data
        t = np.round(np.arange(1, n + 1) * 0.2, 1)
        data = {h: rng.normal(1.0, 0.05, n) for h in HEADERS}
        data["Time (h)"] = t
        data["Temperature(T:K)"] = 298 + rng.normal(0, 0.1, n)
        data["pH(pH:pH)"] = 6.5 + rng.normal(0, 0.02, n)
        data["Penicillin concentration(P:g/L)"] = np.linspace(0, 25, n) + rng.normal(0, 0.2, n)
        fault = np.zeros(n, dtype=int)
        if b > n_batches - n_fault:  # last 10%: a fault switches on mid-batch
            onset = n // 2
            fault[onset:] = 1 + (b % 3)
            data["Agitator RPM(RPM:RPM)"][onset:] *= 0.6
        data["Fault reference(Fault_ref:Fault ref)"] = fault
        data["0 - Recipe driven 1 - Operator controlled(Control_ref:Control ref)"] = int(30 < b <= 60)
        data["PAT control(PAT_ref:PAT ref)"] = 2 if 60 < b <= 90 else 1
        data["Batch reference(Batch_ref:Batch ref)"] = b
        data["Batch ID"] = b
        data["Fault flag"] = int(b > n_batches - n_fault)
        offline = np.full(n, np.nan)
        offline[::12] = rng.normal(1.0, 0.05, len(offline[::12]))  # offline assays are sparse
        data["Offline Penicillin concentration(P_offline:P(g L^{-1}))"] = offline
        for h in RAMAN_HEADERS:
            data[h] = rng.normal(5e4, 1e3, n)
        parts.append(pd.DataFrame(data, columns=HEADERS + RAMAN_HEADERS))
    return pd.concat(parts, ignore_index=True)


def write_csv(frame: pd.DataFrame, path: str | Path, broken_header: bool = True) -> None:
    """Write like the real file: by default with the V3 header row (2 extra fields)."""
    header = list(frame.columns)
    if broken_header:
        i = header.index("PAT control(PAT_ref:PAT ref)")
        header = header[:i] + BROKEN_PAT_LEGEND + header[i + 1 :]
    path = Path(path)
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(header)
        frame.to_csv(f, header=False, index=False)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=Path("data/raw/sample_indpensim.csv"))
    p.add_argument("--batches", type=int, default=100)
    p.add_argument("--rows", type=int, default=60)
    args = p.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    write_csv(make_frame(args.batches, args.rows), args.out)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
