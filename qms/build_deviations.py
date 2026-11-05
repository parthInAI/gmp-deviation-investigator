"""Write qms/deviations.jsonl: 40 synthetic, closed deviation records for fictional fermenter FV-201.

Kept as code so the records are reviewable in one place and regenerate identically.
Every record is invented (see qms/README.md).
"""

import json
from pathlib import Path

D = []


def dev(id, opened, batch, title, category, classification, symptoms, investigation, root_cause, capa, sops):
    D.append(
        {
            "id": id,
            "synthetic": True,
            "opened": opened,
            "batch": batch,
            "equipment": "FV-201",
            "title": title,
            "category": category,
            "classification": classification,
            "symptoms": symptoms,
            "investigation": investigation,
            "root_cause": root_cause,
            "capa": capa,
            "sops": sops,
            "status": "closed",
        }
    )


# ---- aeration_supply (4)
dev("DEV-2023-004", "2023-02-14", "F2-23-011", "Aeration rate fell below recipe for 3 h", "aeration_supply", "major",
    "Aeration rate dropped from 65 to about 40 L/h at 31 h while the setpoint stayed at 65. Off-gas CO2 fell and off-gas O2 rose within 20 min; DO fell to 7.5 mg/L after 50 min.",
    "FIC-201 output was at 100% but FI-202 confirmed the low flow. Sterile air filter differential pressure PDI-203 had risen from 0.18 to 0.41 bar over two days.",
    "Partially blocked sterile air filter limited the air supply.",
    [{"id": "CAPA-23-009", "action": "Add a PDI-203 high alarm at 0.35 bar and a daily filter dP check to the shift checklist.", "effectiveness": "No aeration loss from filter blockage in the next 15 batches: met."}],
    ["SOP-FER-001 §4.4", "SOP-FER-001 §4.5"])
dev("DEV-2024-007", "2024-03-02", "F2-24-008", "Aeration step change during production phase", "aeration_supply", "major",
    "Aeration rate stepped down to about half of recipe at 47 h and recovered at 52 h. Off-gas O2 rose and CO2 fell; DO dipped.",
    "FIC-201 and FI-202 agreed, so the flow really changed. Compressor C-01 log showed a load-shedding event at that time, triggered by a faulty site-wide pressure switch.",
    "Air compressor load-shedding reduced the air supply to FV-201.",
    [{"id": "CAPA-24-012", "action": "Replace the faulty pressure switch PS-01; give FV-201 priority in the compressor load-shedding logic.", "effectiveness": "No load-shedding events in 6 months: met."}],
    ["SOP-FER-001 §4.3", "SOP-FER-001 §4.6"])
dev("DEV-2024-019", "2024-08-21", "F2-24-031", "Air mass flow controller reading drift", "aeration_supply", "minor",
    "FIC-201 showed recipe flow but FI-202 read 12% lower for 18 h. DO trended 1 mg/L below the batch profile; off-gas O2 slightly high.",
    "FIC-201 failed its calibration check (reading 11% high). Process impact assessed as low; productivity within range.",
    "Calibration drift of mass flow controller FIC-201 delivered less air than displayed.",
    [{"id": "CAPA-24-030", "action": "Shorten FIC-201 calibration interval from 12 to 6 months; add an automatic FIC-201/FI-202 comparison alarm at 5%.", "effectiveness": "Comparison alarm verified in OQ; no recurrence in 10 batches: met."}],
    ["SOP-FER-001 §4.2"])
dev("DEV-2025-003", "2025-01-17", "F2-25-002", "Aeration valve stuck after maintenance", "aeration_supply", "major",
    "Aeration rate flat at 42 L/h from inoculation to 15 h instead of ramping to 65 L/h. DO low throughout; slow growth.",
    "Air inlet valve XV-204 had been left at a mechanical stop after maintenance; work order not closed with a functional check.",
    "Air inlet valve left mechanically limited after maintenance restricted the aeration rate.",
    [{"id": "CAPA-25-002", "action": "Add a functional stroke test of XV-204 to the maintenance close-out checklist.", "effectiveness": "Checklist used on all 8 subsequent maintenance jobs: met."}],
    ["SOP-FER-001 §4.1", "SOP-QA-012 §4.3"])

# ---- agitation (3)
dev("DEV-2023-012", "2023-05-09", "F2-23-029", "Agitator trip with DO crash", "agitation", "major",
    "DO fell from 12.5 to 3 mg/L within 15 minutes at 64 h while aeration rate stayed at recipe. Off-gas O2 rose; temperature rose 0.2 K (less mixing at the cooling jacket).",
    "VSD-240 tripped on over-current; restarted after 22 minutes. Motor bearing found worn at the next shutdown.",
    "Agitator drive trip caused by a worn motor bearing stopped oxygen transfer.",
    [{"id": "CAPA-23-021", "action": "Add vibration monitoring to M-240 and replace bearings on condition.", "effectiveness": "No trips in 12 months: met."}],
    ["SOP-FER-005 §4.2", "SOP-FER-005 §4.3"])
dev("DEV-2024-002", "2024-01-11", "F2-24-001", "Agitator speed sensor failure", "agitation", "minor",
    "SI-240 read zero for 40 minutes at 120 h while DO, off-gas and temperature stayed normal.",
    "Process signals showed mixing continued; the sensor cable was damaged. Agitator itself ran normally.",
    "Faulty speed sensor cable; no real loss of agitation.",
    [{"id": "CAPA-24-003", "action": "Replace SI-240 cable with armoured cable; route away from the walkway.", "effectiveness": "No recurrence in 10 batches: met."}],
    ["SOP-FER-005 §4.1"])
dev("DEV-2025-011", "2025-04-22", "F2-25-014", "High agitator current from foaming", "agitation", "minor",
    "Motor current rose to 118% of profile at 150 h; DO fell slowly; oil feed rate normal.",
    "Foam level high; antifoam addition restored current. Linked to a slightly low oil feed concentration (see DEV-2025-012).",
    "Foam increased agitator load; linked to an oil lot with low antifoam effect.",
    [{"id": "CAPA-25-015", "action": "Add foam probe alarm to the operator display.", "effectiveness": "Alarm in use; no high-current events in 10 batches: met."}],
    ["SOP-FER-005 §4.5", "SOP-FER-006 §4.2"])

# ---- substrate_feed (4)
dev("DEV-2023-008", "2023-03-28", "F2-23-019", "Sugar feed pump stopped", "substrate_feed", "major",
    "Sugar feed rate dropped to zero at 46 h for 2 h. Oxygen uptake and off-gas CO2 fell; DO rose; base consumption fell.",
    "P-210 tripped on thermal overload; restart after 2 h. Feed tank weight confirmed no feed.",
    "Sugar feed pump trip interrupted substrate supply.",
    [{"id": "CAPA-23-015", "action": "Add P-210 run-status alarm to the operator display; spare overload relay stocked.", "effectiveness": "No unalarmed feed stops in 12 months: met."}],
    ["SOP-FER-002 §4.3", "SOP-FER-002 §4.4"])
dev("DEV-2024-011", "2024-05-14", "F2-24-017", "Sugar feed above profile after a phase change", "substrate_feed", "major",
    "Sugar feed rate ran well above the recipe profile from 128 h to 141 h. Oxygen uptake and CO2 rose, DO fell and base demand rose.",
    "Feed controller FIC-210 was found to switch to a stale ramp table after the production-phase change: a software configuration error introduced in an unapproved recipe edit.",
    "Feed recipe configuration error made the feed controller run above profile after phase changes.",
    [{"id": "CAPA-24-018", "action": "Restore validated recipe; lock recipe editing to change control; add automatic recipe checksum verification at batch start.", "effectiveness": "Checksum verified at start of next 20 batches: met."}],
    ["SOP-FER-002 §4.1", "SOP-QA-012 §4.2"])
dev("DEV-2024-024", "2024-10-03", "F2-24-040", "Feed flow meter fouling", "substrate_feed", "minor",
    "FI-211 read 8% above the feed tank weight loss for one shift. Process signals unchanged.",
    "Sugar crystal deposit on the flow meter; the real feed was correct and the meter over-read.",
    "Fouled feed flow meter over-reading; actual feed was within profile.",
    [{"id": "CAPA-24-041", "action": "Add hot-water flush of FI-211 to the CIP cycle.", "effectiveness": "No reconciliation failures in 10 batches: met."}],
    ["SOP-FER-002 §4.2"])
dev("DEV-2025-006", "2025-02-25", "F2-25-007", "Feed line partial blockage", "substrate_feed", "major",
    "Sugar feed rate fell to 60% of profile over 6 hours at 130 h; pump speed at maximum. DO rose; oxygen uptake and CO2 fell.",
    "Blockage found at the feed line check valve; caramelised sugar after a steam-in-place cycle.",
    "Partial blockage of the sugar feed line reduced substrate supply.",
    [{"id": "CAPA-25-008", "action": "Change SIP sequence to flush the feed line before steaming; inspect check valve every 5 batches.", "effectiveness": "No blockages in 10 batches: met."}],
    ["SOP-FER-002 §4.4", "SOP-FER-002 §4.5"])

# ---- ph_measurement (3)
dev("DEV-2023-015", "2023-06-20", "F2-23-034", "pH probe drift detected by offline sample", "ph_measurement", "major",
    "Online pH read 6.50 while the offline sample read 6.31 at 96 h. Base flow had risen 35% over 20 hours with feed unchanged.",
    "AE-220 slope found at 88% after the batch; reference junction partly blocked. Controller had been holding a false reading.",
    "pH probe drift: the controller held a wrong pH because the measurement was wrong.",
    [{"id": "CAPA-23-024", "action": "Add a mid-batch offline pH check at 72 h; replace probes after 10 batches.", "effectiveness": "No undetected drift >0.10 in 12 months: met."}],
    ["SOP-FER-003 §4.3", "SOP-FER-003 §4.4"])
dev("DEV-2024-014", "2024-06-25", "F2-24-022", "Intermittent pH reading offset", "ph_measurement", "major",
    "Online pH moved away from 6.50 at 112–119 h; the controller changed base flow in response. Temperature, feeds and aeration unchanged.",
    "Offline pH at 115 h disagreed with online pH by 0.24. Probe AE-220 cable connector had moisture ingress, giving an offset that came and went.",
    "Intermittent pH measurement offset from a wet probe connector.",
    [{"id": "CAPA-24-025", "action": "Replace connector with sealed type; add connector inspection to pre-batch checklist.", "effectiveness": "No offsets in 10 batches: met."}],
    ["SOP-FER-003 §4.3", "SOP-FER-003 §4.5"])
dev("DEV-2025-009", "2025-03-30", "F2-25-011", "pH probe calibration out of limits not detected", "ph_measurement", "minor",
    "Pre-batch calibration record showed slope 93%, below the 95% limit, but the batch was started.",
    "Calibration record reviewed only after the batch. Offline samples agreed within 0.05, so no process impact.",
    "Procedure gap: no hard stop for a failed calibration before sterilisation.",
    [{"id": "CAPA-25-012", "action": "Electronic calibration record now blocks batch start on a failed slope.", "effectiveness": "Verified in system test: met."}],
    ["SOP-FER-003 §4.2"])

# ---- ph_dosing (3)
dev("DEV-2023-018", "2023-08-07", "F2-23-041", "Base pump failure, pH fell", "ph_dosing", "major",
    "Base flow dropped to zero at 58 h while the controller output was 100%. pH fell to 6.2 over 3 hours; offline pH agreed with online.",
    "Base pump P-221 diaphragm ruptured.",
    "Base dosing pump failure: real pH drop.",
    [{"id": "CAPA-23-026", "action": "Install a flow switch on the base line with an alarm when output >50% and no flow.", "effectiveness": "No unalarmed dosing failures in 12 months: met."}],
    ["SOP-FER-003 §4.1"])
dev("DEV-2024-016", "2024-07-15", "F2-24-026", "Acid pump running continuously", "ph_dosing", "major",
    "Acid flow present for 2 h at 110 h although pH was below setpoint; pH fell to 6.35; offline pH confirmed.",
    "Acid pump P-222 start relay welded closed.",
    "Acid dosing pump stuck on: real pH drop.",
    [{"id": "CAPA-24-027", "action": "Replace relay; add acid-flow-when-pH-low interlock.", "effectiveness": "Interlock tested; no recurrence in 10 batches: met."}],
    ["SOP-FER-003 §4.1"])
dev("DEV-2025-013", "2025-05-12", "F2-25-018", "Base concentration wrong after tank change", "ph_dosing", "minor",
    "Base consumption doubled after a base tank change at 90 h; pH held at setpoint; offline pH agreed.",
    "New base solution prepared at half concentration.",
    "Base solution prepared at the wrong concentration (preparation error).",
    [{"id": "CAPA-25-020", "action": "Add conductivity check of base solution before connection.", "effectiveness": "Check performed on all tank changes in 3 months: met."}],
    ["SOP-FER-003 §4.5", "SOP-QC-021 §4.1"])

# ---- coolant_supply (3)
dev("DEV-2023-021", "2023-09-19", "F2-23-047", "Chilled water supply interruption", "coolant_supply", "major",
    "Temperature rose from 298.0 to 298.9 K at 150–162 h with cooling valve TV-231 fully open. Both probes agreed.",
    "Utilities log showed the chilled-water loop at reduced capacity during chiller maintenance; Fermentation was not informed.",
    "Reduced chilled-water supply during chiller maintenance.",
    [{"id": "CAPA-23-030", "action": "Utilities to notify Fermentation before any chiller work; add loop-temperature signal to the FV-201 display.", "effectiveness": "Notifications given for all 4 later maintenance events: met."}],
    ["SOP-FER-004 §4.3", "SOP-FER-004 §4.4"])
dev("DEV-2024-021", "2024-09-02", "F2-24-035", "Cooling valve actuator failure", "coolant_supply", "major",
    "Temperature rose to 298.7 K over 5 hours at 140 h; TIC-230 output at 100% but cooling water flow stayed low.",
    "TV-231 actuator diaphragm leaking; valve could not open fully.",
    "Cooling water valve actuator failure limited cooling.",
    [{"id": "CAPA-24-036", "action": "Add valve position feedback and a deviation alarm between output and position.", "effectiveness": "No recurrence in 10 batches: met."}],
    ["SOP-FER-004 §4.3"])
dev("DEV-2025-015", "2025-06-18", "F2-25-022", "Temperature high during summer peak", "coolant_supply", "minor",
    "Temperature held at 298.4 K for 6 hours at 100 h during a heatwave; cooling valve fully open; both probes agreed.",
    "Chilled-water loop return temperature 3 K above design during the heatwave.",
    "Insufficient chilled-water capacity at high ambient temperature.",
    [{"id": "CAPA-25-024", "action": "Engineering study for chiller capacity; interim: schedule peak-heat batch phases at night.", "effectiveness": "Study completed; interim measure in place: met."}],
    ["SOP-FER-004 §4.5"])

# ---- temperature_measurement (3)
dev("DEV-2023-024", "2023-10-30", "F2-23-052", "Temperature probe failure", "temperature_measurement", "major",
    "TE-230A read 299.2 K while TE-230B read 298.0 K at 186 h; controller was on TE-230A and opened cooling fully, cooling the broth to 297.4 K by TE-230B.",
    "TE-230A element damaged; controller selected the wrong probe because the probe-selection logic used the higher reading.",
    "Failed temperature probe and probe-selection logic caused over-cooling.",
    [{"id": "CAPA-23-033", "action": "Controller now uses the average of both probes and alarms at 0.2 K difference.", "effectiveness": "No recurrence in 12 months: met."}],
    ["SOP-FER-004 §4.2", "SOP-FER-004 §4.3"])
dev("DEV-2024-005", "2024-02-13", "F2-24-004", "Probe calibration offset after replacement", "temperature_measurement", "minor",
    "Both probes agreed but read 0.4 K below the calibrated reference thermometer during the post-batch check.",
    "Replacement probes had been installed without updating the transmitter offset.",
    "Calibration offset not applied after a like-for-like probe replacement.",
    [{"id": "CAPA-24-006", "action": "Add transmitter offset verification to the probe replacement work instruction.", "effectiveness": "Verified on 3 later replacements: met."}],
    ["SOP-FER-004 §4.2", "SOP-QA-012 §4.5"])
dev("DEV-2025-018", "2025-07-29", "F2-25-027", "Intermittent temperature signal spikes", "temperature_measurement", "minor",
    "Short spikes of +1 K on TE-230B lasting seconds, 14 times over 30 h; TE-230A steady; no controller response.",
    "Loose terminal in the junction box.",
    "Loose wiring on a temperature transmitter.",
    [{"id": "CAPA-25-028", "action": "Tighten and inspect junction box terminals each shutdown.", "effectiveness": "No spikes in 10 batches: met."}],
    ["SOP-FER-004 §4.2"])

# ---- vessel_pressure (3)
dev("DEV-2023-027", "2023-11-21", "F2-23-058", "Back-pressure valve sticking", "vessel_pressure", "minor",
    "Head pressure swung between 0.6 and 1.3 bar for 4 h at 160 h; DO followed the pressure; aeration steady.",
    "PV-260 positioner sticking.",
    "Sticking back-pressure valve positioner.",
    [{"id": "CAPA-23-036", "action": "Replace positioner; add PV-260 to preventive maintenance.", "effectiveness": "No recurrence in 10 batches: met."}],
    ["SOP-FER-007 §4.1", "SOP-FER-007 §4.2"])
dev("DEV-2024-027", "2024-11-12", "F2-24-044", "Exhaust filter blockage raised head pressure", "vessel_pressure", "major",
    "Head pressure rose slowly from 0.9 to 1.4 bar over 12 h; DO rose; aeration flow fell slightly as back-pressure increased.",
    "Exhaust filter wetted by foam carry-over.",
    "Wet exhaust filter after foaming raised vessel pressure.",
    [{"id": "CAPA-24-045", "action": "Install exhaust filter heater; link foam alarm to antifoam addition.", "effectiveness": "No recurrence in 10 batches: met."}],
    ["SOP-FER-007 §4.2"])
dev("DEV-2025-021", "2025-08-26", "F2-25-031", "Pressure transmitter failure", "vessel_pressure", "minor",
    "PT-260 read 0 bar for 2 h; DO and off-gas normal.",
    "Transmitter power supply fuse blown; actual pressure normal by local gauge.",
    "Failed pressure transmitter power supply; no process impact.",
    [{"id": "CAPA-25-033", "action": "Add redundant power supply to PT-260.", "effectiveness": "Installed and tested: met."}],
    ["SOP-FER-007 §4.1"])

# ---- precursor_feed (3)
dev("DEV-2023-030", "2023-12-12", "F2-23-063", "PAA overdose", "precursor_feed", "major",
    "PAA flow ran at 18 L/h for 5 h from 60 h (normal up to 11). Oxygen uptake fell slowly over the next 10 h; final titre 20% low.",
    "PAA pump P-250 speed setpoint entered in the wrong units after a recipe change.",
    "PAA overdose from a recipe unit error; toxic to the culture.",
    [{"id": "CAPA-23-039", "action": "Add high-limit interlock on PAA flow; recipe change review by a second person.", "effectiveness": "Interlock verified: met."}],
    ["SOP-FER-006 §4.1", "SOP-QA-012 §4.2"])
dev("DEV-2024-030", "2024-12-04", "F2-24-049", "PAA feed interruption", "precursor_feed", "minor",
    "PAA flow zero for 3 h at 140 h; little immediate change in online signals.",
    "PAA tank empty: refill missed at shift change.",
    "PAA tank ran empty: refill missed.",
    [{"id": "CAPA-24-050", "action": "Add low-level alarm on PAA tank.", "effectiveness": "No recurrence in 10 batches: met."}],
    ["SOP-FER-006 §4.3", "SOP-FER-006 §4.4"])
dev("DEV-2025-024", "2025-09-30", "F2-25-036", "PAA flow meter reconciliation failure", "precursor_feed", "minor",
    "FI-251 totaliser 7% above tank level change over 24 h.",
    "Flow meter zero drift; actual feed correct.",
    "PAA flow meter zero drift.",
    [{"id": "CAPA-25-037", "action": "Add monthly zero check for FI-251.", "effectiveness": "Implemented: met."}],
    ["SOP-FER-006 §4.4"])

# ---- oil_feed (2)
dev("DEV-2024-009", "2024-04-16", "F2-24-013", "Oil feed pump cavitation", "oil_feed", "minor",
    "Oil flow oscillated between 15 and 35 L/h for 8 h at 180 h; slight foam increase.",
    "Air in the oil suction line after tank change.",
    "Oil pump cavitation after a tank change.",
    [{"id": "CAPA-24-015", "action": "Add venting step to the oil tank change instruction.", "effectiveness": "No recurrence in 10 batches: met."}],
    ["SOP-FER-006 §4.2"])
dev("DEV-2025-012", "2025-04-22", "F2-25-014", "Oil lot with low antifoam effect", "oil_feed", "minor",
    "Foaming increased from 140 h; agitator current rose (see DEV-2025-011).",
    "Oil lot complied with specification but was at the low end of the antifoam component.",
    "Oil lot at the low end of specification for antifoam content.",
    [{"id": "CAPA-25-016", "action": "Tighten supplier specification for antifoam content.", "effectiveness": "Specification updated: met."}],
    ["SOP-FER-006 §4.2", "SOP-QC-021 §4.1"])

# ---- contamination (3)
dev("DEV-2023-033", "2023-04-18", "F2-23-024", "Bacterial contamination", "contamination", "critical",
    "From 50 h oxygen uptake and CO2 rose steadily above the batch profile with feeds unchanged; base demand rose; foaming at 70 h.",
    "Microscopy at 72 h showed rod-shaped bacteria; plating confirmed Bacillus sp. Agitator seal water found contaminated.",
    "Contamination through the agitator mechanical seal.",
    [{"id": "CAPA-23-018", "action": "Replace seal; add seal water bioburden testing weekly.", "effectiveness": "No contamination in 20 batches: met."}],
    ["SOP-QC-020 §4.2", "SOP-QC-020 §4.4", "SOP-FER-005 §4.4"])
dev("DEV-2024-033", "2024-05-28", "F2-24-019", "Suspected contamination not confirmed", "contamination", "minor",
    "Oxygen uptake 10% above profile from 90 h; feeds unchanged.",
    "Microscopy and plating negative. Feed solution lot had 4% higher sugar concentration within specification.",
    "Not contamination: higher (in-spec) sugar concentration in the feed lot.",
    [{"id": "CAPA-24-020", "action": "None required; recorded for trending.", "effectiveness": "n/a"}],
    ["SOP-QC-020 §4.3", "SOP-QC-021 §4.4"])
dev("DEV-2025-027", "2025-10-14", "F2-25-040", "Yeast contamination via sample valve", "contamination", "critical",
    "Gradual pH fall corrected by more base from 110 h; oxygen uptake above profile; titre 30% low.",
    "Yeast confirmed by plating; sample valve steam trap failed.",
    "Contamination through the sample valve after steam trap failure.",
    [{"id": "CAPA-25-040", "action": "Replace steam trap; add temperature monitoring on sample valve steaming.", "effectiveness": "No contamination in 10 batches: met."}],
    ["SOP-QC-020 §4.2", "SOP-QC-020 §4.4"])

# ---- raw_material (3)
dev("DEV-2023-036", "2023-07-11", "F2-23-038", "Sugar feed solution below concentration", "raw_material", "major",
    "Oxygen uptake and CO2 below profile from 40 h with the sugar feed rate exactly on recipe; DO above profile.",
    "Feed solution lot tested at 82% of nominal concentration; dilution error at the supplier.",
    "Sugar feed solution lot under strength: feed rate correct but substrate delivered was low.",
    [{"id": "CAPA-23-027", "action": "Add incoming concentration test (refractometry) to sugar lot release.", "effectiveness": "Test in use for all lots: met."}],
    ["SOP-QC-021 §4.1", "SOP-QC-021 §4.4", "SOP-FER-002 §4.6"])
dev("DEV-2024-036", "2024-08-06", "F2-24-029", "Media component lot change slowed growth", "raw_material", "minor",
    "Slower growth in the first 30 h in three consecutive batches using a new corn steep liquor lot.",
    "Lot met specification; nitrogen content at the low end.",
    "Media component lot at the low end of specification.",
    [{"id": "CAPA-24-031", "action": "Add nitrogen content to supplier quality agreement trending.", "effectiveness": "Trending in place: met."}],
    ["SOP-QC-021 §4.4"])
dev("DEV-2025-030", "2025-11-18", "F2-25-043", "Base solution lot contaminated with particulates", "raw_material", "minor",
    "Base line filter blocked at 70 h; base flow fell; pH dipped to 6.42 before filter change.",
    "Base lot contained particulates from a damaged drum liner.",
    "Particulates in a base solution lot blocked the dosing filter.",
    [{"id": "CAPA-25-044", "action": "Visual inspection of drum liners at receipt.", "effectiveness": "Implemented: met."}],
    ["SOP-QC-021 §4.1", "SOP-FER-003 §4.1"])

# ---- operator_error (2)
dev("DEV-2024-039", "2024-12-17", "F2-24-052", "Manual aeration change without approval", "operator_error", "minor",
    "Aeration rate set to 55 L/h for 2 h at 30 h; no batch record entry.",
    "Operator reduced aeration to control foam without supervisor approval. Procedure and system causes examined: the foam alarm had no defined response, which contributed.",
    "Unapproved manual change, made easier by a missing foam-response instruction.",
    [{"id": "CAPA-24-053", "action": "Add foam response instruction to SOP-FER-001; retrain operators.", "effectiveness": "No unapproved changes in 3 months: met."}],
    ["SOP-FER-001 §4.1", "SOP-QA-011 §4.4"])
dev("DEV-2025-033", "2025-12-02", "F2-25-046", "Wrong feed profile selected at batch start", "operator_error", "major",
    "Sugar feed followed the seed fermenter profile for the first 12 h.",
    "Operator selected the wrong recipe; recipe names were similar. System cause: recipe selection screen showed both recipes without a confirmation step.",
    "Wrong recipe selected; system design allowed it without confirmation.",
    [{"id": "CAPA-25-047", "action": "Rename recipes and add a batch-start confirmation step.", "effectiveness": "Implemented and verified: met."}],
    ["SOP-FER-002 §4.1", "SOP-QA-011 §4.4"])

# ---- other (1)
dev("DEV-2024-042", "2024-03-19", "F2-24-011", "Off-gas analyser calibration failure", "other", "minor",
    "Off-gas CO2 and O2 shifted together by about 5% with all inputs steady; DO unchanged.",
    "AE-265 weekly calibration failed; reference gas cylinder near empty.",
    "Off-gas analyser out of calibration; no process change.",
    [{"id": "CAPA-24-014", "action": "Add reference gas cylinder pressure alarm.", "effectiveness": "Implemented: met."}],
    ["SOP-FER-007 §4.3", "SOP-FER-007 §4.4"])

if __name__ == "__main__":
    assert len(D) == 40, len(D)
    assert len({d["id"] for d in D}) == 40
    out = Path(__file__).parent / "deviations.jsonl"
    out.write_text("".join(json.dumps(d, ensure_ascii=False) + "\n" for d in D))
    print(f"{len(D)} deviations -> {out}")
