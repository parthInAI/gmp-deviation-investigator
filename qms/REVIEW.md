# Review record for the synthetic QMS

The project plan says the SOPs and past deviations are drafted with an LLM and then **reviewed by hand**. This file records that review.

## Drafting (done)
- 13 SOPs and 40 deviation records drafted with Claude, 5 Nov 2026.
- Setpoints and ranges taken from the simulated normal batches (pH 6.50, 298.0 K, aeration 42–75, sugar feed 15–105, oil 23–35, PAA 0–11, head pressure 0.6–1.1 bar).
- Root-cause categories spread across all 14 taxonomy categories (2–4 records each), with look-alike pairs: agitation vs aeration (DO drop), raw material vs feed pump (low uptake), pH probe vs pH dosing, coolant supply vs temperature probe, contamination vs higher in-spec feed concentration.

## Leakage check (done, before any agent existed)
The drafts were checked against the pre-registered answer key (`evals/answer_key.json`). Four records first copied the timing of a test fault (aeration steps at 22 h and 101–110 h; overfeed at 20–30/76–92/200–212 h; pH offsets at 80–84/140–160 h; temperature rise at 72–88 h). They were moved to other times so the case historian cannot match a test batch by timing alone. A scan confirms no past case shares both the category and the timing of a test fault episode.

## Hand review (to do: Parth)
Read each SOP and a sample of at least 10 deviations, and check:
- [ ] Nothing reads as a real company, person or batch.
- [ ] Each deviation's symptoms, investigation and root cause are consistent with each other and with the SOP it cites.
- [ ] Nothing a QA professional would find implausible (classification, CAPA, effectiveness checks).
- [ ] Record the date and any changes below.

| Date | Reviewer | Changes |
|---|---|---|
| | | |
