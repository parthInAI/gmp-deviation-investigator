# Synthetic quality system (QMS)

**Everything in this folder is invented.** It describes a fictional site, *Ardmore Biologics, Fermentation Suite F2*, with one 100,000 L penicillin fermenter, FV-201. No real company, procedure, batch or person is described. The documents exist so the case-history agent and the QMS MCP server have realistic material to search.

- `sops/`: 12 standard operating procedures, numbered sections (cite as `SOP-FER-001 §4.2`).
- `deviations.jsonl`: 40 closed deviation records with root cause and CAPA (cite as `DEV-2024-007`).

Setpoints and units match the simulated IndPenSim data (pH 6.50, 298.0 K, aeration rate and feed rates in the dataset's L/h).

The past deviations are spread over 13 root-cause categories on purpose. Several categories produce similar symptoms (a falling dissolved oxygen can come from aeration, agitation or contamination), so a past case is a lead to check against the sensor evidence, never an answer on its own.

Drafted with an LLM and reviewed by hand, as the project plan specifies. See `REVIEW.md` for the review record.
