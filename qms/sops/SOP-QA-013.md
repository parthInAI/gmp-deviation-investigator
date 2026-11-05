---
id: SOP-QA-013
title: Response to multivariate process monitor alarms
version: 1.1
effective: 2024-09-01
owner: Quality Assurance / Process Science
synthetic: true
---
> SYNTHETIC DOCUMENT. Fictional site (Ardmore Biologics). Not a real procedure.

## 1 Purpose
To define what happens when the multivariate process monitor (PCA, Hotelling T² and SPE) raises an alarm on FV-201.

## 2 Scope
The validated, static process monitor and its alarm output.

## 3 Responsibilities
3.1 The operator acknowledges the alarm and starts the checks below.
3.2 Process Science reviews the alarm's top contributing signals.
3.3 QA decides whether a deviation is opened.

## 4 Procedure
4.1 An alarm means the batch has left the normal operating pattern for at least 2 hours. It does not say why.
4.2 The alarm comes with the signals that contributed most. Check those signals first, in order, against the equipment-specific SOP (aeration: SOP-FER-001; sugar feed: SOP-FER-002; pH, acid and base: SOP-FER-003; temperature and cooling: SOP-FER-004).
4.3 The monitor's score is reported as a multiple of the control limit. Scores above 5 times the limit have almost always been real process faults; scores just above the limit need closer review before any action.
4.4 Any alarm that is confirmed as a real process event is recorded as a deviation (SOP-QA-010). An alarm judged to be a false alarm is recorded with the reason.
4.5 Investigation drafts may be prepared by the AI investigation assistant. The assistant only drafts: it cannot open, close or approve a deviation, and nothing it drafts is released without a named QA reviewer's decision.

## 5 Records
Alarm log; deviation record; reviewer decision with name, date and reason.

## 6 References
SOP-QA-010; SOP-QA-011; SOP-QA-012.
