# Phase 4 Source Qualification and Wave Decision Record

**Status:** Review-ready template; project-owner review remains open  
**Date:** 2026-10-15  
**Related controls:** `68_phase4_migration_factory_domain_waves.md`; `69_phase4_domain_wave_register.md`  
**System of record:** `phase4_source_qualification_workbook.xlsx`

## Purpose and use

Use one record for each source or tightly related source group after it has been identified in the authoritative source inventory. The workbook is the working intake and gate tracker; this record captures evidence-backed decisions, trade-offs, and approvals. Keep the same candidate ID in both.

Gates 0–1 determine whether a source may enter mapping. Gates 2–6 govern design through wave closure. A comparative priority score can order review of candidates, but cannot qualify a source, waive a gate, approve a wave, or authorize production.

Do not infer source facts from the Phase 3 proof candidates. Inspection Completion (CAND-01/P01), Application Processing Time (CAND-02/P02), and Portability Activity (CAND-03/P03) remain **NOT ASSESSED** and unassigned until validated against the authoritative inventory and by accountable owners. Unknown facts stay blank and are tracked as open evidence requests.

## 1. Inventory and intake record

Complete the inventory provenance and Gate 0 fields first. Record a source register reference and evidence link or repository path for each material fact. Do not copy unverified assumptions into the workbook as facts.

| Field | Value / evidence reference |
|---|---|
| Candidate ID / workbook ID | |
| Inventory record ID and version | |
| Source system and source entity | |
| Business domain | |
| Business owner / data steward / technical owner | |
| Accountable qualification approver | |
| Intended use and accepting consumer | |
| In-scope entities, dates, inclusions, exclusions | |
| Expected volume, update pattern, service expectations | |
| Upstream and downstream dependencies | |
| Extraction method and source boundary | |
| Data classification, access, retention, and handling constraints | |
| Evidence location and evidence-retention owner | |
| Intake prepared by / date | |
| Owner validation date / evidence | |

### Inventory completeness and provenance

| Check | Result / evidence |
|---|---|
| Inventory source of truth and accountable custodian are identified | |
| Inventory scope, as-of date, and inclusion/exclusion rules are recorded | |
| Source entities can be traced to the authoritative inventory | |
| Reconciliation against known source-system/domain owners is recorded | |
| Gaps, conflicting records, and unverified entries have owners and due dates | |
| Accountable owner accepts the inventory record as accurate enough for Gate 0 | |

An inventory is not authoritative merely because it is complete-looking. Do not select WAVE-01 until the project owner confirms its source of truth, scope, custodian, and review method.

## 2. Gate 0 — Intake accepted

Record **Pass**, **Fail**, or **Not assessed** for each check, with evidence and the responsible reviewer. Any unresolved mandatory item keeps Gate 0 open.

| Check | Result | Evidence / open issue | Owner / due date |
|---|---|---|---|
| Source, system, domain, business owner, technical owner, and data steward identified | | | |
| Intended use, consumer, entities, dates, inclusions, and exclusions recorded | | | |
| Expected volume, update pattern, service expectations, and dependencies recorded | | | |
| Extraction method and source boundary described | | | |
| Classification, access, retention, and boundary constraints recorded | | | |
| Owner confirms intake facts are accurate enough to assess | | | |

**Gate 0 decision:** Pending / Accepted / Returned for information  
**Decision owner and date:**  
**Conditions or evidence requests:**

## 3. Gate 1 — Source qualification

Assess the source with evidence. Any **Fail** or **Not assessed** on a material qualification check blocks an unqualified disposition. The accountable owner records **Ready for mapping**, **Ready with remediation**, or **Deferred**. Remediation must have a named owner, due date or revisit trigger, and explicit mapping-entry condition.

| Check | Result | Evidence / open issue | Owner / due date |
|---|---|---|---|
| Schema availability and stability, keys, duplicates, nulls, and known defects assessed | | | |
| Historical depth, volume, growth, arrival patterns, late and corrected records assessed | | | |
| Extraction repeatability, source impact, and snapshot/change boundary assessed | | | |
| Lineage, access, encryption, retention, and approved handling confirmed | | | |
| Dependencies, consumer criticality, outage tolerance, and operational support assessed | | | |
| Reconciliation limits and unresolved ownership questions recorded | | | |
| Intended consumer confirms testable use and acceptance contact | | | |

**Gate 1 disposition:** Not assessed / In progress / Blocked / Ready for mapping / Ready with remediation / Deferred  
**Accountable owner, approver, and date:**  
**Remediation owner and revisit condition (if applicable):**  
**Reason and evidence references:**

## 4. Review-priority score

Score only after the ratings are supported by source and owner evidence. Rate each input from **1 to 5**. For complexity, **1 is low and favorable; 5 is high and difficult**.

**Priority score = Business value + Reuse value + (6 − Complexity)**, from **3 to 15**. Higher scores receive earlier review. This is a review-order aid only; it is not a readiness score or wave-selection rule.

| Input | Rating (1–5) | Evidence and rationale |
|---|---:|---|
| Business value to an identified consumer | | |
| Reuse value of the migration pattern | | |
| Mapping/dependency complexity | | |
| Calculated review-priority score (3–15) | | |

If scores tie, review cutover coupling, schema stability, and owner availability. Record judgment and trade-offs; never use a score to override missing evidence, a failed gate, or owner approval.

## 5. Gates 2–6 — Wave lifecycle decision record

These controls apply after source qualification. Track full evidence and gate status in the workbook. Production entry at Gate 4 requires Gates 0–3 complete and current approvals.

| Gate | Required decision evidence | Status / evidence reference / approver |
|---|---|---|
| Gate 2 — Mapping and acceptance design | Approved, versioned mappings; transformation and exception rules; reconciliation measures/tolerances; consumer criteria; security review; cutover and rollback triggers/owner | |
| Gate 3 — Dry run ready | Bounded scope and source boundary; run ID/configuration; logs and duration; reconciliation results; exceptions; restart/replay and rollback proof; blockers | |
| Gate 4 — Production wave entry | Gates 0–3 approvals; change window; operator coverage; monitoring; evidence and reconciliation owners; rollback triggers; decision authority | |
| Gate 5 — Reconciliation and acceptance | Source/target controls; tolerances and exclusions; key/required-field checks; trace samples; exception disposition; consumer validation; accept/rollback decision | |
| Gate 6 — Wave closed | Final approval or rollback; reconciliation and exception disposition; retention and rollback-window dates; evidence location/owner; lessons and follow-ups | |

## 6. Candidate and wave recommendation

Recommend a first wave only from the authoritative inventory and sources with accepted Gate 0–1 dispositions. Favor a bounded, low-risk representative source with a committed consumer, reusable pattern, workable reconciliation and recovery, manageable cutover coupling, stable schema, and available owners. Keep unrelated domains or incompatible cutovers separate.

| Decision prompt | Assessment / evidence |
|---|---|
| Candidate status: Ready for mapping / Ready with remediation / Deferred | |
| Proposed wave ID (leave blank until approved) | |
| Named owner and committed consumer | |
| Reusable pattern and compatible dependencies | |
| Reconciliation and recovery evidence | |
| Cutover coupling, schema stability, and operational coverage | |
| Trade-offs, exclusions, and reason to combine or separate sources | |
| Recommendation and rationale | |
| Required approvers / decision date | |

## 7. Decision and evidence log

| Date | Gate / decision | Outcome | Decision owner | Evidence reference | Open action / owner / due date |
|---|---|---|---|---|---|
| | | | | | |

## 8. Known Phase 3 carry-forward candidates

These are proof follow-ups, not a complete source inventory and not production-wave assignments.

| Candidate / workbook ID | Known fact | Evidence needed before disposition | Current status |
|---|---|---|---|
| CAND-01 / P01 — Inspection Completion | Not deployed or executed because the workspace had expired | Validate source and owner from inventory; confirm intended use and execution environment; define deterministic proof criteria | NOT ASSESSED; proof candidate only |
| CAND-02 / P02 — Application Processing Time | Deterministic proof remains outstanding | Validate source and owner from inventory; confirm business definition, expected output, and validation/reconciliation criteria | NOT ASSESSED; proof candidate only |
| CAND-03 / P03 — Portability Activity | Deterministic proof remains outstanding | Validate source and owner from inventory; confirm business definition, expected output, and validation/reconciliation criteria | NOT ASSESSED; proof candidate only |

## 9. Factory review record

WF-00 remains open until the accountable project owner confirms the operating model, approval roles, authoritative inventory source/custodian/review, evidence location, evidence-retention owner, and task-board conventions. This template is ready for that review; it does not record that review as complete.

| WF-00 review item | Owner decision / evidence reference |
|---|---|
| Operating model and gate sequence accepted | |
| Approval roles and decision authority confirmed | |
| Inventory source of truth, custodian, scope, and review method confirmed | |
| Evidence location and retention owner confirmed | |
| Task-board IDs and conventions confirmed | |
| Review outcome, approver, and date | |
