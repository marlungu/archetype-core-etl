# Phase 4 Domain-Wave Register

**Status:** Working register; source inventory, owner review, and repository baseline reconciliation remain open  
**Date:** 2026-10-16  
**Related operating model:** 68_phase4_migration_factory_domain_waves.md  
**Qualification workbook:** phase4_source_qualification_workbook.xlsx

## Control rules

Use Gates 0–1 to decide whether a source may enter mapping. Gate 0 accepts the intake record. Gate 1 records the source qualification and accountable-owner disposition. The workbook shows NOT ASSESSED, IN PROGRESS, BLOCKED, READY FOR MAPPING, READY WITH REMEDIATION, DISPOSITION REQUIRED, or DEFERRED.

A source may be proposed for a wave after Gate 1 is accepted as Ready for mapping or Ready with remediation. Record the remediation owner and revisit condition when remediation is required. A qualification score does not replace Gate 0–1 evidence, approve a wave, or authorize production.

Gates 2–6 govern mapping and acceptance design, dry-run readiness, production entry, reconciliation and acceptance, and wave closure. Production entry requires Gates 0–3 complete and current approvals.

The workbook uses a review-priority score of business value + reuse value + (6 − complexity), ranging from 3 to 15. Higher scores receive earlier review. Leave scores blank until the ratings are supported by source and owner evidence. For ties, consider cutover coupling, schema stability, and owner availability. Do not select a wave by score or domain name alone.

Wave IDs are provisional. Use the project's authoritative task board and decision record when this register is integrated into the repository.

## Repository and baseline evidence check

Read-only check performed 2026-10-14 against the connected `marlungu/archetype-core-etl` repository:

| Check | Observed evidence | Implication |
|---|---|---|
| Repository visibility and default branch | Public repository; default branch `main` | Confirm this is the authorized Phase 4 integration target before placing project controls here |
| Current `main` head | `b93fc0a751e30b107ed6c6147fcfa4c9f73e81e6`, dated 2026-09-30; commit message concerns Unity Catalog governance proof and a PySpark reference | Record as the observed head, not as the Phase 3 close commit |
| Repository description in `README.md` | Airflow/Databricks ETL reference implementation for federal document records; explicitly described as not a client production deployment | Does not establish the authoritative source inventory or project owner approval |
| Phase 4 inventory/register artifacts | No paths matching Phase 4, migration-wave, qualification, or source-inventory artifacts were found in the recursive `main` tree | Current Library artifacts are not yet integrated into this repository |
| Recorded Phase 3 close commit | `7859bc5df036a69fff2c71277bfb907f28d48aa5` was not found by the repository commit lookup | Reconcile commit provenance/history before treating repository `main` as the Phase 3 continuation baseline |

This check establishes repository state only. It does not establish that the accessible repository is the intended Phase 4 system of record. The control artifacts may be reviewed on a draft branch; do not merge them or derive source inventory facts from the repository until the accountable project owner confirms the target repository, baseline, and inventory source of truth.

## Source-inventory discovery check

Read-only searches of the accessible project Google Drive were performed on 2026-10-16 for `HCAS source contract`, `PIM source contract`, `Phase 3 HCAS inventory`, and `source inventory Archetype`. No authoritative Phase 1–3 source inventory, HCAS/PIM source contract, or source-owner record was identified. Broader matches were unrelated MarOS operating records and an Archetype Core audit; they are not evidence for source qualification.

**Disposition:** Inventory provenance, scope, custodian, owner assignments, and source facts remain unverified. Keep CAND-01 through CAND-03 **NOT ASSESSED**, leave WAVE-01 unassigned, and request the authoritative inventory and accountable custodian through the project governance channel before Gate 0 begins.

## Work items

| ID | Workbook ID | Type | Scope | Current status | Entry evidence / blocker | Exit condition |
|---|---|---|---|---|---|---|
| WF-00 | — | Factory controls | Review and adopt intake, qualification, mapping, reconciliation, acceptance, rollback, and closure controls | Drafted; project-owner review pending | Confirm project roles, approval points, evidence location, retention owner, task-board conventions, and authorized repository/baseline | Operating model and required records accepted by the accountable project owner |
| CAND-01 | P01 | Proof candidate | Inspection Completion | NOT ASSESSED; proof not deployed or executed | Phase 3 record says the workspace expired. Re-establish source, owner, intended use, and execution environment before deciding whether to retry | Deterministic proof reviewed and source disposition recorded, or candidate explicitly deferred |
| CAND-02 | P02 | Proof candidate | Application Processing Time | NOT ASSESSED; deterministic proof outstanding | Identify source, owner, business definition, output, and validation and reconciliation criteria | Deterministic proof reviewed and source disposition recorded, or candidate explicitly deferred |
| CAND-03 | P03 | Proof candidate | Portability Activity | NOT ASSESSED; deterministic proof outstanding | Identify source, owner, business definition, output, and validation and reconciliation criteria | Deterministic proof reviewed and source disposition recorded, or candidate explicitly deferred |
| WAVE-01 | — | Production wave | First bounded, low-risk representative source, to be selected | Not selected | Requires the authoritative source inventory, Gate 0–1 qualification, owner approval, compatible dependencies, and a workable migration pattern | Complete Gates 0–6 with approvals, reconciliation evidence, exception disposition, and closure record |

## Gate 0–1 qualification checklist

Complete the source profile and record evidence in the qualification workbook before assigning a candidate to a proposed production wave.

**Gate 0 — Intake accepted**

- [ ] Source, system, business domain, business owner, technical owner, and data steward identified
- [ ] Intended use, consumers, entities, in-scope dates, expected volume, update pattern, and service expectations recorded
- [ ] Upstream and downstream dependencies recorded
- [ ] Sensitivity, access, retention, and boundary constraints recorded
- [ ] Accountable owners confirm the intake record is accurate enough to assess

**Gate 1 — Source qualified**

- [ ] Schema availability and stability, key quality, duplicates, nulls, and known defects assessed
- [ ] Historical depth, volume, growth, arrival patterns, and late or corrected records assessed
- [ ] Extract method, source impact, and snapshot or change-boundary repeatability assessed
- [ ] Lineage, access, encryption, retention, and approved handling requirements confirmed
- [ ] Dependencies, consumer criticality, outage tolerance, and operational support assessed
- [ ] Reconciliation limits and unresolved ownership questions recorded
- [ ] Accountable owner accepts the disposition: Ready for mapping, Ready with remediation, or Deferred
- [ ] Remediation owner and revisit condition recorded when applicable

## Gates 2–6 wave controls

Use the operating model for detailed acceptance evidence. Record the evidence reference, owner, target date, open issues, and decision for each gate in the workbook.

- **Gate 2 — Mapping and acceptance design:** Approve versioned mappings and transformations, late/corrected/deleted-record rules, exception handling, reconciliation method and tolerances, security review, consumer criteria, cutover and rollback triggers, and rollback owner.
- **Gate 3 — Dry run ready:** Record bounded scope, source boundary, run ID and configuration, execution logs and duration, reconciliation results, exceptions, restart/replay proof, rollback steps, and unresolved blockers.
- **Gate 4 — Production wave entry:** Confirm Gates 0–3 approvals, change window, operator coverage, source retention, monitoring, evidence and reconciliation owners, rollback triggers, and decision authority.
- **Gate 5 — Reconciliation and acceptance:** Record source/target counts and controls, tolerances and exclusions, key and required-field checks, trace samples, exception dispositions, consumer validation, and acceptance or rollback decision.
- **Gate 6 — Wave closed:** Record final approval or rollback, reconciliation and exception disposition, source-retention and rollback-window dates, evidence location and retention owner, lessons, and assigned follow-up actions.

## Sequencing

1. **Confirm the integration baseline:** Have the accountable project owner confirm the target repository, explain the Phase 3 commit mismatch, and identify the authoritative inventory source and custodian. A draft review branch is not approval to merge or qualify sources.
2. **Close WF-00 review:** Confirm the operating model, project approval roles, evidence location, retention owner, task-board conventions, and repository target.
3. **Build the authoritative inventory:** Identify all in-scope source systems, entities, and domains. Do not infer the inventory from the three Phase 3 carry-forward candidates or the repository README.
4. **Resolve candidate ownership and facts:** Validate CAND-01 through CAND-03 against source records and owner evidence. Keep proof status separate from production readiness.
5. **Qualify candidates:** Complete Gates 0 and 1, record the owner-accepted disposition, and score only when evidence supports the ratings.
6. **Select WAVE-01:** Choose a bounded representative source from the qualified inventory using control requirements, compatibility, dependency/cutover coupling, schema stability, reconciliation and recovery feasibility, consumer readiness, and owner availability. Use the score to prioritize review, not as automatic selection.
7. **Design and execute in order:** Complete Gates 2–6 before declaring the wave closed. Production entry remains a separate approval at Gate 4.

## Decision log

| Decision | State | Rationale / condition |
|---|---|---|
| Treat Phase 3 carry-forward items as unqualified proof candidates | Agreed | Prior proof gaps do not establish production readiness or domain-wave membership |
| Use P01–P03 as the workbook crosswalk for CAND-01–CAND-03 | Working convention | Keeps candidate identity consistent between the register and qualification workbook |
| Use Gates 0–1 for source qualification and Gates 2–6 for the wave lifecycle | Agreed in the qualification workbook | Separates source readiness for mapping from later design, production, acceptance, and closure decisions |
| Use the 3–15 value/reuse/complexity score to order review | Working rule; review at WF-00 | Score supports prioritization only and cannot override controls or owner approval |
| Select WAVE-01 only after inventory and source qualification | Proposed | Prevents arbitrary or unsupported source assignment |
| Keep this increment documentation and control focused | Agreed | Establish a repeatable process before implementation pipeline changes |
| Assign project roles and approval authority | Open | Must be confirmed against active project governance |
| Select evidence location and retention owner | Open | Must follow the repository and project evidence controls |
| Confirm repository target and reconcile Phase 3 close commit with current `main` | Open | Read-only check on 2026-10-14 found `main` at `b93fc0a751e30b107ed6c6147fcfa4c9f73e81e6`; recorded Phase 3 SHA was not found |

## Progress measure

The next implementation decision is ready when the repository target and baseline are reconciled, WF-00 is reviewed, the source inventory is authoritative, each carry-forward candidate has an owner and disposition, and at least one source has an accepted Gate 0–1 qualification record. WAVE-01 remains unassigned until those conditions are met. Production entry requires Gates 0–3 and current approvals; acceptance and closure require Gates 5–6 evidence.

