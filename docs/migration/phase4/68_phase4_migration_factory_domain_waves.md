# Phase 4: Migration Factory and Domain Waves

**Status:** Working operating model, documentation and control increment  
**Date:** 2026-10-15  
**Predecessor:** The Phase 3 closeout record identifies signed `main` commit `7859bc5df036a69fff2c71277bfb907f28d48aa5`. A read-only lookup on 2026-10-14 did not find that commit in the connected repository; the repository baseline remains to be reconciled in the domain-wave register.

## 1. Purpose

Define a repeatable, evidence-backed method for qualifying data sources, planning migration waves, reconciling outcomes, accepting migrated data, and rolling back when an agreed threshold is breached. This increment establishes the operating model and its controls before implementation pipelines are changed.

The model applies per source or bounded source group. A domain wave may include multiple sources only when ownership, dependencies, data handling, migration pattern, and acceptance conditions are compatible.

## 2. Scope and boundary

This increment produces the migration-factory workflow, gate criteria, minimum evidence, reusable wave record, exception handling, rollback conditions, and exit criteria.

It does not authorize production migration, pipeline implementation, data deletion, or a change to system architecture. Source-specific mappings and wave assignments remain unapproved until the relevant owners and stewards validate them against an inventory.

## 3. Operating principles

- Assign a named business owner, technical owner, and data steward before qualifying a source.
- Preserve the ability to trace a target record to its source and migration run.
- Version mappings, rules, run parameters, and reconciliation results.
- Make restart and replay behavior explicit for each migration pattern; do not assume it.
- Keep source data available through the agreed acceptance and rollback window.
- Record exceptions with severity, owner, disposition, and evidence.
- Require consumer acceptance before declaring a wave complete.
- Treat security, privacy, retention, and boundary requirements as entry conditions, not after-the-fact checks.

## 4. Factory workflow and gates

### Gate 0 — Intake accepted

**Purpose:** Establish scope and accountability.

**Required evidence:**
- Source and system name, business domain, source owner, technical owner, and data steward
- Intended consumers and business use
- In-scope entities, date range, expected volume, update pattern, and service expectations
- Known upstream and downstream dependencies
- Sensitivity, retention, access, and boundary constraints
- Initial scope decision and open questions

**Exit:** Owners confirm the inventory record is accurate enough to assess readiness.

### Gate 1 — Source qualified

**Purpose:** Decide whether the source can enter design, needs remediation, or should be deferred.

**Assess:**
- Schema availability and stability, key quality, duplicates, nulls, and known data defects
- Historical depth, volume, growth, arrival patterns, and late or corrected records
- Extract method, source load impact, and repeatability of the source snapshot or change boundary
- Lineage, access model, encryption, retention, and approved handling requirements
- Dependencies, consumer criticality, outage tolerance, and operational support
- Known reconciliation limitations and unresolved ownership questions

**Disposition:** Ready for mapping; ready with explicit remediation; or deferred with owner and revisit condition.

**Exit:** Readiness disposition, risks, and prerequisites are recorded and accepted by the accountable owner.

### Gate 2 — Mapping and acceptance design approved

**Purpose:** Agree what moves, how it is interpreted, and how correctness will be demonstrated.

**Required evidence:**
- Versioned source-to-target mapping and transformation rules
- Field-level type, null, default, key, timestamp, and code-set treatment
- Inclusion, exclusion, deletion, correction, and late-arrival semantics
- Rejected-record handling and exception severity rules
- Reconciliation method, tolerances, sampling approach, and known blind spots
- Access and security review appropriate to the data
- Consumer acceptance measures and named approvers
- Planned cutover, rollback triggers, and rollback owner

**Exit:** Data steward, technical owner, security or privacy approver where required, and consumer representative approve the design.

### Gate 3 — Dry run ready

**Purpose:** Demonstrate that the planned run and evidence can be produced safely.

**Required evidence:**
- Dry-run scope, source boundary, run identifier, and versioned configuration
- Execution results, duration, resource observations, and run logs
- Reconciliation output and exception register
- Demonstrated restart or replay behavior for the selected pattern
- Confirmed rollback steps and access to required operators
- No unresolved blocker that would invalidate production acceptance

**Exit:** Wave lead recommends production entry; accountable owner approves it.

### Gate 4 — Production wave entry

**Purpose:** Authorize a bounded migration run.

**Entry checklist:**
- Gates 0–3 complete and approvals current
- Change window, communication plan, operator coverage, and escalation path confirmed
- Source remains available for the agreed recovery period
- Source boundary or snapshot recorded
- Monitoring, audit evidence, and reconciliation owners assigned
- Rollback triggers and decision authority restated before execution

**Exit:** Run is complete, stopped safely, or rolled back; all outcomes are recorded.

### Gate 5 — Reconciliation and acceptance

**Purpose:** Decide whether target data is fit for the stated use.

**Required evidence:**
- Source and target counts by agreed partition or business key
- Agreed control totals and aggregates, with tolerances and exclusions shown
- Key uniqueness and required-field checks
- Record-level traceability samples and documented sampling method
- Exceptions with severity, impact, disposition, and evidence
- Consumer validation against the agreed use cases
- Security, access, and operational checks required for acceptance

**Exit:** Consumer and data owner accept; or the wave remains open with a corrective action, bounded use restriction, or rollback decision.

### Gate 6 — Wave closed

**Purpose:** Close the change and improve the factory.

**Required evidence:**
- Acceptance or rollback decision and approvers
- Final reconciliation result and exception disposition
- Cutover record, source-retention decision, and rollback-window end date
- Evidence bundle location and retention owner
- Lessons, reusable patterns, and follow-up actions with owners and due dates

**Exit:** No open blocker remains; any accepted residual issue has an owner and explicit approval.

## 5. Domain-wave selection

Do not assign sources to a wave by domain name alone. Group them only where they share a manageable migration pattern and compatible dependencies.

For each proposed wave, score or explicitly assess:
- Business criticality and consumer readiness
- Sensitivity and control requirements
- Schema stability and data quality
- Volume, change rate, and timing constraints
- Dependency count and cutover coupling
- Mapping complexity and availability of source-to-target rules
- Reconciliation feasibility and acceptable tolerance
- Recovery and rollback feasibility
- Operational readiness and owner availability

Prefer a bounded, representative wave that proves the controls and operating pattern without coupling unrelated high-risk sources. Separate sources when a shared cutover, security condition, or reconciliation rule would obscure acceptance.

### Wave record template

- Wave ID and proposed domain:
- Scope and exclusions:
- Source systems and entities:
- Business, technical, data, and consumer owners:
- Consumers and intended use:
- Sensitivity and handling constraints:
- Dependencies and sequencing:
- Migration pattern and source boundary:
- Mapping version:
- Reconciliation rules and tolerances:
- Dry-run outcome:
- Production entry approval:
- Rollback triggers and decision owner:
- Acceptance decision and approvers:
- Exceptions and dispositions:
- Evidence bundle and retention:
- Follow-up actions:

## 6. Reconciliation and exception controls

Reconciliation is defined before execution and is specific to the source and intended use. Counts alone are insufficient where key, field, or business-rule correctness matters.

Every exception records:
- Unique identifier, source/target reference, and run
- Severity, business impact, and whether acceptance is blocked
- Detection rule and supporting evidence
- Assigned owner and target resolution date
- Disposition: corrected, accepted with rationale, excluded by approved scope, or unresolved
- Approver for any accepted residual risk

Unresolved high-impact exceptions block acceptance. Lower-severity exceptions may remain only with a documented impact assessment and accountable approval.

## 7. Rollback and recovery

Define rollback conditions before Gate 4. Triggers may include reconciliation outside an approved tolerance, material loss of traceability, unauthorized access or handling, unacceptable source impact, or failure of a critical consumer validation.

For each wave, document:
- Observable trigger and threshold
- Who can pause the run and who authorizes rollback
- Consumer routing or use reversal steps
- Source restoration or continued-source-use steps
- Data quarantine and duplicate-prevention steps during recovery
- Evidence, logs, and records to preserve
- Conditions for retry and required re-approval

Keep the source intact through the agreed rollback window. Do not delete or retire source data as part of this operating-model increment.

## 8. Phase 3 carry-forward items

The Phase 3 closeout records these items as outstanding:
- Inspection Completion was not deployed or executed because the workspace had expired.
- Application Processing Time needs a deterministic proof.
- Portability Activity needs a deterministic proof.

Track these as proof and qualification work. Do not treat them as production-ready sources or assign them to a production wave until ownership, source readiness, mapping, and acceptance evidence meet the gates above.

## 9. Phase 4 increment acceptance criteria

This documentation/control increment is ready to close when:
- The workflow and gates are reviewed by the project owner.
- Required roles and approval points are confirmed.
- The wave record and exception fields are usable for the project.
- Reconciliation, rollback, and acceptance thresholds are defined at source or wave design time.
- Phase 3 carry-forward items are recorded with owners or explicitly retained as open.
- The task board and decision record identify the next bounded implementation or proof task.

## 10. Decisions still required before wave execution

1. Confirm the authoritative source inventory and domain boundaries.
2. Name the accountable approvers for source qualification, production entry, and acceptance.
3. Select a low-risk representative source for the first proof wave only after qualification.
4. Define evidence storage, access, and retention location.
5. Set source-specific reconciliation tolerances and rollback thresholds.
6. Resolve or assign the Phase 3 carry-forward proof items.

