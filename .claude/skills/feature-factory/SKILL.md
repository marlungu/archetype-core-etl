---
name: feature-factory
description: The agent chain for shipping a feature on archetype-core-etl. Use whenever the user asks to "build", "ship", "add", "implement", or "create" a feature, DAG, classification logic, or pipeline change. Coordinates researcher → spec-writer → human approval → pipeline-builder → test-verifier → validator → human PR review.
---

# Feature Factory

This skill is the chain that ships features on archetype-core-etl. It runs five subagents in sequence with two human checkpoints.

## When to invoke this skill

The user says any of: "build", "ship", "add", "implement", "create", or describes a feature, a new DAG, a classification change, a Bedrock prompt change, a schema change, or a service addition.

If the user is asking a question, debugging existing behavior, or doing exploratory work — this skill does NOT apply. Answer normally.

## The chain

### Step 1 — Researcher

Invoke the `researcher` subagent with the user's feature description.

Pass through its output as the research note.

### Step 2 — Spec-writer

Invoke the `spec-writer` subagent with:
- The user's feature description
- The researcher's findings

Show the user the full spec.

### **HUMAN CHECKPOINT 1**

Stop. Wait for the user to type "approved" or to request edits.

If edits are requested: re-invoke spec-writer with the edit notes. Loop until approved.

Do NOT proceed past this checkpoint without explicit approval in the chat.

### Step 3 — Pipeline-builder

Invoke the `pipeline-builder` subagent with:
- The approved spec
- The researcher's findings

Pass through its summary of files changed, helpers reused, and the API contract.

### Step 4 — Test-verifier

Invoke the `test-verifier` subagent with:
- The approved spec
- The pipeline-builder's summary

Pass through its report: which criteria passed, which failed.

### Step 5 — Validator

Invoke the `validator` subagent with:
- The approved spec
- The pipeline-builder's summary
- The test-verifier's report

If the validator finds CRITICAL issues:
- Loop back to pipeline-builder with the validator report
- Then re-run test-verifier
- Then re-run validator
- Repeat until validator is clean OR three iterations have passed

If after three iterations the validator is still finding CRITICAL issues, stop and surface this to the user. The chain may have hit a structural problem the human needs to resolve.

### **HUMAN CHECKPOINT 2**

Show the user:
- The full diff
- The validator's clean report
- The test-verifier's pass report

Wait for the user to approve the PR.

## Rules

- Never skip an agent.
- Never combine agents into a single call.
- Never proceed past a human checkpoint without explicit approval in the chat.
- The researcher's findings get passed to every downstream agent as context.
- The spec is the source of truth. If an agent disagrees with the spec mid-flow, stop and surface it to the user — do not silently work around the spec.
- If pipeline-builder needs to touch files outside the spec's scope list, the chain stops and asks the user.

## What success looks like

A clean validator report, all acceptance tests passing, a diff scoped exactly to what the spec said, and the user reviewing a PR that took two interventions on their part (approve spec, approve PR) instead of fifty.
