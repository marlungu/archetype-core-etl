# Feature factory post-mortem

Date: 2026-05-30
Scope: first feature shipped end to end through the Claude Code agent chain

## Context

This repository is developed with a Claude Code agent chain — five scoped subagents (researcher, spec-writer, pipeline-builder, test-verifier, validator) coordinated by an orchestrator skill, with two human approval checkpoints. The setup runs inside a devcontainer so the chain executes with an in-container Python environment and a default-deny network firewall.

The first feature shipped end to end through the chain was a small one on purpose: add a `reason` string to `confidence_band` so each routing decision carries a short human-readable explanation for the audit trail. The point was not the feature. The point was to run the whole chain once, watch where it held and where it slipped, and write down what it taught us before building anything larger.

This ADR records that.

## What we learned

### The spec step earns its place

The feature description said the reason was "for the audit trail." The researcher found that `confidence_band` has zero production callers — only the unit tests consume it. So "for the audit trail" was motivation, not a wiring path. Nothing persists from this function yet.

A single-session agent builds what the prompt says and leaves the gap for later. The chain surfaced it before any code was written. The spec-writer then made three decisions with rationale: return a small `BandDecision` dataclass matching the existing `GateResult` pattern, defer persistence as out of scope (no caller exists to persist from, and forcing it would mean schema migrations across bronze, gold, and the Postgres audit table — the exact drift the project warns against), and render the reason text from the threshold constants so it cannot drift from the routing logic.

That is the value. Not speed. The chain catches the thing you would have discovered three commits later.

### Friction became rules

Two assumptions the chain got wrong, both now written into CLAUDE.md:

- `make test` shelled out to `docker compose`, which has no daemon inside the devcontainer. The chain kept hitting it and falling back to running pytest directly. The fix was a context-aware Makefile: on the host the targets route through `docker compose --profile tools`; inside the container they run the tools directly. CLAUDE.md now states this so no future run rediscovers it.
- CLAUDE.md described the confidence gate as a single threshold ("below 0.85") when the code uses two (0.85 and 0.60). The spec-writer flagged the discrepancy rather than papering over it. Reconciled.

The pattern worth keeping: every friction point should end as a rule in CLAUDE.md. The file grows from real mistakes, not from guessing up front what the agents might need.

### The validator was not truly tested

The validator returned zero findings. That is honest, but the feature was too small to exercise it. A twenty-line helper with no callers gives the validator little to catch. We will not know whether the validator earns its place until a messier feature — one with real scope, schema changes, or a governance surface — gives it something to find. Noted, not resolved.

### Overhead and checkpoints

The chain took roughly eight to ten minutes for a twenty-line function. That overhead is wrong for a helper and right for a real feature. The lesson is to reserve the full chain for changes that touch Bedrock, Delta, or the audit path, and not to spend it on trivial edits.

The approval checkpoint also did not announce itself clearly. The chain stops and waits for "approved," but the pause looks like any other end of turn. It was nearly missed twice. The checkpoint should read as a checkpoint.

## What we'll do differently

1. Keep the five-agent chain and the two checkpoints. The spec step alone justified the structure on the first run.
2. Reserve the full chain for features that touch Bedrock, Delta, or the audit trail. Use plain editing for trivial changes.
3. Treat CLAUDE.md as a living record. Every rule the chain gets wrong becomes an entry.
4. Make the approval checkpoint visually unmistakable in the orchestrator skill.
5. Re-evaluate the validator after a larger feature, where it has something real to catch.

## Consequences

The repository now carries its own development method as a tracked, explainable artifact. A reviewer can read the agent definitions, the checkpoints, and this post-mortem and see how the code is built, not just what it does. That traceability is the point — the same property the pipeline itself is built to provide.