# CLAUDE.md

`AGENTS.md` is the canonical instruction file for this repository. Read it first. The rules below add Claude-specific guidance, not contradictions.

If you see a conflict between this file and `AGENTS.md`, `AGENTS.md` wins. Open a pull request to fix the conflict.

## Project Context

This repository is an Archetype Core open-source reference project for audit-ready ETL and AI/data infrastructure. The goal is to make the work explainable, testable, governed, reusable, and strong enough to share publicly with pride.

## Read First

Before making changes, read in this order:

1. `AGENTS.md` - universal contract for all agents
2. `README.md` - project surface
3. `00_CONTEXT/project-brief.md` - what this project is
4. `00_CONTEXT/glossary.md` - terms used throughout the codebase
5. `00_CONTEXT/decisions.md` - why the code is the way it is
6. `01_SYSTEM/system-overview.md` - runtime flow and component map
7. `01_SYSTEM/data-contracts.md` - what each pipeline stage hands off
8. `01_SYSTEM/security-and-controls.md` - controls that must be preserved
9. `02_AGENT_ROLES/` - the role you are playing on this task
10. `04_WORKFLOW/change-checklist.md` - before and after editing
11. `04_WORKFLOW/task-board.md` - current state of project work

## Role Split

Claude Code and Codex both work on this repository. They are not interchangeable.

**Claude Code typically plays:**
- `ai-governance-reviewer` - prompt versioning, audit trail review, token and cost tracking
- Architecture review and critique for any role
- Cross-cutting refactors that touch multiple modules
- Documentation and operating-system folder updates
- Planning before implementation

**Codex typically plays:**
- `data-engineer` - extractors, normalizers, quality gates, loader behavior
- `infra-operator` - Terraform, Docker Compose, deployment runbooks
- Focused single-function changes with clear scope
- Test generation for existing code

Both agents follow the same rules. The distinction is task fit, not authority.

When the task is ambiguous about which agent should pick it up, the human assigns. Do not negotiate with the other agent.

## Working Rules

These are Claude-specific clarifications on top of `AGENTS.md`. The full rule set is in `AGENTS.md`.

- When updating the `00_CONTEXT/decisions.md` log for an architecture change, write the entry yourself rather than asking the human to do it.
- When a task requires cross-cutting changes across more than three files, write the plan first and confirm scope before editing.
- When reviewing code that another agent wrote, focus on whether the change preserves the audit and governance controls listed in `AGENTS.md` under "Audit Expectations." Do not rewrite for style.
- When asked for an opinion on contested architecture, give the opinion with the tradeoff. Do not hedge.

## Completion Format

When work is complete, summarize:

1. What changed
2. Why it changed
3. Files changed
4. Tests run
5. Decisions updated (if any)
6. Risks or follow-ups
