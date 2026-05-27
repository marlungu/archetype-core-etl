# CLAUDE.md

Use AGENTS.md as the primary project instruction file.

This repository is an Archetype Core open-source reference project for audit-ready ETL and AI/data infrastructure. The goal is to make the work explainable, testable, governed, reusable, and strong enough to share publicly with pride.

## Read First

Before making changes, read:

- AGENTS.md
- README.md
- 00_CONTEXT/project-brief.md
- 00_CONTEXT/glossary.md
- 00_CONTEXT/decisions.md
- 01_SYSTEM/system-overview.md
- 01_SYSTEM/data-contracts.md
- 01_SYSTEM/security-and-controls.md
- 04_WORKFLOW/change-checklist.md
- 04_WORKFLOW/release-checklist.md
- 04_WORKFLOW/task-board.md

## Working Rules

- Do not modify application code without a clear task.
- Do not make architecture changes without updating 00_CONTEXT/decisions.md and the relevant file in 01_SYSTEM/.
- Do not remove tests, logging, documentation, quality gates, or governance controls for speed.
- Do not add dependencies unless the reason is documented.
- Do not expose secrets, credentials, tokens, or private configuration.
- Keep changes small and focused.
- Prefer existing project patterns.
- Run tests before calling work complete.
- Update README.md when project behavior changes.
- Update 04_WORKFLOW/task-board.md when a task moves forward.
- Use `rg --files` before completing documentation or operating-system work, and confirm every referenced repo file path exists.
- Do not reference missing files. If a required file is missing, create it or point the instruction to the real file.

## Role Split

Codex may be used for focused repo edits and implementation work.

Claude may be used for planning, architecture review, documentation, critique, and implementation support.

Both tools must follow the same project rules. Git, tests, AGENTS.md, README.md, 00_CONTEXT/decisions.md, and the project operating-system files remain the source of truth.

## Completion Format

When work is complete, summarize:

1. What changed
2. Why it changed
3. Files changed
4. Tests run
5. Decisions updated
6. Risks or follow-ups
