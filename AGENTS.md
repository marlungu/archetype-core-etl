# AGENTS.md

Repo-level instructions for agents working on `archetype-core-etl`.

## Project

This repository is an audit-ready ETL and AI/data infrastructure project.

It uses:
- Python for ETL code in `src/archetype_core_etl/`
- Airflow DAGs in `dags/`
- Amazon Bedrock for classification
- Databricks Delta tables for output
- PostgreSQL for audit records
- Terraform/OpenTofu for cloud infrastructure
- Docker Compose for local development

## Operating System Folders

Use these folders for project operating docs:

- `00_CONTEXT/` - project context: `README.md`, `project-brief.md`, `glossary.md`, `decisions.md`
- `01_SYSTEM/` - system architecture, data contracts, and controls: `README.md`, `system-overview.md`, `data-contracts.md`, `security-and-controls.md`
- `02_AGENT_ROLES/` - role-specific instructions for agents: `README.md`, `data-engineer.md`, `infra-operator.md`, `ai-governance-reviewer.md`
- `03_PROMPTS/` - prompt guidance and evaluation notes: `README.md`, `prompt-change-policy.md`, `evaluation-notes.md`
- `04_WORKFLOW/` - workflow and runbooks: `README.md`, `change-checklist.md`, `release-checklist.md`, `incident-runbook.md`, `task-board.md`
- `05_OUTPUTS/` - audit outputs and handoff notes: `README.md`, `audit-evidence-template.md`, `handoff-template.md`

Do not put application source code in these folders.

## Rules for Changes

- Do not modify application code unless the task asks for it.
- Do not read `.env`, `.env.*`, `*.tfvars`, private keys, or credential files.
- Do not add secrets, tokens, passwords, or account IDs to docs or code.
- Do not remove tests, logging, audit fields, validation, or governance controls for speed.
- Do not make architecture changes without explaining the tradeoff first and recording the decision in `00_CONTEXT/decisions.md`.
- Keep changes small and specific.
- Prefer existing project patterns.
- Use plain language.
- Do not use em dashes.

## Git Workflow

All agent work follows the same workflow. There are no exceptions for "small" changes.

- One branch per task. Branch names follow `<agent>/<short-description>`, for example `codex/fix-audit-hash-lookup` or `claude/refactor-classify-batch`.
- One concern per pull request. If a task touches three unrelated concerns, open three pull requests.
- A pull request requires human approval before merge. Agents do not approve their own pull requests.
- The CI workflow at `.github/workflows/ci.yml` must pass before merge. CI runs `make precommit` and `make test` inside the dev container, which matches the local dev workflow exactly.
- Commits use the agent's distinct git identity. The author identity makes `git log --author` and `git blame` accurate.
- Commit messages follow the format below.

### Commit Message Format

```
<type>(<scope>): <short summary>

<longer explanation if needed, wrapped at 72 columns>

<optional footer with issue references or co-author tags>
```

Types: `fix`, `feat`, `refactor`, `test`, `docs`, `chore`, `security`, `infra`.

Scopes match the package or folder: `audit`, `classifier`, `quality-gate`, `dag`, `infra`, `docs`, `pre-commit`, and so on.

Examples:

```
fix(audit): correct input_record_hash lookup when records are skipped
refactor(classifier): rename max_retries to max_attempts, default 3
infra(dev-container): add dev-write service for make format
```

### Author Identity

Each agent configures its git identity at the start of the session:

```
# Codex
git config user.name "Codex (archetype-core-etl)"
git config user.email "codex@archetype-core-etl.local"

# Claude Code
git config user.name "Claude Code (archetype-core-etl)"
git config user.email "claude-code@archetype-core-etl.local"
```

Use repo-local config (`git config`, not `git config --global`) so the identity stays project-scoped.

## Documentation Consistency

Before completing documentation or operating-system work:

- Use `rg --files` to check actual filenames before adding links or path references.
- Confirm every referenced repo file path exists.
- Do not reference files that do not exist.
- If a required file is missing, either create it or update the instruction to point to the real file.

## Commands

Use these commands when validation is needed:

- `make test` - run pytest inside the dev container
- `make lint` - run ruff checks inside the dev container
- `make typecheck` - run mypy inside the dev container
- `make format` - run ruff formatting through the writable dev service
- `make precommit` - run all pre-commit hooks inside the dev container
- `python3 -m pytest` - fallback when running directly
- `python3 -m ruff check src/ tests/ dags/` - fallback lint command

Cloud commands can create cost:

- `make cloud-cost-check`
- `make cloud-up`
- `make cloud-down`
- `make cloud-destroy-all`

Do not run cloud create or destroy commands unless explicitly requested.

## Audit Expectations

Preserve these controls:

- `pipeline_run_id` is generated at runtime and passed through tasks.
- Input records keep stable hashes for audit evidence.
- Prompt files are versioned. Do not edit an existing production prompt in place.
- Prompt hashes are recorded with classification outputs.
- Token costs are tracked separately for input and output tokens.
- SQL writes must stay parameterized.
- Dead letter writes must not crash the pipeline.
- Secrets must stay in secret managers, env vars, or local ignored files.

## Completion Format

When work is complete, summarize:

1. What changed
2. Files changed
3. Tests run
4. Risks or follow-ups
