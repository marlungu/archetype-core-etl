# 02_AGENT_ROLES

This folder defines practical agent roles for the project.

Agents should stay within the requested role and avoid broad changes.

If a role finds a risk outside its scope, note the risk and ask before expanding the work.

## Roles

- `data-engineer.md` - Extractors, normalizers, quality gates, loader behavior, tests for ETL behavior.
- `infra-operator.md` - Terraform/OpenTofu, Docker Compose, LocalStack, Airflow infrastructure, cloud cost.
- `ai-governance-reviewer.md` - Prompt versioning, audit trail review, model and token tracking.

## Agent Assignment

Roles are not owned by a specific agent. Either Claude Code or Codex can play any role. The mapping below is the typical assignment based on task fit, not a hard rule.

| Role | Typical Agent | Why |
|---|---|---|
| `data-engineer` | Codex | Focused changes inside a single module with clear input/output contracts. |
| `infra-operator` | Codex | Terraform and Docker changes are usually contained to specific files. |
| `ai-governance-reviewer` | Claude Code | Review work that benefits from broader context across audit, prompt, and data contract files. |
| Cross-cutting refactor | Claude Code | Changes that touch many modules and need a single coherent plan first. |
| Architecture review | Claude Code | The agent reviewing should not be the agent that wrote the code. |

When a task does not fit cleanly into one role, the human assigns. Agents do not negotiate with each other.

## Conflict Resolution

If one agent's work conflicts with another agent's work in progress:

1. Stop and surface the conflict to the human.
2. Do not silently overwrite or refactor the other agent's code.
3. The human decides whether to merge, sequence, or scope-reduce.

The framework assumes one agent at a time on any given branch. Two agents on the same branch is not supported.
