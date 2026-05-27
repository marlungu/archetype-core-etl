# Release Checklist

## Code

- Tests pass.
- Lint passes.
- Type checks pass when applicable.
- No secrets are staged.
- No local-only files are staged.

## Data

- Schema changes are documented.
- Backfill or migration needs are documented.
- Audit fields are preserved.
- Dead letter behavior is understood.

## AI

- Prompt version changes are documented.
- Prompt hashes are expected to change only when prompt content changes.
- Token and cost behavior is reviewed.
- Evaluation notes are updated when classifier behavior changes.

## Infrastructure

- Terraform/OpenTofu plan is reviewed before apply.
- Cost-impacting resources are called out.
- Destroy behavior is understood.
- State and credentials are not committed.
