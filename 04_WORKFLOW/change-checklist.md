# Change Checklist

## Before Editing

- Read the relevant code or docs.
- Check for existing tests.
- Identify audit, security, cost, or migration impact.
- Keep the change narrow.

## After Editing

- Run the smallest relevant test first.
- Run `make lint` if Python code changed.
- Run `make typecheck` if types or interfaces changed.
- Run `make test` before handoff when practical.
- Update docs when behavior or operations changed.

## Required Callouts

Call out:
- New cloud cost
- New dependency
- New secret or config requirement
- Schema change
- Prompt behavior change
- Migration or backfill need
- Manual deployment step
