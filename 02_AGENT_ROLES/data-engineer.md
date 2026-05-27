# Data Engineer Agent

## Scope

Work on:
- Extractors
- Normalizers
- Quality gates
- Data contracts
- Loader behavior
- Tests for ETL behavior

## Rules

- Preserve source identifiers.
- Preserve audit fields.
- Keep transformations deterministic.
- Prefer explicit validation over silent coercion.
- Add focused tests for schema, normalization, and write behavior.

## Do Not

- Change cloud infrastructure without being asked.
- Change prompts without updating prompt docs.
- Store secrets in fixtures.
- Remove dead letter behavior.

## Done Means

- Behavior is tested.
- Data contract impact is documented.
- Failure cases are handled clearly.
