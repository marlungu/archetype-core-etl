# Decisions

Use this file to record project decisions that affect architecture, governance, operations, cost, security, or auditability.

A decision belongs here when it has long-lived consequences. Day-to-day implementation choices stay in pull requests. Decisions that shape how the project works, what it costs to operate, or how it is audited belong here.

## Decision Log

| Date | Decision | Rationale | Status |
|---|---|---|---|
| 2026-04-15 | Use record_id lookup instead of list index when joining classification results to input records in `AuditWriter._build_entries`. | The classifier may skip records on transient failure, so `len(results) < len(input_records)` is a normal outcome. Index-based mapping silently writes the wrong `input_record_hash` for every audit row after a skip. A lookup dictionary keyed on `record_id` removes the alignment risk. Regression tests in `tests/unit/test_audit_writer.py`. | Accepted |
| 2026-04-17 | Rename `BedrockClassifier` retry parameter from `max_retries` to `max_attempts`, with default 3. | The previous parameter and default value did not match the function's behavior. `max_retries=1` with the existing condition produced zero retries. The new name describes what the function actually does. Default of 3 covers normal Bedrock transient errors without unbounded retry cost. | Accepted |
| 2026-04-19 | Remove the hardcoded 8-second sleep between classification calls and wire `RateLimiter` into both batch and streaming DAGs. | The hardcoded sleep did not adapt to actual API conditions and stacked with the limiter if both were used. The `RateLimiter` class exists for this purpose. Conservative starting limits (20 RPM, 40k TPM) can be tuned after observing real traffic. | Accepted |
| 2026-04-21 | Field presence check in the quality gate uses explicit `None` and key membership rather than Python falsiness. | The previous check treated `0`, `False`, and `""` as missing fields. A record with `pages=0` was rejected for the wrong reason. The new check at `src/archetype_core_etl/transform/field_presence.py` is correct and tested at 100% coverage. | Accepted |
| 2026-04-23 | Remove all credential defaults from `configure_mwaa_runtime`. `DATABRICKS_TOKEN` and `ARCHETYPE_DB_AUDIT_URL` are now required environment variables with explicit runtime validation. | Hardcoded credential defaults at module scope leak into every Airflow log, error trace, and recording risk. The previous values should be treated as compromised. The function only runs under Airflow (when `AIRFLOW_HOME` is set), so pytest collection stays clean. | Accepted |
| 2026-04-25 | All direct `boto3.client()` calls outside `common/aws.py` are blocked by a pre-commit hook. The `build_boto3_client` factory is the only entry point. | Direct boto3 client construction bypasses the LocalStack endpoint override, breaking local development for any code path that does it. Centralizing client construction in one factory makes the dev/prod switch a single configuration concern. | Accepted |
| 2026-04-28 | The dev environment runs in a sandboxed Docker container with read-only source mounts by default. A separate `dev-write` service provides read-write mounts exclusively for `make format`. | Read-only source mounts make every code modification visible and explicit. The only operation that writes to source is `make format`, which uses a separate named service. Read-only is the default; writability is the named exception. | Accepted |
| 2026-04-28 | Pre-commit hooks run inside the dev container in validation-only mode. Formatting is not silently applied on commit. | Validation-only hooks fail the commit when something is wrong, forcing the developer to run `make format` and review the diff before committing. Silent auto-fixes mask problems and remove the chance to learn from the failure. | Accepted |
| 2026-04-28 | The `detect-aws-credentials` pre-commit hook is removed. Real credential leak protection is provided by the `detect-private-key` hook and the custom `no direct boto3.client()` hook. | The hook produced false positives on the literal string `test`, which is the LocalStack convention used throughout the codebase. The replacement hooks have no false positives in this codebase. | Accepted |
| 2026-05-27 | Keep operating-system files in lowercase kebab-case. | Matches the repository's existing documentation naming style and avoids duplicate uppercase files. | Accepted |

## When to Add an Entry

Add an entry to the log when the change affects any of:

- The shape or content of the audit trail
- How secrets are stored, retrieved, or validated
- The contract between pipeline stages
- The cost profile of a normal run
- Which services run in the dev container, and how they are sandboxed
- Which controls run in CI and what they enforce
- Any decision that future you would want to know the reason for

## When Not to Add an Entry

Implementation choices that do not change the operating picture stay in pull requests. Examples of things that do not need an entry: renaming a private variable, fixing a typo, adding a test for existing behavior, formatting changes, dependency version bumps inside the same major version.
