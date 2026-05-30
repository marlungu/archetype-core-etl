#!/usr/bin/env bash
# Pre-commit secrets guard for archetype-core-etl.
# Blocks commits that include sensitive filenames or inline credentials.
# Wired via PreToolUse hook on the Bash tool — runs ahead of any shell command.
# Self-guards: exits 0 immediately if nothing is staged, so non-commit Bash usage is unaffected.

set -e

# Only act when something is staged for commit.
staged=$(git diff --cached --name-only 2>/dev/null || true)
if [ -z "$staged" ]; then
  exit 0
fi

# Filenames that should never be committed.
if echo "$staged" | grep -qE '\.(env|key|pem|p12|pfx)$|credentials\.json$|secrets\.(yaml|yml|json)$'; then
  echo "BLOCKED: staged file matches a sensitive pattern (.env, .key, .pem, credentials.json, secrets.yaml)."
  echo "Review staged files. If intentional, commit manually outside of Claude Code."
  exit 2
fi

# Inline credentials in the staged diff.
diff_content=$(git diff --cached 2>/dev/null || true)
if echo "$diff_content" | grep -qiE 'aws_secret_access_key|aws_access_key_id'; then
  echo "BLOCKED: staged diff appears to contain AWS credentials."
  exit 2
fi

if echo "$diff_content" | grep -qE 'dapi[0-9a-f]{32}'; then
  echo "BLOCKED: staged diff appears to contain a Databricks PAT (pattern: dapi + 32 hex chars)."
  exit 2
fi

if echo "$diff_content" | grep -qE 'sk-[a-zA-Z0-9]{40,}'; then
  echo "BLOCKED: staged diff appears to contain an OpenAI-style API key."
  exit 2
fi

if echo "$diff_content" | grep -qE 'AKIA[0-9A-Z]{16}'; then
  echo "BLOCKED: staged diff appears to contain an AWS access key ID."
  exit 2
fi

exit 0
