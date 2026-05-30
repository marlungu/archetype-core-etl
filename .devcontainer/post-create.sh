#!/usr/bin/env bash
# Runs once after the container is created.
# Installs archetype-core-etl dependencies into the in-container venv so that
# pytest, ruff, and mypy all run without reaching outside the container.

set -euo pipefail

echo "==> Installing archetype-core-etl into /opt/venv"

# Upgrade pip tooling first.
/opt/venv/bin/python -m pip install --upgrade pip setuptools wheel

# Install the project with dev extras. Adjust the extra name if pyproject.toml
# uses something other than [dev] (e.g. [test], [all]).
if grep -q '\[project.optional-dependencies\]' pyproject.toml 2>/dev/null; then
  /opt/venv/bin/python -m pip install -e ".[dev]" || \
  /opt/venv/bin/python -m pip install -e .
else
  /opt/venv/bin/python -m pip install -e .
fi

echo "==> Verifying the toolchain is importable inside the container"
/opt/venv/bin/python - <<'PYEOF'
import sys
print(f"python: {sys.version.split()[0]}")
for mod in ("pytest", "ruff", "mypy"):
    try:
        __import__(mod)
        print(f"ok: {mod}")
    except ImportError:
        print(f"MISSING: {mod} (add it to your dev dependencies)")
PYEOF

echo "==> post-create complete"
