"""Versioned prompt templates for the Bedrock classifier.

Each prompt is stored as a plain text file so changes are visible in
git history. The active prompt is loaded by version name at classifier
initialization time.

When modifying a prompt, create a NEW version file (e.g. compliance_v2.txt)
rather than editing an existing one. This preserves the exact text that
generated historical classifications — critical for audit trail integrity.
"""

from __future__ import annotations

import hashlib
from functools import lru_cache
from importlib import resources


@lru_cache(maxsize=4)
def load_prompt(version: str = "compliance_v1") -> str:
    """Load a prompt template by version name.

    Returns the raw text content of the prompt file, stripped of
    leading/trailing whitespace.
    """
    ref = resources.files(__package__).joinpath(f"{version}.txt")
    return ref.read_text(encoding="utf-8").strip()


def prompt_hash(version: str = "compliance_v1") -> str:
    """SHA-256 hash (first 16 hex chars) of the prompt text."""
    text = load_prompt(version)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


__all__ = ["load_prompt", "prompt_hash"]
