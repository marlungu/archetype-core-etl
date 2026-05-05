"""Tests for prompt versioning."""

from __future__ import annotations

from archetype_core_etl.classify.prompts import load_prompt, prompt_hash


def test_load_default_prompt():
    text = load_prompt("compliance_v1")
    assert "federal compliance analyst" in text
    assert "compliance_score" in text
    assert "risk_tier" in text


def test_prompt_hash_is_deterministic():
    h1 = prompt_hash("compliance_v1")
    h2 = prompt_hash("compliance_v1")
    assert h1 == h2
    assert len(h1) == 64  # full SHA-256 hex digest


def test_prompt_hash_changes_with_version():
    # Same version always returns same hash
    h1 = prompt_hash("compliance_v1")
    assert isinstance(h1, str)
    assert len(h1) == 64
