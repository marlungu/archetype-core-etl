"""Application configuration package.

Callers should import :func:`get_settings` rather than instantiating
:class:`Settings` directly — the cached accessor guarantees a single parse
per process and gives tests a clear seam (``get_settings.cache_clear()``)
to force a re-read after mutating the environment.
"""

from __future__ import annotations

from functools import lru_cache

from .settings import (
    AirflowSettings,
    AWSSettings,
    BedrockSettings,
    DatabricksSettings,
    DatabaseSettings,
    Settings,
)

__all__ = [
    "AWSSettings",
    "AirflowSettings",
    "BedrockSettings",
    "DatabricksSettings",
    "DatabaseSettings",
    "Settings",
    "get_settings",
]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide :class:`Settings` instance.

    The result is cached for the lifetime of the process. Call
    ``get_settings.cache_clear()`` in tests when you need to reload
    environment variables.
    """
    return Settings()
