"""Domain exceptions for archetype-core-etl.

Every exception raised by application code should inherit from
:class:`ArchetypeError`. Catching the base class at pipeline boundaries lets
the orchestrator record a single audit event per failure while still
preserving the specific subclass for downstream routing.
"""

from __future__ import annotations


class ArchetypeError(Exception):
    """Base class for all archetype-core-etl errors."""


class ConfigurationError(ArchetypeError):
    """Configuration is missing, invalid, or internally inconsistent."""


class ExtractionError(ArchetypeError):
    """A source extraction step failed."""


class TransformationError(ArchetypeError):
    """A transformation step failed."""


class LoadError(ArchetypeError):
    """A load / sink step failed."""


class ClassificationError(ArchetypeError):
    """A classification step (e.g. Bedrock inference) failed."""


__all__ = [
    "ArchetypeError",
    "ClassificationError",
    "ConfigurationError",
    "ExtractionError",
    "LoadError",
    "TransformationError",
]
