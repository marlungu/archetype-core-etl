"""Field-presence validation for raw record dicts.

Checks whether every required field is present in a dict *and* is not
``None``.  Falsy-but-valid values (``0``, ``False``, ``""``) are
intentionally accepted; schema-level value validation is the job of
:class:`FederalDocumentRecord`.
"""

from __future__ import annotations

from typing import Any


def check_required_fields(
    record: dict[str, Any],
    required_fields: set[str],
) -> list[str]:
    """Return a list of field names that are absent or explicitly ``None``.

    A field is considered missing if:
    - the key is not present in ``record``, or
    - the key is present but its value is ``None``.

    Falsy-but-valid values such as ``0``, ``False``, and ``""`` are *not*
    treated as missing.
    """
    return [field for field in required_fields if field not in record or record.get(field) is None]


__all__ = ["check_required_fields"]
