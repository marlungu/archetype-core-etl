"""Structured JSON logging setup.

All archetype-core-etl processes should call :func:`configure_logging`
exactly once during startup (e.g. at the top of a DAG file or a CLI
entrypoint). Module code should use :func:`get_logger` to obtain a named
logger and let log records propagate to the root handler configured here.
"""

from __future__ import annotations

import logging
import sys
from typing import IO

from pythonjsonlogger.json import JsonFormatter

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging(
    level: str | int = "INFO",
    *,
    stream: IO[str] | None = None,
) -> None:
    """Configure the root logger to emit structured JSON.

    Idempotent: existing handlers on the root logger are replaced, so it is
    safe to call this again after a process fork (e.g. in a Celery worker).

    Parameters
    ----------
    level:
        Log level name or numeric level. Defaults to ``INFO``.
    stream:
        Destination stream for log records. Defaults to ``sys.stdout`` so
        container runtimes capture logs via their stdout collectors.
    """
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)

    handler = logging.StreamHandler(stream or sys.stdout)
    formatter = JsonFormatter(
        _DEFAULT_FORMAT,
        rename_fields={"asctime": "timestamp", "levelname": "level"},
        json_ensure_ascii=False,
    )
    handler.setFormatter(formatter)

    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    """Return a module-scoped logger that inherits the root configuration."""
    return logging.getLogger(name)


__all__ = ["configure_logging", "get_logger"]
