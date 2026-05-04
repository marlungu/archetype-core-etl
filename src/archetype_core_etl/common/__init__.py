"""Shared utilities: exceptions, logging, and cross-cutting helpers."""

from .aws import build_boto3_client
from .dead_letter import DeadLetterWriter

__all__ = ["DeadLetterWriter", "build_boto3_client"]
