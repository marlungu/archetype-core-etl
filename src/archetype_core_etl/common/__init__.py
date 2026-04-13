"""Shared utilities: exceptions, logging, and cross-cutting helpers."""

from .aws import build_boto3_client

__all__ = ["build_boto3_client"]
