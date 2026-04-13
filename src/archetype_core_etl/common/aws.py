"""Shared boto3 client factory.

Centralizes AWS client construction so that region, endpoint override,
and optional static credentials are resolved in exactly one place.
"""

from __future__ import annotations

from typing import Any

import boto3

from archetype_core_etl.config import get_settings


def build_boto3_client(service: str) -> Any:
    """Build a boto3 client for *service* using the active application settings."""
    settings = get_settings()
    kwargs: dict[str, Any] = {"region_name": settings.aws.region}
    if settings.aws.endpoint_url:
        kwargs["endpoint_url"] = settings.aws.endpoint_url
    if settings.aws.access_key_id and settings.aws.secret_access_key:
        kwargs["aws_access_key_id"] = settings.aws.access_key_id.get_secret_value()
        kwargs["aws_secret_access_key"] = settings.aws.secret_access_key.get_secret_value()
    return boto3.client(service, **kwargs)  # type: ignore[call-overload]


__all__ = ["build_boto3_client"]
