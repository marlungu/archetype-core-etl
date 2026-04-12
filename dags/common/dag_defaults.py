"""Shared Airflow DAG defaults for archetype-core-etl."""

from __future__ import annotations

from datetime import timedelta

default_args = {
    "owner": "archetype-data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}
