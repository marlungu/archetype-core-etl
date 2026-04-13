#!/usr/bin/env python3
"""Generate synthetic federal document records for local development and testing.

Produces NDJSON files matching the ``FederalDocumentRecord`` schema with
realistic weighted distributions for agency, priority tier, and flags.

Usage::

    # Local: 1000 records to data/raw/YYYY/MM/DD/
    python scripts/generate_data.py

    # Reproducible run, custom count
    python scripts/generate_data.py --records 5000 --seed 42

    # Write directly to S3 (uses ARCHETYPE_AWS_* settings)
    python scripts/generate_data.py --output s3 --output-path raw/federal
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from faker import Faker

# ---------------------------------------------------------------------------
# Weighted distributions
# ---------------------------------------------------------------------------

_AGENCIES: list[tuple[str, float]] = [
    ("USCIS", 0.60),
    ("CBP", 0.15),
    ("ICE", 0.10),
    ("TSA", 0.10),
    ("FEMA", 0.05),
]

_PRIORITY_TIERS: list[tuple[str, float]] = [
    ("standard", 0.70),
    ("expedite", 0.20),
    ("emergency", 0.10),
]

_FLAG_POOL = [
    "expedited_review",
    "high_value",
    "fraud_alert",
    "manual_override",
    "audit_required",
    "incomplete_docs",
    "priority_hold",
    "legal_review",
]

_DOCUMENT_TYPES = [
    "I-130 Petition",
    "I-485 Adjustment",
    "I-765 EAD",
    "I-140 Immigrant Petition",
    "I-20 Student Status",
    "I-539 Extension",
    "N-400 Naturalization",
    "I-90 Green Card Renewal",
    "I-129 Worker Petition",
    "I-751 Remove Conditions",
]

_CASE_STATUSES = [
    "pending",
    "under_review",
    "approved",
    "denied",
    "rfe_issued",
    "escalated",
]

_FORM_NUMBERS = [
    "I-130",
    "I-485",
    "I-765",
    "I-140",
    "I-20",
    "I-539",
    "N-400",
    "I-90",
    "I-129",
    "I-751",
    None,
    None,
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _weighted_choice(choices: list[tuple[str, float]], rng: random.Random) -> str:
    """Pick a value from *choices* using the given weights."""
    values, weights = zip(*choices, strict=True)
    return rng.choices(values, weights=weights, k=1)[0]


def _pick_flags(rng: random.Random) -> list[str]:
    """Return flags with distribution: 80% none, 15% one, 5% two."""
    roll = rng.random()
    if roll < 0.80:
        return []
    if roll < 0.95:
        return [rng.choice(_FLAG_POOL)]
    return rng.sample(_FLAG_POOL, k=2)


def _generate_record(fake: Faker, rng: random.Random) -> dict[str, Any]:
    """Generate a single record dict matching ``FederalDocumentRecord``."""
    return {
        "record_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "submitted_at": fake.date_time_between(
            start_date="-90d", end_date="now", tzinfo=timezone.utc
        ).isoformat(),
        "document_type": rng.choice(_DOCUMENT_TYPES),
        "agency": _weighted_choice(_AGENCIES, rng),
        "applicant_id": f"APP-{rng.randint(100_000, 999_999)}",
        "case_status": rng.choice(_CASE_STATUSES),
        "priority_tier": _weighted_choice(_PRIORITY_TIERS, rng),
        "document_text": fake.paragraph(nb_sentences=rng.randint(3, 12)),
        "form_number": rng.choice(_FORM_NUMBERS),
        "pages": rng.randint(1, 50),
        "flags": _pick_flags(rng),
        "officer_notes": fake.sentence() if rng.random() < 0.30 else None,
    }


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def _write_local(records: list[dict[str, Any]], output_path: str) -> str:
    """Write NDJSON to local disk, partitioned by date."""
    now = datetime.now(tz=timezone.utc)
    partition = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%S")

    base = Path(output_path) if output_path else Path("data/raw")
    dest_dir = base / partition
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = dest_dir / f"records_{timestamp}.ndjson"
    with filename.open("w") as fh:
        for record in records:
            fh.write(json.dumps(record, default=str) + "\n")

    return str(filename)


def _write_s3(records: list[dict[str, Any]], output_path: str | None) -> str:
    """Write NDJSON to S3, partitioned by date."""
    import boto3

    from archetype_core_etl.config import get_settings

    settings = get_settings()
    aws = settings.aws

    kwargs: dict[str, Any] = {"region_name": aws.region}
    if aws.endpoint_url:
        kwargs["endpoint_url"] = aws.endpoint_url
    if aws.access_key_id and aws.secret_access_key:
        kwargs["aws_access_key_id"] = aws.access_key_id.get_secret_value()
        kwargs["aws_secret_access_key"] = aws.secret_access_key.get_secret_value()

    client = boto3.client("s3", **kwargs)
    bucket = aws.raw_bucket

    now = datetime.now(tz=timezone.utc)
    partition = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%S")
    prefix = output_path.rstrip("/") if output_path else "raw"
    key = f"{prefix}/{partition}/records_{timestamp}.ndjson"

    body = "\n".join(json.dumps(r, default=str) for r in records) + "\n"
    client.put_object(Bucket=bucket, Key=key, Body=body.encode())

    return f"s3://{bucket}/{key}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic federal document records."
    )
    parser.add_argument(
        "--records",
        type=int,
        default=1000,
        help="Number of records to generate (default: 1000).",
    )
    parser.add_argument(
        "--output",
        choices=["local", "s3"],
        default="local",
        help="Output target (default: local).",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Local directory or S3 prefix for output.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible runs.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    rng = random.Random(args.seed)
    fake = Faker()
    if args.seed is not None:
        Faker.seed(args.seed)

    print(f"Generating {args.records} synthetic records...")
    t0 = time.perf_counter()

    records = [_generate_record(fake, rng) for _ in range(args.records)]

    if args.output == "s3":
        location = _write_s3(records, args.output_path)
    else:
        location = _write_local(records, args.output_path)

    elapsed = time.perf_counter() - t0

    print(f"Records:  {args.records}")
    print(f"Output:   {location}")
    print(f"Elapsed:  {elapsed:.2f}s")


if __name__ == "__main__":
    main()
