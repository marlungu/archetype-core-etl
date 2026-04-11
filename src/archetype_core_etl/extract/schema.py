"""Pydantic v2 schemas for extracted federal document records."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

Agency = Literal["USCIS", "CBP", "ICE", "TSA", "FEMA"]
PriorityTier = Literal["standard", "expedite", "emergency"]


class FederalDocumentRecord(BaseModel):
    """Canonical representation of a federal document as it enters the pipeline.

    Immutable by design (``frozen=True``) — downstream transforms produce new
    instances rather than mutating upstream ones. ``extra="forbid"`` so that
    unknown fields surface as validation errors instead of being silently
    dropped.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    record_id: UUID
    submitted_at: datetime
    document_type: str
    agency: Agency
    applicant_id: str
    case_status: str
    priority_tier: PriorityTier
    document_text: str = Field(..., min_length=10)
    form_number: str | None = None
    pages: int = Field(..., ge=1)
    flags: list[str] = Field(default_factory=list)
    officer_notes: str | None = None


__all__ = ["Agency", "FederalDocumentRecord", "PriorityTier"]
