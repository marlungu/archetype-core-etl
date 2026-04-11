"""Transform layer: normalization and quality-gate validation."""

from .normalizer import normalize_record
from .quality_gate import GateResult, QualityGate

__all__ = ["GateResult", "QualityGate", "normalize_record"]
